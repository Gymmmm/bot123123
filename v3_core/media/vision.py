"""Per-photo visual semantics for cover planning (Gym spec 2026-10-11 §一).

Every usable photo gets one structured record:

    room_type            客厅/卧室/厨房/卫生间/阳台/外观/庭院/泳池/健身房/停车位/入口/景观/其他
    property_type_hint   公寓/别墅/无法确认
    quality              {sharpness, brightness, composition, completeness} in 0..1
    hero_score, obstruction_score, distortion_score, confidence   in 0..1
    duplicate_group      model-supplied group key ("" when unknown)
    objects              visible evidence (沙发/床/灶台/马桶/立面 …) for cross-validation
    view                 window / balcony view is a feature of the shot
    reason               short Chinese explanation

Providers are pluggable and chosen by environment variables only; no key ever
lives in code:

    V3_VISION_PROVIDER   "gemini" | "openai_compat" | "fixture" | "heuristic" (default)
    V3_VISION_API_KEY    provider key; for gemini (or openai_compat pointed at
                         Google's endpoint) falls back to GEMINI_API_KEY
    V3_VISION_BASE_URL   gemini default https://generativelanguage.googleapis.com/v1beta;
                         openai_compat default https://api.openai.com/v1 (Gemini's
                         compat endpoint …/v1beta/openai/ also works)
    V3_VISION_MODEL      vision-capable model id; a comma list is tried in order
                         when a model is not available to the key (HTTP 404)
    V3_VISION_THINKING   gemini thinking level: minimal | low | medium | high | off
    V3_VISION_TIMEOUT    seconds per request (default 30)
    V3_VISION_MAX_ATTEMPTS  attempts per photo incl. retries on 429/5xx/timeout (default 4)
    V3_VISION_RPM        optional client-side pacing (requests/minute; e.g. 14 on a free key)
    V3_VISION_MAX_BACKOFF   cap in seconds for one retry wait (default 60; honours retryDelay)
    V3_VISION_FIXTURE_DIR  directory of recorded JSON results (tests / local evidence)

The default ``heuristic`` provider makes no network calls: it re-uses the
colour/edge ranker and caps confidence below the label threshold, so labels are
hidden and the cover planner ranks by measurable quality only (safe degrade).
Results are cached next to the photo so a listing is never paid for twice.
"""
from __future__ import annotations

import base64
import io
import json
import logging
import os
import random
import socket
import threading
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping, Protocol
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import numpy as np
from PIL import Image, ImageOps

LOGGER = logging.getLogger(__name__)

PROMPT_VERSION = "cover_vision_v1_20261011"
ROOM_TYPES = (
    "客厅", "卧室", "厨房", "卫生间", "阳台", "外观", "庭院", "泳池",
    "健身房", "停车位", "入口", "景观", "其他",
)
PROPERTY_HINTS = ("公寓", "别墅", "无法确认")
OBJECT_VOCAB = (
    "沙发", "电视", "茶几", "餐桌", "床", "衣柜", "书桌", "灶台", "水槽", "橱柜", "冰箱",
    "马桶", "淋浴", "洗手台", "浴缸", "镜柜", "立面", "屋顶", "大门", "车库", "院墙",
    "草坪", "树木", "泳池水面", "健身器材", "车辆", "洗衣机", "栏杆", "窗景", "高楼景观",
)
# Legacy ranker room keys → canonical Chinese room types.
LEGACY_ROOM = {
    "living": "客厅", "bedroom": "卧室", "kitchen": "厨房", "toilet": "卫生间",
    "exterior": "外观", "pool": "泳池", "balcony": "阳台",
}
HEURISTIC_CONFIDENCE_CAP = 0.45


def _clamp(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    if number != number:  # NaN
        return default
    if number > 1.0 and number <= 100.0:
        number /= 100.0  # tolerate 0..100 answers
    return max(0.0, min(1.0, number))


@dataclass(frozen=True)
class PhotoVision:
    room_type: str = "其他"
    property_type_hint: str = "无法确认"
    quality: dict[str, float] = field(default_factory=dict)
    hero_score: float = 0.0
    obstruction_score: float = 0.0
    distortion_score: float = 0.0
    duplicate_group: str = ""
    confidence: float = 0.0
    objects: tuple[str, ...] = ()
    view: bool = False
    reason: str = ""
    provider: str = ""

    @property
    def quality_score(self) -> float:
        parts = [self.quality.get(k) for k in ("sharpness", "brightness", "composition", "completeness")]
        values = [float(v) for v in parts if v is not None]
        return sum(values) / len(values) if values else 0.0

    def as_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["objects"] = list(self.objects)
        data["quality_score"] = round(self.quality_score, 3)
        return data


def normalize_vision(raw: Mapping[str, Any] | None, *, provider: str = "") -> PhotoVision:
    """Coerce any provider answer into the closed schema; unknown → 其他/无法确认."""
    raw = dict(raw or {})
    room = str(raw.get("room_type") or "").strip()
    room = LEGACY_ROOM.get(room.lower(), room)
    if room == "餐厅":
        room = "客厅"
    if room not in ROOM_TYPES:
        room = "其他"
    hint = str(raw.get("property_type_hint") or "").strip()
    if hint not in PROPERTY_HINTS:
        hint = "无法确认"
    quality_raw = raw.get("quality") if isinstance(raw.get("quality"), Mapping) else {}
    quality = {
        key: round(_clamp(quality_raw.get(key), 0.5), 3)
        for key in ("sharpness", "brightness", "composition", "completeness")
    }
    objects = tuple(
        dict.fromkeys(str(item).strip() for item in (raw.get("objects") or ()) if str(item).strip() in OBJECT_VOCAB)
    )
    return PhotoVision(
        room_type=room,
        property_type_hint=hint,
        quality=quality,
        hero_score=round(_clamp(raw.get("hero_score")), 3),
        obstruction_score=round(_clamp(raw.get("obstruction_score")), 3),
        distortion_score=round(_clamp(raw.get("distortion_score")), 3),
        duplicate_group=str(raw.get("duplicate_group") or "").strip()[:40],
        confidence=round(_clamp(raw.get("confidence")), 3),
        objects=objects,
        view=bool(raw.get("view")),
        reason=str(raw.get("reason") or "").strip()[:200],
        provider=str(raw.get("provider") or provider or ""),
    )


class VisionClient(Protocol):
    name: str

    def analyze(self, path: str, *, ranking_row: Mapping[str, Any] | None = None) -> PhotoVision: ...


# ---------------------------------------------------------------- heuristic
class HeuristicVisionClient:
    """No-network fallback. Never confident: labels stay hidden downstream."""

    name = "heuristic"

    def analyze(self, path: str, *, ranking_row: Mapping[str, Any] | None = None) -> PhotoVision:
        row = dict(ranking_row or {})
        room_scores = row.get("room") if isinstance(row.get("room"), Mapping) else {}
        label = str(row.get("room_label") or "")
        room = LEGACY_ROOM.get(label, "其他")
        top = float(room_scores.get(label) or 0.0) if label else 0.0
        pct = lambda key, default=50.0: _clamp(float(row.get(key) or default) / 100.0)  # noqa: E731
        return normalize_vision(
            {
                "room_type": room,
                "property_type_hint": "无法确认",
                "quality": {
                    "sharpness": pct("sharpness"),
                    "brightness": (pct("brightness") + pct("exposure")) / 2,
                    "composition": (pct("orientation") + pct("space")) / 2,
                    "completeness": pct("space"),
                },
                "hero_score": pct("score", 50.0) if row.get("score") is not None else 0.5,
                "obstruction_score": 0.3,
                "distortion_score": 0.3,
                "confidence": min(HEURISTIC_CONFIDENCE_CAP, top * 0.5),
                "reason": "颜色/线条启发式（未接视觉模型，标签不显示）",
            },
            provider=self.name,
        )


# ---------------------------------------------------------------- fixtures
FIXTURE_CROP_STEPS = tuple(round(0.02 * i, 2) for i in range(16))  # bottom crops 0–30 %


def perceptual_key(path: str | Path, size: int = 16, *, bottom_crop: float = 0.0) -> str:
    """256-bit dHash hex; stable across re-encode / resize of the same photo.

    ``bottom_crop`` lets fixtures pre-compute keys for the watermark-strip crop
    the source scrubber may apply, so a recorded photo still matches after it.
    """
    with Image.open(path) as source:
        image = ImageOps.exif_transpose(source).convert("L")
        if bottom_crop > 0:
            image = image.crop((0, 0, image.width, max(1, round(image.height * (1 - bottom_crop)))))
        image = image.resize((size + 1, size), Image.Resampling.LANCZOS)
        pixels = np.asarray(image, dtype=np.int16)
    bits = 0
    for flag in (pixels[:, :-1] > pixels[:, 1:]).ravel():
        bits = (bits << 1) | int(flag)
    return f"{bits:0{size * size // 4}x}"


def _key_distance(a: str, b: str) -> int:
    try:
        return (int(a, 16) ^ int(b, 16)).bit_count()
    except ValueError:
        return 10_000


class FixtureVisionClient:
    """Recorded results (tests / local evidence). Unknown photos → heuristic."""

    name = "fixture"
    MAX_DISTANCE = 40  # of 256 bits

    def __init__(self, fixture_dir: str | Path):
        self.records: list[tuple[str, dict[str, Any]]] = []
        for file in sorted(Path(fixture_dir).glob("*.json")):
            data = json.loads(file.read_text(encoding="utf-8"))
            for record in data.get("photos", []):
                keys = record.get("perceptual_keys") or [record["perceptual_key"]]
                for key in keys:
                    self.records.append((str(key), dict(record["vision"])))
        self.fallback = HeuristicVisionClient()

    def analyze(self, path: str, *, ranking_row: Mapping[str, Any] | None = None) -> PhotoVision:
        key = perceptual_key(path)
        best = min(self.records, key=lambda item: _key_distance(key, item[0]), default=None)
        if best is not None and _key_distance(key, best[0]) <= self.MAX_DISTANCE:
            return normalize_vision(best[1], provider=self.name)
        return self.fallback.analyze(path, ranking_row=ranking_row)


# ---------------------------------------------------------------- real API
SYSTEM_PROMPT = (
    "你是柬埔寨金边租房平台的房源照片审核员。只根据照片里看得见的内容判断，看不清或不确定就返回“其他”/“无法确认”，"
    "并给低 confidence。不要猜测，不要编造。只输出一个 JSON 对象，不要任何其他文字。"
)
USER_PROMPT = (
    "分析这张房源照片，返回 JSON：\n"
    "{\"room_type\": 取值 " + "/".join(ROOM_TYPES) + "（餐厅归客厅；阳台看到城市景观仍是阳台）,\n"
    " \"property_type_hint\": 公寓/别墅/无法确认,\n"
    " \"objects\": 看得见的物件，只能从此列表选: " + "、".join(OBJECT_VOCAB) + ",\n"
    " \"view\": 是否有明显窗外/阳台景观 true/false,\n"
    " \"quality\": {\"sharpness\":0-1, \"brightness\":0-1, \"composition\":0-1, \"completeness\":0-1(主体是否完整，外观是否拍全建筑)},\n"
    " \"hero_score\": 0-1 适合作为频道封面大图的程度,\n"
    " \"obstruction_score\": 0-1 主体被树/车/人/杂物遮挡程度,\n"
    " \"distortion_score\": 0-1 广角/仰拍/倾斜造成的变形程度,\n"
    " \"duplicate_group\": 用几个字描述拍摄位置（同一位置同一角度给相同描述）,\n"
    " \"confidence\": 0-1 对 room_type 的把握,\n"
    " \"reason\": 一句中文理由}"
)


def _encode_image(path: str, max_side: int = 768) -> str:
    with Image.open(path) as source:
        image = ImageOps.exif_transpose(source).convert("RGB")
    image.thumbnail((max_side, max_side), Image.Resampling.LANCZOS)
    buffer = io.BytesIO()
    image.save(buffer, format="JPEG", quality=82)
    return base64.b64encode(buffer.getvalue()).decode("ascii")


def _extract_json(text: str) -> dict[str, Any]:
    text = str(text or "").strip()
    if text.startswith("```"):
        text = text.strip("`")
        text = text[text.find("{"):]
    start, end = text.find("{"), text.rfind("}")
    if start < 0 or end <= start:
        raise ValueError("vision_answer_not_json")
    return json.loads(text[start:end + 1])


# Structured-output schema (OpenAPI subset accepted by Gemini responseSchema and
# by OpenAI-style json_schema). Closed enums mean the model cannot invent rooms.
def _unit(description: str) -> dict[str, Any]:
    return {"type": "number", "description": description}


RESPONSE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "room_type": {"type": "string", "enum": list(ROOM_TYPES)},
        "property_type_hint": {"type": "string", "enum": list(PROPERTY_HINTS)},
        "objects": {"type": "array", "items": {"type": "string", "enum": list(OBJECT_VOCAB)}},
        "view": {"type": "boolean"},
        "quality": {
            "type": "object",
            "properties": {
                "sharpness": _unit("0-1 清晰度"),
                "brightness": _unit("0-1 亮度/曝光合适程度"),
                "composition": _unit("0-1 构图"),
                "completeness": _unit("0-1 主体完整度；外观是否拍全建筑"),
            },
            "required": ["sharpness", "brightness", "composition", "completeness"],
        },
        "hero_score": _unit("0-1 适合做频道封面大图"),
        "obstruction_score": _unit("0-1 主体被遮挡程度"),
        "distortion_score": _unit("0-1 广角/仰拍/倾斜变形"),
        "duplicate_group": {"type": "string"},
        "confidence": _unit("0-1 对 room_type 的把握"),
        "reason": {"type": "string"},
    },
    "required": [
        "room_type", "property_type_hint", "objects", "view", "quality", "hero_score",
        "obstruction_score", "distortion_score", "duplicate_group", "confidence", "reason",
    ],
}

RETRYABLE_STATUS = frozenset({408, 409, 425, 429, 500, 502, 503, 504})


class VisionProviderError(RuntimeError):
    def __init__(self, message: str, *, status: int = 0, retry_after: float | None = None):
        super().__init__(message)
        self.status = status
        self.retry_after = retry_after
        self.daily_quota = False


def _http_post_json(url: str, payload: dict[str, Any], headers: Mapping[str, str], timeout: float) -> dict[str, Any]:
    """POST JSON; HTTP errors become VisionProviderError (status + Retry-After). Never logs headers."""
    request = Request(url, data=json.dumps(payload).encode("utf-8"),
                      headers={"Content-Type": "application/json", **headers}, method="POST")
    try:
        with urlopen(request, timeout=timeout) as response:  # noqa: S310 - fixed https endpoint from env
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        retry_after = None
        raw_after = exc.headers.get("Retry-After") if exc.headers else None
        try:
            retry_after = float(raw_after) if raw_after else None
        except ValueError:
            retry_after = None
        try:
            detail = json.loads(exc.read().decode("utf-8", "replace")).get("error", {})
            message = str(detail.get("status") or detail.get("message") or "")[:120]
            quota = ""
            for item in detail.get("details") or ():
                delay = str(item.get("retryDelay") or "")
                if delay.endswith("s") and retry_after is None:
                    retry_after = float(delay[:-1] or 0)
                for violation in item.get("violations") or ():
                    quota = quota or str(violation.get("quotaId") or "")
        except Exception:
            message, quota = "", ""
        error = VisionProviderError(f"http_{exc.code} {message}".strip(), status=exc.code, retry_after=retry_after)
        error.daily_quota = "PerDay" in quota
        raise error from None
    except (URLError, TimeoutError, socket.timeout) as exc:
        raise VisionProviderError(f"network_{type(exc).__name__}", status=0) from None


class _RemoteVisionClient:
    """Shared retry / backoff / model fallback / usage accounting for network providers.

    * 429 / 5xx / timeouts: exponential backoff (honours Retry-After / retryDelay),
      up to ``max_attempts`` per photo.
    * 404 (model not offered to this key): next model in the list, remembered.
    * 400 / 401 / 403: not retried; 401/403 trip a breaker so the rest of the
      listing degrades instantly instead of hammering the API.
    * Anything else that fails → that photo degrades to the heuristic (labels hidden).
    """

    name = "remote"
    BACKOFF_BASE = 1.5

    def __init__(self, *, api_key: str, models: Iterable[str] | str, timeout: float = 30.0,
                 max_attempts: int = 4, transport=None, sleep=None, rpm: float = 0.0,
                 max_backoff: float = 60.0):
        model_list = [m.strip() for m in (models.split(",") if isinstance(models, str) else models) if m and m.strip()]
        if not api_key or not model_list:
            raise ValueError("vision_provider_requires_key_and_model")
        self._api_key = api_key
        self.models = model_list
        self.timeout = float(timeout)
        self.max_attempts = max(1, int(max_attempts))
        self._transport = transport
        self._sleep = sleep or time.sleep
        self._lock = threading.RLock()
        self._unavailable: set[str] = set()
        self._broken = ""
        self._min_interval = 60.0 / float(rpm) if rpm and float(rpm) > 0 else 0.0
        self._next_slot = 0.0
        self.backoff_cap = float(max_backoff)
        self.usage_log: list[dict[str, Any]] = []
        self.fallback = HeuristicVisionClient()

    def __repr__(self) -> str:  # never expose the key
        return f"<{type(self).__name__} models={self.models}>"

    @property
    def model(self) -> str:
        with self._lock:
            for candidate in self.models:
                if candidate not in self._unavailable:
                    return candidate
        return self.models[-1]

    # provider hooks
    def _request(self, model: str, path: str) -> tuple[str, dict[str, Any], dict[str, str]]:
        raise NotImplementedError

    def _parse(self, answer: dict[str, Any]) -> tuple[str, dict[str, int]]:
        raise NotImplementedError

    def _send(self, url: str, payload: dict[str, Any], headers: dict[str, str]) -> dict[str, Any]:
        if self._transport is not None:
            return self._transport(url, payload)
        return _http_post_json(url, payload, headers, self.timeout)

    def _delay(self, attempt: int, error: VisionProviderError | None) -> float:
        if error is not None and error.retry_after:
            return min(self.backoff_cap, max(0.5, error.retry_after + random.random()))
        return min(self.backoff_cap, self.BACKOFF_BASE * (2 ** attempt)) * (0.75 + random.random() * 0.5)

    def _throttle(self) -> None:
        """Optional client-side pacing (V3_VISION_RPM) shared by all worker threads."""
        if not self._min_interval:
            return
        with self._lock:
            now = time.monotonic()
            slot = max(now, self._next_slot)
            self._next_slot = slot + self._min_interval
        if slot > now:
            self._sleep(slot - now)

    def analyze(self, path: str, *, ranking_row: Mapping[str, Any] | None = None) -> PhotoVision:
        if self._broken:
            return self.fallback.analyze(path, ranking_row=ranking_row)
        last = "unknown"
        attempt = 0
        while attempt < self.max_attempts:
            model = self.model
            started = time.monotonic()
            try:
                url, payload, headers = self._request(model, path)
                self._throttle()
                started = time.monotonic()
                answer = self._send(url, payload, headers)
                text, usage = self._parse(answer)
                result = normalize_vision(_extract_json(text), provider=f"{self.name}:{model}")
                with self._lock:
                    self.usage_log.append({"photo": Path(path).name, "model": model, "attempts": attempt + 1,
                                           "latency_s": round(time.monotonic() - started, 3), **usage})
                return result
            except VisionProviderError as exc:
                last = str(exc)
                if exc.status == 404 and model not in self._unavailable and len(self.models) > 1:
                    with self._lock:
                        self._unavailable.add(model)
                    if any(m not in self._unavailable for m in self.models):
                        continue  # try the next model without spending an attempt
                if exc.status in (401, 403) or exc.daily_quota:
                    self._broken = last  # bad key / daily quota gone: degrade the rest instantly
                    break
                if exc.status and exc.status not in RETRYABLE_STATUS:
                    break
                attempt += 1
                if attempt < self.max_attempts:
                    self._sleep(self._delay(attempt - 1, exc))
            except Exception as exc:  # malformed / blocked answer: one more try
                last = type(exc).__name__
                attempt += 1
        LOGGER.warning("vision_provider_failed provider=%s photo=%s error=%s", self.name, Path(path).name, last)
        with self._lock:
            self.usage_log.append({"photo": Path(path).name, "model": self.model, "failed": last})
        return self.fallback.analyze(path, ranking_row=ranking_row)


class OpenAICompatibleVisionClient(_RemoteVisionClient):
    """Any OpenAI-compatible /chat/completions endpoint with image input (stdlib only).

    Works with Gemini via base_url https://generativelanguage.googleapis.com/v1beta/openai/.
    """

    name = "openai_compat"

    def __init__(self, *, api_key: str, model: str, base_url: str = "https://api.openai.com/v1",
                 timeout: float = 30.0, transport=None, max_attempts: int = 4, sleep=None, **pacing):
        super().__init__(api_key=api_key, models=model, timeout=timeout, max_attempts=max_attempts,
                         transport=transport, sleep=sleep, **pacing)
        self.base_url = base_url.rstrip("/")

    def payload(self, path: str, model: str | None = None) -> dict[str, Any]:
        return {
            "model": model or self.model,
            "temperature": 0,
            "max_tokens": 600,
            "response_format": {"type": "json_object"},
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": [
                    {"type": "text", "text": USER_PROMPT},
                    {"type": "image_url", "image_url": {"url": "data:image/jpeg;base64," + _encode_image(path),
                                                        "detail": "low"}},
                ]},
            ],
        }

    def _request(self, model: str, path: str):
        return (self.base_url + "/chat/completions", self.payload(path, model),
                {"Authorization": f"Bearer {self._api_key}"})

    def _parse(self, answer: dict[str, Any]):
        content = answer["choices"][0]["message"]["content"]
        if isinstance(content, list):
            content = "".join(str(part.get("text") or "") for part in content if isinstance(part, dict))
        usage = answer.get("usage") or {}
        return content, {"input_tokens": int(usage.get("prompt_tokens") or 0),
                         "output_tokens": int(usage.get("completion_tokens") or 0), "thinking_tokens": 0}


class GeminiVisionClient(_RemoteVisionClient):
    """Native Gemini generateContent with responseSchema (structured JSON output).

    Key goes in the ``x-goog-api-key`` header (never in the URL, so it cannot leak
    into access logs / exception text).
    """

    name = "gemini"
    DEFAULT_BASE = "https://generativelanguage.googleapis.com/v1beta"

    def __init__(self, *, api_key: str, model: str, base_url: str = DEFAULT_BASE, timeout: float = 30.0,
                 thinking: str = "", max_side: int = 768, transport=None, max_attempts: int = 4, sleep=None,
                 **pacing):
        super().__init__(api_key=api_key, models=model, timeout=timeout, max_attempts=max_attempts,
                         transport=transport, sleep=sleep, **pacing)
        base = base_url.rstrip("/")
        if base.endswith("/openai"):
            base = base[: -len("/openai")]
        self.base_url = base
        self.thinking = str(thinking or "").strip().lower()
        self.max_side = int(max_side)

    def payload(self, path: str, model: str | None = None) -> dict[str, Any]:
        config: dict[str, Any] = {
            "temperature": 0,
            "maxOutputTokens": 2048,
            "responseMimeType": "application/json",
            "responseSchema": RESPONSE_SCHEMA,
        }
        if self.thinking in {"off", "none", "0"}:
            config["thinkingConfig"] = {"thinkingBudget": 0}
        elif self.thinking.isdigit():
            config["thinkingConfig"] = {"thinkingBudget": int(self.thinking)}
        elif self.thinking:
            config["thinkingConfig"] = {"thinkingLevel": self.thinking}
        return {
            "systemInstruction": {"parts": [{"text": SYSTEM_PROMPT}]},
            "contents": [{"role": "user", "parts": [
                {"text": USER_PROMPT},
                {"inline_data": {"mime_type": "image/jpeg", "data": _encode_image(path, self.max_side)}},
            ]}],
            "generationConfig": config,
        }

    def _request(self, model: str, path: str):
        return (f"{self.base_url}/models/{model}:generateContent", self.payload(path, model),
                {"x-goog-api-key": self._api_key})

    def _parse(self, answer: dict[str, Any]):
        candidates = answer.get("candidates") or []
        if not candidates:
            reason = (answer.get("promptFeedback") or {}).get("blockReason") or "no_candidates"
            raise ValueError(f"gemini_{reason}")
        parts = (candidates[0].get("content") or {}).get("parts") or []
        text = "".join(str(part.get("text") or "") for part in parts if not part.get("thought"))
        meta = answer.get("usageMetadata") or {}
        return text, {"input_tokens": int(meta.get("promptTokenCount") or 0),
                      "output_tokens": int(meta.get("candidatesTokenCount") or 0),
                      "thinking_tokens": int(meta.get("thoughtsTokenCount") or 0)}


# ---------------------------------------------------------------- factory + cache
GOOGLE_HOST = "generativelanguage.googleapis.com"


def _api_key(env: Mapping[str, str], *, google: bool) -> str:
    key = str(env.get("V3_VISION_API_KEY") or "").strip()
    if not key and google:
        key = str(env.get("GEMINI_API_KEY") or "").strip()
    return key


def vision_client_from_env(env: Mapping[str, str] | None = None) -> VisionClient:
    env = os.environ if env is None else env
    provider = str(env.get("V3_VISION_PROVIDER") or "").strip().lower()
    timeout = float(env.get("V3_VISION_TIMEOUT") or 30)
    attempts = int(env.get("V3_VISION_MAX_ATTEMPTS") or 4)
    pacing = {"rpm": float(env.get("V3_VISION_RPM") or 0), "max_backoff": float(env.get("V3_VISION_MAX_BACKOFF") or 60)}
    try:
        if provider == "gemini":
            return GeminiVisionClient(
                api_key=_api_key(env, google=True),
                model=str(env.get("V3_VISION_MODEL") or "gemini-3.5-flash-lite"),
                base_url=str(env.get("V3_VISION_BASE_URL") or GeminiVisionClient.DEFAULT_BASE),
                thinking=str(env.get("V3_VISION_THINKING") or ""),
                timeout=timeout,
                max_attempts=attempts,
                **pacing,
            )
        if provider == "openai_compat" or (not provider and env.get("V3_VISION_API_KEY")):
            base_url = str(env.get("V3_VISION_BASE_URL") or "https://api.openai.com/v1")
            return OpenAICompatibleVisionClient(
                api_key=_api_key(env, google=GOOGLE_HOST in base_url),
                model=str(env.get("V3_VISION_MODEL") or ""),
                base_url=base_url,
                timeout=timeout,
                max_attempts=attempts,
                **pacing,
            )
        if provider == "fixture" and env.get("V3_VISION_FIXTURE_DIR"):
            return FixtureVisionClient(str(env["V3_VISION_FIXTURE_DIR"]))
    except Exception as exc:
        LOGGER.warning("vision_provider_config_invalid provider=%s error=%s", provider, type(exc).__name__)
    return HeuristicVisionClient()


def _cache_path(path: str, client: VisionClient) -> Path:
    tag = getattr(client, "model", "") or client.name
    safe = "".join(ch if ch.isalnum() or ch in "-_." else "_" for ch in f"{client.name}_{tag}")
    return Path(path).with_name(Path(path).name + f".{PROMPT_VERSION}.{safe}.vision.json")


def analyze_photos(
    paths: Iterable[str],
    client: VisionClient,
    *,
    ranking: Iterable[Mapping[str, Any]] = (),
    use_cache: bool = True,
    max_workers: int = 4,
) -> dict[str, PhotoVision]:
    """Analyse every photo once (cached per photo/provider/prompt version).

    Network providers run up to ``max_workers`` requests in parallel; a failed
    request degrades that photo to the heuristic (labels hidden), never the listing.
    """
    rows = {str(Path(str(item.get("file") or "")).resolve()): item for item in ranking}
    ordered = [str(Path(str(raw)).resolve()) for raw in paths]
    results: dict[str, PhotoVision] = {}
    pending: list[str] = []
    for path in ordered:
        cache = _cache_path(path, client)
        if use_cache and client.name not in {"heuristic", "fixture"} and cache.is_file():
            try:
                results[path] = normalize_vision(json.loads(cache.read_text(encoding="utf-8")))
                continue
            except Exception:
                pass
        pending.append(path)

    def _one(path: str) -> tuple[str, PhotoVision]:
        return path, client.analyze(path, ranking_row=rows.get(path))

    if client.name in {"heuristic", "fixture"} or max_workers <= 1 or len(pending) <= 1:
        done = [_one(path) for path in pending]
    else:
        from concurrent.futures import ThreadPoolExecutor

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            done = list(pool.map(_one, pending))
    for path, vision in done:
        results[path] = vision
        if use_cache and client.name not in {"heuristic", "fixture"} and not vision.provider.startswith("heuristic"):
            try:
                _cache_path(path, client).write_text(json.dumps(vision.as_dict(), ensure_ascii=False), encoding="utf-8")
            except OSError:
                pass
    return {path: results[path] for path in ordered}


__all__ = [
    "FixtureVisionClient",
    "GeminiVisionClient",
    "RESPONSE_SCHEMA",
    "VisionProviderError",
    "HeuristicVisionClient",
    "OBJECT_VOCAB",
    "OpenAICompatibleVisionClient",
    "PhotoVision",
    "ROOM_TYPES",
    "analyze_photos",
    "normalize_vision",
    "perceptual_key",
    "vision_client_from_env",
]
