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

    V3_VISION_PROVIDER   "openai_compat" | "fixture" | "heuristic" (default)
    V3_VISION_API_KEY    bearer key for openai_compat (OpenAI / xAI / OpenRouter /
                         DashScope compatible-mode / any /chat/completions API)
    V3_VISION_BASE_URL   e.g. https://api.x.ai/v1 (default https://api.openai.com/v1)
    V3_VISION_MODEL      vision-capable model id (required for openai_compat)
    V3_VISION_TIMEOUT    seconds per request (default 30)
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
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping, Protocol
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


class OpenAICompatibleVisionClient:
    """Any OpenAI-compatible /chat/completions endpoint with image input (stdlib only)."""

    name = "openai_compat"

    def __init__(self, *, api_key: str, model: str, base_url: str = "https://api.openai.com/v1",
                 timeout: float = 30.0, transport=None):
        if not api_key or not model:
            raise ValueError("vision_provider_requires_key_and_model")
        self._api_key = api_key
        self.model = model
        self.base_url = base_url.rstrip("/")
        self.timeout = float(timeout)
        self._transport = transport or self._post
        self.fallback = HeuristicVisionClient()

    def _post(self, url: str, payload: dict[str, Any]) -> dict[str, Any]:
        request = Request(
            url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Authorization": f"Bearer {self._api_key}", "Content-Type": "application/json"},
            method="POST",
        )
        with urlopen(request, timeout=self.timeout) as response:  # noqa: S310 - fixed https endpoint from env
            return json.loads(response.read().decode("utf-8"))

    def payload(self, path: str) -> dict[str, Any]:
        return {
            "model": self.model,
            "temperature": 0,
            "max_tokens": 400,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": [
                    {"type": "text", "text": USER_PROMPT},
                    {"type": "image_url", "image_url": {"url": "data:image/jpeg;base64," + _encode_image(path),
                                                        "detail": "low"}},
                ]},
            ],
        }

    def analyze(self, path: str, *, ranking_row: Mapping[str, Any] | None = None) -> PhotoVision:
        last_error: Exception | None = None
        for _attempt in range(2):
            try:
                answer = self._transport(self.base_url + "/chat/completions", self.payload(path))
                content = answer["choices"][0]["message"]["content"]
                if isinstance(content, list):
                    content = "".join(str(part.get("text") or "") for part in content if isinstance(part, dict))
                result = normalize_vision(_extract_json(content), provider=f"{self.name}:{self.model}")
                usage = answer.get("usage") if isinstance(answer, dict) else None
                if usage:
                    LOGGER.info("vision usage %s", {k: usage.get(k) for k in ("prompt_tokens", "completion_tokens")})
                return result
            except Exception as exc:  # network / quota / malformed answer
                last_error = exc
        LOGGER.warning("vision_provider_failed path=%s error=%s", Path(path).name, type(last_error).__name__)
        return self.fallback.analyze(path, ranking_row=ranking_row)


# ---------------------------------------------------------------- factory + cache
def vision_client_from_env(env: Mapping[str, str] | None = None) -> VisionClient:
    env = os.environ if env is None else env
    provider = str(env.get("V3_VISION_PROVIDER") or "").strip().lower()
    try:
        if provider == "openai_compat" or (not provider and env.get("V3_VISION_API_KEY")):
            return OpenAICompatibleVisionClient(
                api_key=str(env.get("V3_VISION_API_KEY") or ""),
                model=str(env.get("V3_VISION_MODEL") or ""),
                base_url=str(env.get("V3_VISION_BASE_URL") or "https://api.openai.com/v1"),
                timeout=float(env.get("V3_VISION_TIMEOUT") or 30),
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
