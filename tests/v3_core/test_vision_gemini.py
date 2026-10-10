"""Gemini vision provider + label-safety rules (no network: fake transports only).

Real-model evidence lives outside the repo (/workspace/cover_vision/real_gemini_*.json);
these tests pin the request shape, retry/degrade behaviour and key handling.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from v3_core.media import cover_plan as cp
from v3_core.media.cover_plan import cross_validate
from v3_core.media.vision import (
    RESPONSE_SCHEMA,
    ROOM_TYPES,
    GeminiVisionClient,
    OpenAICompatibleVisionClient,
    VisionProviderError,
    analyze_photos,
    normalize_vision,
    vision_client_from_env,
)

PHOTO = str(Path(__file__).parent / "fixtures" / "cover_vision" / "photos" / "7117" / "04.jpg")
ANSWER = {"room_type": "厨房", "property_type_hint": "公寓", "objects": ["灶台", "水槽", "橱柜"], "view": False,
          "quality": {"sharpness": 0.9, "brightness": 0.8, "composition": 0.7, "completeness": 0.8},
          "hero_score": 0.4, "obstruction_score": 0.05, "distortion_score": 0.2, "duplicate_group": "厨房",
          "confidence": 0.95, "reason": "橱柜+水槽+灶台"}


def _gemini_answer(payload=ANSWER, usage=(1500, 180, 0)):
    return {"candidates": [{"content": {"parts": [{"text": json.dumps(payload, ensure_ascii=False)}]}}],
            "usageMetadata": {"promptTokenCount": usage[0], "candidatesTokenCount": usage[1],
                              "thoughtsTokenCount": usage[2]},
            "modelVersion": "fake"}


def test_env_gemini_reads_key_from_env_with_gemini_fallback():
    client = vision_client_from_env({"V3_VISION_PROVIDER": "gemini", "GEMINI_API_KEY": "g-secret",
                                     "V3_VISION_MODEL": "gemini-2.5-flash"})
    assert isinstance(client, GeminiVisionClient) and client.model == "gemini-2.5-flash"
    assert "g-secret" not in repr(client)
    own = vision_client_from_env({"V3_VISION_PROVIDER": "gemini", "V3_VISION_API_KEY": "own",
                                  "GEMINI_API_KEY": "g"})
    assert own._api_key == "own"                                   # V3_VISION_API_KEY wins
    assert vision_client_from_env({"V3_VISION_PROVIDER": "gemini"}).name == "heuristic"  # no key → safe
    compat = vision_client_from_env({
        "V3_VISION_PROVIDER": "openai_compat", "GEMINI_API_KEY": "g", "V3_VISION_MODEL": "gemini-2.5-flash",
        "V3_VISION_BASE_URL": "https://generativelanguage.googleapis.com/v1beta/openai/"})
    assert isinstance(compat, OpenAICompatibleVisionClient) and compat._api_key == "g"
    # GEMINI_API_KEY is never sent to a non-Google endpoint
    other = vision_client_from_env({"V3_VISION_PROVIDER": "openai_compat", "GEMINI_API_KEY": "g",
                                    "V3_VISION_MODEL": "m", "V3_VISION_BASE_URL": "https://api.example.com/v1"})
    assert other.name == "heuristic"


def test_gemini_request_uses_structured_output_and_header_key():
    client = GeminiVisionClient(api_key="secret-key", model="gemini-2.5-flash", thinking="minimal")
    url, payload, headers = client._request("gemini-2.5-flash", PHOTO)
    assert url == "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent"
    assert "secret-key" not in url and headers == {"x-goog-api-key": "secret-key"}
    config = payload["generationConfig"]
    assert config["responseMimeType"] == "application/json" and config["responseSchema"] is RESPONSE_SCHEMA
    assert config["temperature"] == 0 and config["thinkingConfig"] == {"thinkingLevel": "minimal"}
    assert RESPONSE_SCHEMA["properties"]["room_type"]["enum"] == list(ROOM_TYPES)
    parts = payload["contents"][0]["parts"]
    assert parts[1]["inline_data"]["mime_type"] == "image/jpeg" and len(parts[1]["inline_data"]["data"]) > 1000
    off = GeminiVisionClient(api_key="k", model="m", thinking="off").payload(PHOTO)
    assert off["generationConfig"]["thinkingConfig"] == {"thinkingBudget": 0}
    # OpenAI-compatible base URL given to the native client is normalised
    assert GeminiVisionClient(api_key="k", model="m",
                              base_url="https://generativelanguage.googleapis.com/v1beta/openai/").base_url \
        == "https://generativelanguage.googleapis.com/v1beta"


def test_gemini_parses_answer_and_records_usage():
    client = GeminiVisionClient(api_key="k", model="gemini-2.5-flash", transport=lambda url, payload: _gemini_answer())
    result = client.analyze(PHOTO)
    assert result.room_type == "厨房" and result.provider == "gemini:gemini-2.5-flash"
    assert client.usage_log[0]["input_tokens"] == 1500 and client.usage_log[0]["output_tokens"] == 180


def test_rate_limit_retries_honour_retry_delay_then_succeed():
    calls, sleeps = [], []

    def transport(url, payload):
        calls.append(url)
        if len(calls) < 3:
            raise VisionProviderError("http_429 RESOURCE_EXHAUSTED", status=429, retry_after=32.0)
        return _gemini_answer()

    client = GeminiVisionClient(api_key="k", model="m", transport=transport, sleep=sleeps.append)
    assert client.analyze(PHOTO).room_type == "厨房"
    assert len(calls) == 3 and len(sleeps) == 2 and all(32 <= s <= 33.5 for s in sleeps)
    assert client.usage_log[-1]["attempts"] == 3


def test_persistent_failure_degrades_photo_to_hidden_labels():
    sleeps = []

    def transport(url, payload):
        raise VisionProviderError("network_TimeoutError")

    client = GeminiVisionClient(api_key="k", model="m", transport=transport, sleep=sleeps.append, max_attempts=3)
    result = client.analyze(PHOTO)
    assert result.provider == "heuristic" and result.confidence < cp.LABEL_MIN_CONFIDENCE
    assert len(sleeps) == 2 and client.usage_log[-1]["failed"].startswith("network_")


def test_bad_key_and_daily_quota_trip_breaker_without_retry():
    calls = []

    def transport(url, payload):
        calls.append(url)
        raise VisionProviderError("http_403 PERMISSION_DENIED", status=403)

    client = GeminiVisionClient(api_key="k", model="m", transport=transport, sleep=lambda s: None)
    client.analyze(PHOTO)
    client.analyze(PHOTO)
    assert len(calls) == 1                       # second photo degrades instantly

    daily = VisionProviderError("http_429", status=429, retry_after=5)
    daily.daily_quota = True
    calls.clear()

    def quota(url, payload):
        calls.append(url)
        raise daily

    client = GeminiVisionClient(api_key="k", model="m", transport=quota, sleep=lambda s: None)
    assert client.analyze(PHOTO).provider == "heuristic" and len(calls) == 1


def test_model_not_available_falls_through_model_list():
    seen = []

    def transport(url, payload):
        seen.append(url.rsplit("/", 1)[-1])
        if "gemini-2.5-flash:" in url:
            raise VisionProviderError("http_404 NOT_FOUND", status=404)
        return _gemini_answer()

    client = GeminiVisionClient(api_key="k", model="gemini-2.5-flash,gemini-3.5-flash-lite", transport=transport)
    assert client.analyze(PHOTO).provider == "gemini:gemini-3.5-flash-lite"
    assert client.analyze(PHOTO).room_type == "厨房"
    assert seen == ["gemini-2.5-flash:generateContent", "gemini-3.5-flash-lite:generateContent",
                    "gemini-3.5-flash-lite:generateContent"]


def test_blocked_or_malformed_answer_degrades():
    client = GeminiVisionClient(api_key="k", model="m", sleep=lambda s: None,
                                transport=lambda url, payload: {"promptFeedback": {"blockReason": "OTHER"}})
    assert client.analyze(PHOTO).provider == "heuristic"


def test_pacing_spaces_requests():
    sleeps = []
    client = GeminiVisionClient(api_key="k", model="m", rpm=60, sleep=sleeps.append,
                                transport=lambda url, payload: _gemini_answer())
    for _ in range(3):
        client.analyze(PHOTO)
    # fake sleep does not advance the clock, so reserved slots stack: ~1 s, ~2 s
    assert len(sleeps) == 2 and 0.8 < sleeps[0] <= 1.0 and 1.8 < sleeps[1] <= 2.0


def test_parallel_failures_do_not_deadlock(tmp_path):
    photos = []
    for i in range(6):
        dst = tmp_path / f"p{i}.jpg"
        dst.write_bytes(Path(PHOTO).read_bytes())
        photos.append(str(dst))

    def transport(url, payload):
        raise VisionProviderError("http_503", status=503)

    client = GeminiVisionClient(api_key="k", model="m", transport=transport, sleep=lambda s: None, max_attempts=2)
    result = analyze_photos(photos, client, max_workers=4)
    assert len(result) == 6 and all(v.provider == "heuristic" for v in result.values())


# ------------------------------------------------- label safety found in real Gemini runs
def test_mixed_scene_keeps_room_but_hides_label():
    # real 7117/10: balcony with washing machine + a cabinet → never shown as 厨房
    room, conf, notes = cross_validate(normalize_vision(
        {"room_type": "阳台", "objects": ["橱柜", "洗衣机", "栏杆", "高楼景观"], "confidence": 0.95}))
    assert room == "阳台" and conf < cp.LABEL_MIN_CONFIDENCE and "物件冲突" in notes[0]
    # real 6735/02: garden in front of a facade → neither 庭院 nor 外观 is shown
    room, conf, _ = cross_validate(normalize_vision(
        {"room_type": "庭院", "objects": ["草坪", "树木", "立面", "屋顶"], "confidence": 0.9}))
    assert room == "庭院" and conf < cp.LABEL_MIN_CONFIDENCE


@pytest.mark.parametrize("objects,shown", [(["衣柜"], False), (["床", "衣柜"], True)])
def test_bedroom_label_needs_a_bed(objects, shown):
    room, conf, _ = cross_validate(normalize_vision({"room_type": "卧室", "objects": objects, "confidence": 0.95}))
    assert room == "卧室" and (conf >= cp.LABEL_MIN_CONFIDENCE) is shown


def test_suite_never_reaches_real_api():
    client = GeminiVisionClient(api_key="k", model="m", sleep=lambda s: None)   # no fake transport
    assert client.analyze(PHOTO).provider == "heuristic"                       # conftest blocks the HTTP layer
    assert vision_client_from_env().name == "heuristic"                        # shell env stripped
