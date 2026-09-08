from __future__ import annotations

import pytest

from v3_core.user_bot.transition_callbacks import (
    TRANSITION_PREFIX,
    encode_transition_choice,
    parse_transition_callback,
)
from v3_core.user_bot.transition_views import TransitionChoice


PUBLIC_ID = "QL-RF-A2B3"


def test_codec_uses_only_v3_namespaces_and_reuses_existing_listing_and_change_search_contracts():
    choices = (
        TransitionChoice("今天", "appointment_date", "09-08"),
        TransitionChoice("📅 其他日期", "appointment_other_date"),
        TransitionChoice("🎥 改为视频看房", "appointment_mode", "video"),
        TransitionChoice("下午 14:00–17:00", "appointment_time", "pm"),
        TransitionChoice("✍️ 其他时间", "appointment_other_time"),
        TransitionChoice("⬅️ 修改日期", "appointment_back_date"),
        TransitionChoice("🏠 返回首页", "home"),
        TransitionChoice("$400–600", "budget_choice", "b2", budget_min=400, budget_max=600),
        TransitionChoice("✍️ 自己输入", "budget_custom"),
        TransitionChoice("📍 按区域", "search_area"),
        TransitionChoice("💰 按预算", "search_budget"),
        TransitionChoice("🏠 按户型", "search_layout"),
        TransitionChoice("🏘 当前可约", "search_available"),
    )

    encoded = [encode_transition_choice(choice) for choice in choices]

    assert all(value.startswith(f"{TRANSITION_PREFIX}:") for value in encoded)
    assert all(len(value.encode("utf-8")) <= 64 for value in encoded)
    assert not any(value.startswith(("apdate:", "apmode:", "aptime:", "findbudget:", "hub:")) for value in encoded)
    assert encode_transition_choice(
        TransitionChoice("⬅️ 返回房源", "listing_details", public_listing_id=PUBLIC_ID)
    ) == f"v3u:listing:details:{PUBLIC_ID}"
    assert encode_transition_choice(
        TransitionChoice("⬅️ 返回", "change_search")
    ) == "v3u:change_search"


def test_value_callbacks_round_trip_and_reject_malformed_values():
    for kind, value in (
        ("appointment_date", "09-08"),
        ("appointment_mode", "video"),
        ("appointment_mode", "offline"),
        ("appointment_time", "am"),
        ("appointment_time", "pm"),
        ("appointment_time", "evening"),
        ("budget_choice", "b6"),
    ):
        raw = encode_transition_choice(TransitionChoice("x", kind, value))
        parsed = parse_transition_callback(raw)
        assert parsed is not None
        assert parsed.kind == kind
        assert parsed.value == value

    for raw in (
        "v3u:t:appointment_date:13-08",
        "v3u:t:appointment_date:09-32",
        "v3u:t:appointment_date:9-8",
        "v3u:t:appointment_mode:walk",
        "v3u:t:appointment_time:noon",
        "v3u:t:budget_choice:b7",
        "v3u:t:budget_choice",
        "v3u:t:home:extra",
    ):
        assert parse_transition_callback(raw) is None


def test_flag_callbacks_round_trip_without_values():
    for kind in (
        "appointment_other_date",
        "appointment_other_time",
        "appointment_back_date",
        "home",
        "budget_custom",
        "search_area",
        "search_budget",
        "search_layout",
        "search_available",
    ):
        raw = encode_transition_choice(TransitionChoice("x", kind))
        parsed = parse_transition_callback(raw)
        assert parsed is not None
        assert parsed.kind == kind
        assert parsed.value == ""


def test_non_transition_v3_and_legacy_callbacks_are_not_claimed_by_transition_parser():
    for raw in (
        f"v3u:listing:details:{PUBLIC_ID}",
        "v3u:change_search",
        "apdate:09-08",
        "aptime:pm",
        "findbudget:b2",
        "home",
        "",
    ):
        assert parse_transition_callback(raw) is None


def test_encoder_fails_closed_on_unsupported_or_invalid_choice():
    with pytest.raises(ValueError, match="invalid_transition_appointment_date"):
        encode_transition_choice(TransitionChoice("今天", "appointment_date", "13-01"))
    with pytest.raises(ValueError, match="invalid_transition_appointment_time"):
        encode_transition_choice(TransitionChoice("时间", "appointment_time", "noon"))
    with pytest.raises(ValueError, match="invalid_transition_budget_choice"):
        encode_transition_choice(TransitionChoice("预算", "budget_choice", "b9"))
    with pytest.raises(ValueError, match="flag_transition_callback_must_not_have_value"):
        encode_transition_choice(TransitionChoice("首页", "home", "x"))
    with pytest.raises(ValueError, match="unsupported_transition_choice"):
        encode_transition_choice(TransitionChoice("未知", "unsupported"))  # type: ignore[arg-type]
