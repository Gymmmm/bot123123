from __future__ import annotations

import pytest

from v3_core.user_bot.callbacks import (
    encode_card_callback,
    encode_change_search_callback,
    encode_listing_callback,
    encode_semantic_action,
    parse_callback,
    validate_card_navigation,
)
from v3_core.user_bot.listing_responses import SemanticAction


def test_listing_callback_round_trip_uses_public_identity_only():
    raw = encode_listing_callback("details", "ql-bk-a2b3")

    assert raw == "v3u:listing:details:QL-BK-A2B3"
    parsed = parse_callback(raw)
    assert parsed is not None
    assert parsed.kind == "listing"
    assert parsed.action == "details"
    assert parsed.public_listing_id == "QL-BK-A2B3"
    assert parsed.target_index is None
    assert len(raw.encode("utf-8")) <= 64


def test_all_supported_listing_actions_are_explicit_and_round_trip():
    for action in ("details", "photos", "book", "consult", "similar"):
        raw = encode_listing_callback(action, "QL-RF-A2B3")
        parsed = parse_callback(raw)
        assert parsed is not None
        assert parsed.kind == "listing"
        assert parsed.action == action
        assert parsed.public_listing_id == "QL-RF-A2B3"


def test_card_callback_round_trip_carries_index_and_public_id():
    raw = encode_card_callback(2, "QL-B2-E6F7")

    assert raw == "v3u:card:2:QL-B2-E6F7"
    parsed = parse_callback(raw)
    assert parsed is not None
    assert parsed.kind == "card"
    assert parsed.action == "show_card"
    assert parsed.target_index == 2
    assert parsed.public_listing_id == "QL-B2-E6F7"


def test_semantic_previous_and_next_encode_to_target_card_identity():
    previous = SemanticAction(
        "⬅️ 上一套",
        "previous",
        target_public_listing_id="QL-B2-E6F7",
        target_index=2,
    )
    next_ = SemanticAction(
        "下一套 ➡️",
        "next",
        target_public_listing_id="QL-BK-C4D5",
        target_index=1,
    )

    assert encode_semantic_action(previous) == "v3u:card:2:QL-B2-E6F7"
    assert encode_semantic_action(next_) == "v3u:card:1:QL-BK-C4D5"


def test_change_search_has_one_fixed_callback_without_listing_identity():
    raw = encode_change_search_callback()

    assert raw == "v3u:change_search"
    parsed = parse_callback(raw)
    assert parsed is not None
    assert parsed.kind == "change_search"
    assert parsed.public_listing_id == ""
    assert encode_semantic_action(SemanticAction("✏️ 换个条件", "change_search")) == raw


def test_stale_or_tampered_card_callback_is_rejected_by_session_position():
    ids = ("QL-BK-A2B3", "QL-BK-C4D5", "QL-B2-E6F7")
    good = parse_callback("v3u:card:1:QL-BK-C4D5")
    wrong_id = parse_callback("v3u:card:1:QL-B2-E6F7")
    wrong_index = parse_callback("v3u:card:8:QL-BK-C4D5")

    assert good is not None and validate_card_navigation(good, ids)
    assert wrong_id is not None and not validate_card_navigation(wrong_id, ids)
    assert wrong_index is not None and not validate_card_navigation(wrong_index, ids)


def test_legacy_internal_callbacks_do_not_enter_v3_contract():
    for raw in (
        "findcard:1:LST_2",
        "listing:detail:LST_2",
        "listing:photos:QC0350",
        "listing:appoint:l_35",
        "v3u:listing:details:QC0350",
        "v3u:listing:book:LST_2",
    ):
        assert parse_callback(raw) is None


def test_unknown_or_malformed_callbacks_are_rejected():
    for raw in (
        "",
        "v3u:anything",
        "v3u:listing:delete:QL-BK-A2B3",
        "v3u:card:-1:QL-BK-A2B3",
        "v3u:card:x:QL-BK-A2B3",
        "v3u:listing:details:not-an-id",
    ):
        assert parse_callback(raw) is None


def test_encoder_rejects_invalid_actions_targets_and_indices():
    with pytest.raises(ValueError, match="unsupported_listing_callback_action"):
        encode_listing_callback("delete", "QL-BK-A2B3")
    with pytest.raises(ValueError, match="invalid_public_listing_id"):
        encode_listing_callback("details", "LST_1")
    with pytest.raises(ValueError, match="invalid_search_card_index"):
        encode_card_callback(-1, "QL-BK-A2B3")
    with pytest.raises(ValueError, match="search_card_action_missing_index"):
        encode_semantic_action(
            SemanticAction(
                "下一套",
                "next",
                target_public_listing_id="QL-BK-C4D5",
            )
        )
