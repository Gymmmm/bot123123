from qiaolian_dual.attribution import (
    attach_to_lead_payload as legacy_attach_to_lead_payload,
    classify_bot_event as legacy_classify_bot_event,
    classify_start_arg as legacy_classify_start_arg,
    merge_touch as legacy_merge_touch,
)
from v3_core.attribution.classification import (
    attach_to_lead_payload,
    classify_bot_event,
    classify_public_start_arg,
    merge_touch,
)
from v3_core.publishing.channel_contract import channel_start_payload


PUBLIC_ID = "QL-RF-A2B3"


def test_v3_public_listing_attribution_matches_locked_production():
    for action in ("details", "photos", "book"):
        payload = channel_start_payload(PUBLIC_ID, action)
        assert classify_public_start_arg(payload) == legacy_classify_start_arg(payload)


def test_direct_start_attribution_matches_locked_production():
    assert classify_public_start_arg("") == legacy_classify_start_arg("")


def test_bot_event_attribution_matches_locked_production_for_current_user_flows():
    cases = (
        {"action": "appointment_click", "source": "listing_card", "payload": {"listing_id": "L1"}},
        {"action": "photos_click", "source": "channel_deeplink", "payload": {"listing_id": "L2"}},
        {"action": "smart_search", "source": "user_search", "payload": {}},
        {"action": "service_hub", "source": "home", "payload": {}},
        {"action": "repair_submit", "source": "service", "payload": {}},
    )
    for kwargs in cases:
        assert classify_bot_event(**kwargs) == legacy_classify_bot_event(**kwargs)


def test_bot_event_with_public_start_arg_matches_locked_production_latest_override():
    start_arg = channel_start_payload(PUBLIC_ID, "book")
    kwargs = {
        "action": "appointment_submit",
        "source": "channel_deeplink",
        "payload": {"start_arg": start_arg, "listing_id": PUBLIC_ID},
    }
    assert classify_bot_event(**kwargs) == legacy_classify_bot_event(**kwargs)


def test_first_touch_latest_touch_merge_matches_locked_production():
    first_incoming = classify_public_start_arg(channel_start_payload(PUBLIC_ID, "details"))
    kwargs = {
        "user_id": 123,
        "username": "alice",
        "display_name": "Alice",
        "listing_id": PUBLIC_ID,
        "now": "2026-09-08 20:00:00",
    }
    first_v3 = merge_touch(None, first_incoming, **kwargs)
    first_legacy = legacy_merge_touch(None, first_incoming, **kwargs)
    assert first_v3 == first_legacy

    next_incoming = classify_bot_event(
        action="appointment_submit",
        source="listing_card",
        payload={"listing_id": PUBLIC_ID},
    )
    next_kwargs = {
        **kwargs,
        "now": "2026-09-08 20:10:00",
    }
    assert merge_touch(first_v3, next_incoming, **next_kwargs) == legacy_merge_touch(
        first_legacy, next_incoming, **next_kwargs
    )


def test_attached_lead_payload_matches_locked_production():
    incoming = classify_public_start_arg(channel_start_payload(PUBLIC_ID, "photos"))
    touch = merge_touch(
        None,
        incoming,
        user_id=123,
        username="alice",
        display_name="Alice",
        listing_id=PUBLIC_ID,
        now="2026-09-08 20:00:00",
    )
    payload = {"listing_id": PUBLIC_ID, "custom": "keep"}

    assert attach_to_lead_payload(payload, touch) == legacy_attach_to_lead_payload(payload, touch)
