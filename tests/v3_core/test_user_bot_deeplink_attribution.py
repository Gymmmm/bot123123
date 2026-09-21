from __future__ import annotations

import pytest

from v3_core.publishing.channel_contract import (
    channel_start_payload,
    official_channel_action_urls,
)
from v3_core.user_bot.deeplink import parse_channel_start_payload


def test_legacy_channel_payload_stays_compatible():
    route = parse_channel_start_payload("property_QL-RF-A2B3_details")

    assert route is not None
    assert route.action == "details"
    assert route.public_listing_id == "QL-RF-A2B3"
    assert route.source == "channel_deeplink"
    assert route.source_code == ""


def test_channel_source_code_resolves_to_internal_attribution():
    route = parse_channel_start_payload("property_QL-RF-A2B3_photos__ch")

    assert route is not None
    assert route.action == "photos"
    assert route.public_listing_id == "QL-RF-A2B3"
    assert route.source == "channel_listing"
    assert route.source_code == "ch"


def test_known_source_codes_are_internal_only_and_all_parse():
    expected = {
        "sr": "search_result",
        "dt": "listing_details",
        "ph": "listing_photos",
        "ap": "appointment_success",
    }
    for code, source in expected.items():
        route = parse_channel_start_payload(
            f"property_QL-RF-A2B3_book__{code}"
        )
        assert route is not None
        assert route.source == source
        assert route.source_code == code


def test_unknown_valid_source_code_falls_back_without_breaking_link():
    route = parse_channel_start_payload("property_QL-RF-A2B3_details__x1")

    assert route is not None
    assert route.source == "channel_deeplink"
    assert route.source_code == "x1"


def test_official_channel_urls_emit_exact_v1_payloads():
    urls = official_channel_action_urls(
        "qiaolian_rent_bot", "QL-RF-A2B3", advisor_url="https://t.me/qiaolian_advisor"
    )

    assert urls["details"].endswith("property_QL-RF-A2B3_details")
    assert urls["photos"].endswith("property_QL-RF-A2B3_photos")
    assert urls["book"].endswith("property_QL-RF-A2B3_book")
    assert urls["consult"].endswith("property_QL-RF-A2B3_contact")


def test_explicit_payload_builder_keeps_legacy_default_and_validates_source_code():
    assert channel_start_payload("QL-RF-A2B3", "details") == (
        "property_QL-RF-A2B3_details"
    )
    assert channel_start_payload(
        "QL-RF-A2B3",
        "details",
        source_code="ch",
    ) == "property_QL-RF-A2B3_details__ch"

    with pytest.raises(ValueError, match="invalid_channel_source_code"):
        channel_start_payload(
            "QL-RF-A2B3",
            "details",
            source_code="bad-code",
        )
