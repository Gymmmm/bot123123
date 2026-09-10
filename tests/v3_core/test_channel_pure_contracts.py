import re

import pytest

from v3_core.publishing.channel_contract import (
    CHANNEL_CTA_LABELS,
    channel_action_url,
    channel_actions,
    channel_start_payload,
    official_channel_action_urls,
    official_channel_button_spec,
    official_channel_cta_keys,
)
from v3_core.publishing.formatting import display_floor, display_layout
from v3_core.publishing.public_ids import (
    generate_public_id,
    legacy_to_internal,
    normalize_public_id,
)


def test_layout_and_floor_match_current_public_display_contract():
    assert display_layout("2房1厅1卫") == "2房1厅｜2卫"
    assert display_layout("2房1办公2卫") == "2房＋书房｜2卫"
    assert display_layout("2房1办公2卫", "办公室") == "2房1办公2卫"
    assert display_floor("19") == "19楼"
    assert display_floor("19楼") == "19楼"
    assert display_floor("未知") == ""


def test_public_id_normalization_keeps_current_ql_shape():
    assert normalize_public_id("ql-rf-a2b3") == "QL-RF-A2B3"
    assert normalize_public_id("QLRFA2B3") == "QL-RF-A2B3"
    assert legacy_to_internal("QC0350") == "l_350"
    assert legacy_to_internal("l_350") == "l_350"
    generated = generate_public_id("RF")
    assert re.fullmatch(r"QL-RF-[A-HJ-NP-Z][2-9][A-HJ-NP-Z][2-9]", generated)


def test_channel_contract_is_only_details_photos_book():
    public_id = "QL-RF-A2B3"
    assert channel_actions(public_id) == (
        "property_QL-RF-A2B3_details",
        "property_QL-RF-A2B3_photos",
        "property_QL-RF-A2B3_book",
    )
    assert channel_action_url("@QiaolianBot", public_id, "details") == (
        "https://t.me/QiaolianBot?start=property_QL-RF-A2B3_details"
    )
    with pytest.raises(ValueError, match="unsupported_channel_action"):
        channel_start_payload(public_id, "advisor")
    with pytest.raises(ValueError, match="invalid_public_listing_id"):
        channel_start_payload("l_350", "details")
    with pytest.raises(ValueError, match="invalid_public_listing_id"):
        official_channel_action_urls("QiaolianBot", "l_350")
    with pytest.raises(ValueError, match="invalid_public_listing_id"):
        official_channel_action_urls("QiaolianBot", "")


def test_official_button_spec_shares_public_id_and_gates_book():
    actions = official_channel_action_urls("QiaolianBot", "QL-RF-A2B3")
    bookable = official_channel_button_spec(actions, inventory_status="active")
    rented = official_channel_button_spec(actions, inventory_status="rented")
    assert CHANNEL_CTA_LABELS == {
        "details": "🏠 房源详情",
        "photos": "📸 更多实拍",
        "book": "📅 预约看房",
    }
    assert official_channel_cta_keys("active") == ("details", "photos", "book")
    assert official_channel_cta_keys("rented") == ("details", "photos")
    assert [label for row in bookable for label, _url in row] == [
        "🏠 房源详情",
        "📸 更多实拍",
        "📅 预约看房",
    ]
    assert [label for row in rented for label, _url in row] == [
        "🏠 房源详情",
        "📸 更多实拍",
    ]
    urls = [url for row in bookable for _label, url in row]
    assert urls == [
        "https://t.me/QiaolianBot?start=property_QL-RF-A2B3_details",
        "https://t.me/QiaolianBot?start=property_QL-RF-A2B3_photos",
        "https://t.me/QiaolianBot?start=property_QL-RF-A2B3_book",
    ]
    assert all("l_350" not in url and "l_1" not in url for url in urls)
