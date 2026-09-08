import re

import pytest

from v3_core.publishing.channel_contract import (
    channel_action_url,
    channel_actions,
    channel_start_payload,
)
from v3_core.publishing.formatting import display_floor, display_layout
from v3_core.publishing.public_ids import (
    generate_public_id,
    legacy_to_internal,
    normalize_public_id,
)


def test_layout_and_floor_match_current_public_display_contract():
    assert display_layout("2房1厅2卫") == "2房1厅｜2卫"
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
