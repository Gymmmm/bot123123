from __future__ import annotations

import json

import pytest

from v3_core.user_bot.adviser_notes import adviser_notes_for_view
from v3_core.user_bot.listing_responses import build_details_response
from v3_core.user_bot.public_inventory import PublishedListingView


def _view(
    *,
    adviser_copy: object = None,
    adviser_copy_source: object = None,
    include_copy: bool = True,
    canonical_facts: dict | None = None,
    listing: dict | None = None,
) -> PublishedListingView:
    snapshot = {
        "schema": "v3_publication_snapshot.v1",
        "listing_id": "LST_1",
        "public_listing_id": "QL-RF-A2B3",
        "offer_id": "OFF_1",
        "canonical_facts": dict(canonical_facts or {}),
        "listing": {
            "project_name": "富力城",
            "public_location_display": "富力城",
            "property_type": "公寓",
            **dict(listing or {}),
        },
        "offer": {
            "offer_type": "rent",
            "monthly_rent_usd": 800,
            "publication_policy": "telegram_rent",
        },
    }
    if include_copy:
        snapshot["adviser_copy"] = adviser_copy
    if adviser_copy_source is not None:
        snapshot["adviser_copy_source"] = adviser_copy_source
    return PublishedListingView(
        listing={
            "listing_id": "LST_1",
            "public_listing_id": "QL-RF-A2B3",
            "inventory_status": "active",
        },
        offer={
            "offer_id": "OFF_1",
            "offer_type": "rent",
            "offer_status": "active",
            "publication_policy": "telegram_rent",
        },
        publication={"instance_id": "PUB_1"},
        package={"snapshot_json": json.dumps(snapshot, ensure_ascii=False)},
    )


@pytest.mark.parametrize("source", ["auto", "manual"])
def test_publisher_copy_is_returned_and_rendered_verbatim(source):
    frozen = "第一句由 Publisher 冻结。\n第二句顺序保持不变。"
    view = _view(adviser_copy=frozen, adviser_copy_source=source)
    assert adviser_notes_for_view(view) == frozen
    text = build_details_response(view).text
    assert "💬 侨联说" in text
    assert "<b>侨联说</b>" not in text
    assert frozen.splitlines()[0] in text
    assert frozen.splitlines()[1] in text
    assert "<blockquote>" not in text
    assert "• 第一句" not in text


def test_hidden_source_never_displays_even_when_copy_exists():
    view = _view(adviser_copy="这段不能公开", adviser_copy_source="hidden")
    assert adviser_notes_for_view(view) == ""
    assert "侨联说" not in build_details_response(view).text
    assert "这段不能公开" not in build_details_response(view).text


@pytest.mark.parametrize("copy", [None, "", "   "])
def test_empty_or_missing_copy_hides_entire_section(copy):
    view = _view(adviser_copy=copy, include_copy=copy is not None)
    assert adviser_notes_for_view(view) == ("" if copy is None else str(copy))
    assert "侨联说" not in build_details_response(view).text


@pytest.mark.parametrize(
    "facts,listing",
    [
        ({"included": ["物业费", "Wi-Fi"]}, {}),
        ({"amenities": ["泳池", "健身房"]}, {}),
        ({"adviser_signals": ["management_wifi", "high_floor"]}, {"floor": "39"}),
        ({}, {"property_type": "别墅", "floor": "28"}),
    ],
)
def test_listing_facts_never_generate_user_bot_adviser_copy(facts, listing):
    view = _view(include_copy=False, canonical_facts=facts, listing=listing)
    assert adviser_notes_for_view(view) == ""
    assert "侨联说" not in build_details_response(view).text


def test_other_details_and_actions_remain_present_with_authoritative_copy():
    view = _view(adviser_copy="已冻结建议。", adviser_copy_source="auto")
    response = build_details_response(view)
    assert "💬 侨联说" in response.text
    assert "已冻结建议。" in response.text
    assert "富力城" in response.text
    assert "$800" in response.text
    assert "🟢 房源状态：当前可预约" in response.text
    labels = [action.label for row in response.action_rows for action in row]
    assert "📅 预约看房" in labels
    assert "💬 中文顾问" in labels
