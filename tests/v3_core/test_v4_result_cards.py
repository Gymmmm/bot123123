"""V4.0 result card: header i/N, 📸 看实拍 straight to album, verified differences."""
from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from v3_core.user_bot.callbacks import encode_semantic_action, parse_callback
from v3_core.user_bot.public_inventory import PublishedListingView
from v3_core.user_bot.search_cards import build_search_card, build_search_cards
from v3_core.user_bot.search_query import SearchCriteria
from v3_core.user_bot.search_session import search_context_payload, search_context_values
from v3_core.user_bot.similar_differences import area_matches, listing_differences, room_matches


def _view(public_id="QL-RF-A2B3", *, rent=800, gallery=(), layout="2房1厅", bedrooms=None,
          location_key="BKK1", location="BKK1", project="富力城"):
    snapshot = {
        "schema": "v3_publication_snapshot.v1",
        "listing_id": f"L_{public_id}",
        "public_listing_id": public_id,
        "offer_id": "O",
        "canonical_record_id": "C",
        "canonical_facts_hash": "h",
        "canonical_facts": {},
        "listing": {"project_name": project, "property_type": "公寓", "layout": layout,
                    "public_location_display": location},
        "offer": {"offer_type": "rent", "monthly_rent_usd": rent, "publication_policy": "telegram_rent"},
    }
    listing = {"listing_id": f"L_{public_id}", "public_listing_id": public_id, "inventory_status": "active",
               "layout": layout, "public_location_key": location_key}
    if bedrooms is not None:
        listing["bedrooms"] = bedrooms
    return PublishedListingView(
        listing=listing,
        offer={"offer_id": "O", "offer_type": "rent", "offer_status": "active",
               "publication_policy": "telegram_rent", "monthly_rent_usd": rent},
        publication={"instance_id": "P"},
        package={"snapshot_json": json.dumps(snapshot, ensure_ascii=False),
                 "gallery_json": json.dumps(list(gallery)), "cover_path": ""},
    )


def _labels(card):
    return [[a.label for a in row] for row in card.action_rows]


def test_card_header_and_photos_button_go_straight_to_album():
    items = (_view(gallery=("/x/1.jpg",)), _view("QL-BK-C4D5"))
    card = build_search_card(items, 0)
    assert card.text.startswith("🏠 <b>找到这些房源</b> · 1/2")
    assert _labels(card) == [
        ["📸 看实拍", "🏠 房源详情"],
        ["📅 预约看房", "💬 咨询这套"],
        ["⬅️ 上一套", "下一套 ➡️"],
        ["🔄 调整条件"],
    ]
    photos = card.action_rows[0][0]
    raw = encode_semantic_action(photos)
    assert raw == "v3u:listing:photos:QL-RF-A2B3"
    parsed = parse_callback(raw)
    assert parsed.kind == "listing" and parsed.action == "photos" and parsed.page_index is None


def test_single_result_has_no_counter_and_no_photo_button_without_gallery():
    card = build_search_card((_view(),), 0)
    assert card.text.startswith("🏠 <b>找到这些房源</b>\n")
    assert "看实拍" not in repr(_labels(card))


def test_strict_match_has_no_difference_labels():
    criteria = SearchCriteria(location_keys=("BKK1",), budget_max=900, room_type="2房")
    card = build_search_cards((_view(),), criteria=criteria)[0]
    assert "比预算高" not in card.text and "不在你选的区域" not in card.text and "户型不同" not in card.text


def test_similar_cards_label_only_verified_differences():
    criteria = SearchCriteria(location_keys=("BKK1",), budget_max=700, room_type="1房")
    view = _view(rent=800, location_key="TK", location="堆谷", bedrooms=2)
    cards = build_search_cards((view, _view("QL-BK-C4D5")), criteria=criteria, similar=True)
    text = cards[0].text
    assert text.startswith("🏘️ <b>这几套也值得看看</b> · 1/2")
    assert "💰 比预算高 $100" in text
    assert "📍 不在你选的区域（在 堆谷）" in text
    assert "🛏️ 户型不同：" in text
    assert "相邻" not in text


def test_unknown_data_never_produces_labels():
    listing = {"layout": "", "bedrooms": None}
    assert room_matches(listing, "2房") is None
    assert area_matches({}, ("BKK1",)) is None
    view = SimpleNamespace(listing={})
    assert listing_differences(view, SearchCriteria(location_keys=("BKK1",), room_type="2房"), rent=None) == ()


def test_room_matcher_mirrors_sql_rules():
    assert room_matches({"bedrooms": 5}, "4房") is True
    assert room_matches({"bedrooms": 2}, "2房") is True
    assert room_matches({"bedrooms": 0, "layout": "两房一厅"}, "2房") is True
    assert room_matches({"layout": "Studio"}, "studio") is True
    assert room_matches({"layout": "1房"}, "不限") is True


def test_search_context_round_trip_keeps_differences_on_navigation():
    criteria = SearchCriteria(location_keys=("BKK1",), budget_min=None, budget_max=700, room_type="1房")
    payload = search_context_payload(criteria, similar=True)
    json.dumps(payload)
    restored, similar = search_context_values(payload)
    assert similar is True
    assert restored.budget_max == 700 and restored.location_keys == ("BKK1",) and restored.room_type == "1房"
    assert search_context_values(None) == (None, False)


@pytest.mark.asyncio
async def test_interim_panel_deleted_when_photo_card_is_sent():
    from v3_core.user_bot.telegram_transition_action_handler import _drop_interim_panel

    deleted = []

    async def _delete():
        deleted.append(True)

    query = SimpleNamespace(message=SimpleNamespace(photo=None, delete=_delete))
    presentation = SimpleNamespace(response=SimpleNamespace(photo_path="/tmp/card.jpg"))
    await _drop_interim_panel(query, presentation)
    assert deleted == [True]

    deleted.clear()
    await _drop_interim_panel(query, SimpleNamespace(response=SimpleNamespace(photo_path="")))
    assert deleted == []
