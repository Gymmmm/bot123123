from __future__ import annotations

import json

from v3_core.user_bot.listing_responses import (
    build_details_response,
    build_photos_response,
)
from v3_core.user_bot.public_inventory import PublishedListingView


def _view(*, status="active", offer_status="active", gallery=(), canonical_facts=None):
    snapshot = {
        "schema": "v3_publication_snapshot.v1",
        "listing_id": "LST_1",
        "public_listing_id": "QL-RF-A2B3",
        "offer_id": "OFF_1",
        "canonical_record_id": "CAN_1",
        "canonical_facts_hash": "hash-1",
        "canonical_facts": dict(canonical_facts or {}),
        "listing": {
            "project_name": "富力城",
            "property_type": "公寓",
            "layout": "2房1厅",
            "public_location_display": "BKK1",
            "size_sqm": 95,
            "floor": "19",
        },
        "offer": {
            "offer_type": "rent",
            "monthly_rent_usd": 800,
            "deposit_terms": "押2付1",
            "payment_terms": "押1付1",
            "contract_term": "1年",
            "publication_policy": "telegram_rent",
        },
    }
    return PublishedListingView(
        listing={
            "listing_id": "LST_1",
            "public_listing_id": "QL-RF-A2B3",
            "inventory_status": status,
        },
        offer={
            "offer_id": "OFF_1",
            "offer_type": "rent",
            "offer_status": offer_status,
            "publication_policy": "telegram_rent",
        },
        publication={"instance_id": "PUB_1"},
        package={
            "snapshot_json": json.dumps(snapshot, ensure_ascii=False),
            "gallery_json": json.dumps(list(gallery), ensure_ascii=False),
        },
    )


def _actions(rows):
    return [[item.action for item in row] for row in rows]


def _labels(rows):
    return [[item.label for item in row] for row in rows]


def test_details_response_omits_adviser_section_without_supported_signal():
    view = _view(canonical_facts={"highlights": ["采光好", "钥匙已备"]})

    response = build_details_response(view)

    assert response.text == (
        "📋 <b>租赁详情</b>\n"
        "\n"
        "🏠 <b>富力城｜2房1厅</b>\n"
        "💵 <b>$800/月</b>\n"
        "\n"
        "📍 BKK1\n"
        "📐 95㎡｜19楼\n"
        "🔑 押1付1 · 1年\n"
        "🟢 房态：当前可预约\n"
        "🆔 QL-RF-A2B3"
    )
    assert _actions(response.action_rows) == [["book", "consult"], ["similar"]]
    assert _labels(response.action_rows) == [
        ["📅 预约看房", "💬 问这套房"],
        ["🔍 看相近房源"],
    ]


def test_details_response_uses_live_rented_state_but_keeps_frozen_public_facts():
    view = _view(status="rented", offer_status="inactive")
    response = build_details_response(view)

    assert "💵 <b>$800/月</b>" in response.text
    assert "🔴 房态：已租出" in response.text
    assert _actions(response.action_rows) == [["consult"], ["similar"]]
    assert _labels(response.action_rows) == [
        ["💬 问这套房"],
        ["🔍 看相近房源"],
    ]


def test_photos_response_single_flipper_with_detail_caption(tmp_path):
    files = []
    for index in range(12):
        path = tmp_path / f"room-{index}.jpg"
        path.write_bytes(str(index).encode())
        files.append(str(path))
    cover = tmp_path / "cover.jpg"
    cover.write_bytes(b"cover")
    gallery = [str(cover), *files, files[0]]

    first = build_photos_response(_view(gallery=gallery))

    assert first.has_media
    assert first.photo_path == files[0]
    assert first.photo_index == 0
    assert first.photo_total == 12
    assert first.media_groups == ((files[0],),)
    assert "📋 <b>租赁详情</b> · 📸 1/12" in first.text
    assert "富力城｜2房1厅" in first.text
    assert "$800" in first.text
    assert "QL-RF-A2B3" in first.text
    assert "再看更多" not in first.text
    assert _actions(first.action_rows) == [["photos", "photos"], ["book", "consult"], ["similar"]]
    assert _labels(first.action_rows) == [
        ["⬅️ 上一张", "下一张 ➡️"],
        ["📅 预约看房", "💬 问这套房"],
        ["🔍 看相近房源"],
    ]
    prev_btn, next_btn = first.action_rows[0]
    assert prev_btn.target_index == 11
    assert next_btn.target_index == 1

    second = build_photos_response(_view(gallery=gallery), offset=1)
    assert second.photo_path == files[1]
    assert "📸 2/12" in second.text
    assert second.action_rows[0][0].target_index == 0
    assert second.action_rows[0][1].target_index == 2

    last = build_photos_response(_view(gallery=gallery), offset=11)
    assert last.photo_path == files[11]
    assert "📸 12/12" in last.text
    assert last.action_rows[0][1].target_index == 0


def test_photos_response_drops_missing_files_and_keeps_detail_fallback(tmp_path):
    missing = tmp_path / "missing.jpg"
    response = build_photos_response(_view(gallery=[str(missing)]))

    assert not response.has_media
    assert response.media_groups == ()
    assert response.photo_path == ""
    assert "📋 <b>租赁详情</b>" in response.text
    assert "富力城｜2房1厅" in response.text
    assert _labels(response.action_rows) == [
        ["📅 预约看房", "💬 问这套房"],
        ["🔍 看相近房源"],
    ]


def test_rented_photos_response_keeps_contact_but_removes_book(tmp_path):
    photo = tmp_path / "room.jpg"
    photo.write_bytes(b"room")
    response = build_photos_response(
        _view(status="rented", offer_status="inactive", gallery=[str(photo)])
    )

    assert _actions(response.action_rows) == [["consult"], ["similar"]]
    assert _labels(response.action_rows) == [
        ["💬 问这套房"],
        ["🔍 看相近房源"],
    ]
