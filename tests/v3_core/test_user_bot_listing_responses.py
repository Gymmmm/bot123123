from __future__ import annotations

import json

from v3_core.user_bot.listing_responses import (
    build_detail_caption,
    build_details_response,
    build_photos_response,
)
from v3_core.user_bot.public_inventory import PublishedListingView


def _view(
    *,
    status="active",
    offer_status="active",
    gallery=(),
    canonical_facts=None,
    adviser_copy="",
):
    snapshot = {
        "schema": "v3_publication_snapshot.v1",
        "listing_id": "LST_1",
        "public_listing_id": "QL-RF-A2B3",
        "offer_id": "OFF_1",
        "canonical_record_id": "CAN_1",
        "canonical_facts_hash": "hash-1",
        "canonical_facts": dict(canonical_facts or {}),
        "adviser_copy": adviser_copy,
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


def test_detail_caption_matches_locked_listing_copy_shape():
    view = _view(
        canonical_facts={
            "management_fee": "含物业费",
            "water_rate": "按表",
            "electric_rate": "0.25$/度",
            "amenities": ["泳池", "健身房"],
            "highlights": ["采光好", "钥匙已备"],
        },
        adviser_copy="采光面宽，适合长期住。\n楼下配套成熟。",
    )

    text = build_detail_caption(view)

    assert text.startswith("━━━━━━━━━━━━━━━\n🏢 金边优质房源出租\n━━━━━━━━━━━━━━━")
    assert "📌基本信息  房源编号：QL-RF-A2B3" in text
    assert "・项目区域：富力城 · BKK1" in text
    assert "・户型格局：2房1厅" in text
    assert "・楼层类型：19楼" in text
    assert "・房屋面积：95㎡" in text
    assert "💰 租金与付款" in text
    assert "・月租金额：$800 / 月" in text
    assert "・押付方式：押1付1" in text
    assert "・起租租期：1年" in text
    assert "🧾 费用与配套" in text
    assert "・物业管理：含物业费" in text
    assert "・水电费用：水 按表 / 电 0.25$/度" in text
    assert "・大楼配套：泳池、健身房" in text
    assert "💬 侨联说" in text
    assert "采光面宽，适合长期住。" in text
    assert "楼下配套成熟。" in text
    assert text.strip().endswith("🟢 房源状态：随时可预约看房")
    assert "钥匙已备" not in text


def test_detail_caption_omits_missing_bullets_and_adviser_without_copy():
    view = _view(canonical_facts={"highlights": ["采光好", "钥匙已备"]})

    response = build_details_response(view)
    text = response.text

    assert "🏢 金边优质房源出租" in text
    assert "📌基本信息  房源编号：QL-RF-A2B3" in text
    assert "・月租金额：$800 / 月" in text
    assert "🧾 费用与配套" not in text
    assert "物业管理" not in text
    assert "水电费用" not in text
    assert "大楼配套" not in text
    assert "💬 侨联说" not in text
    assert "🟢 房源状态：随时可预约看房" in text
    assert _actions(response.action_rows) == [["book", "consult"], ["similar"]]
    assert _labels(response.action_rows) == [
        ["📅 预约看房", "💬 问这套房"],
        ["🔍 看相近房源"],
    ]


def test_details_response_uses_live_rented_state_but_keeps_frozen_public_facts():
    view = _view(status="rented", offer_status="inactive")
    response = build_details_response(view)

    assert "・月租金额：$800 / 月" in response.text
    assert "🔴 房源状态：已租出" in response.text
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
    assert "🏢 金边优质房源出租" in first.text
    assert "📸 1/12" in first.text
    assert "📋" not in first.text
    assert "租赁详情" not in first.text
    assert "富力城 · BKK1" in first.text
    assert "$800 / 月" in first.text
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
    assert "🏢 金边优质房源出租" in response.text
    assert "📸 " not in response.text
    assert "富力城 · BKK1" in response.text
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
    assert "🔴 房源状态：已租出" in response.text
