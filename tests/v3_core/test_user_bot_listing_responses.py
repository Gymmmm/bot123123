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


def test_details_response_preserves_layout_and_frozen_adviser_notes():
    view = _view(canonical_facts={"highlights": ["采光好", "钥匙已备"]})

    response = build_details_response(view)

    assert response.text == (
        "🏠 <b>租赁详情</b>\n"
        "\n"
        "🏠 <b>富力城｜2房1厅</b>\n"
        "<b>区域：</b> BKK1\n"
        "<b>租金：</b> <b>$800/月</b>\n"
        "📐 面积：95㎡\n"
        "🏢 楼层：19楼\n"
        "🔑 租约：押1付1 · 1年\n"
        "🟢 房态：当前可预约\n"
        "🆔 房源编号：QL-RF-A2B3\n"
        "\n"
        "💬 <b>侨联说</b>\n"
        "\n"
        "这套标注在BKK1，项目是富力城，可以按实际通勤路线再判断。\n"
        "资料明确标注：采光好、钥匙已备；具体状态可以结合实拍确认。"
    )
    assert _actions(response.action_rows) == [["book", "photos"], ["consult"]]
    assert _labels(response.action_rows) == [
        ["📅 预约看房", "📸 更多实拍"],
        ["💬 联系中文顾问"],
    ]


def test_details_response_uses_live_rented_state_but_keeps_frozen_public_facts():
    view = _view(status="rented", offer_status="inactive")
    response = build_details_response(view)

    assert "<b>租金：</b> <b>$800/月</b>" in response.text
    assert "🔴 房态：已租出" in response.text
    assert _actions(response.action_rows) == [["photos", "consult"], ["similar"]]
    assert _labels(response.action_rows) == [
        ["📸 更多实拍", "💬 联系中文顾问"],
        ["🏘 看相近房源"],
    ]


def test_photos_response_chunks_existing_frozen_gallery_by_ten(tmp_path):
    files = []
    for index in range(12):
        path = tmp_path / f"room-{index}.jpg"
        path.write_bytes(str(index).encode())
        files.append(str(path))
    cover = tmp_path / "cover.jpg"
    cover.write_bytes(b"cover")
    gallery = [str(cover), *files, files[0]]

    response = build_photos_response(_view(gallery=gallery))

    assert response.has_media
    assert tuple(len(group) for group in response.media_groups) == (10, 2)
    flattened = [item for group in response.media_groups for item in group]
    assert flattened == files
    assert response.text == "📸 <b>以上是这套房目前保存的现场实拍。</b>"
    assert "QL-RF-A2B3" not in response.text
    assert "富力城" not in response.text
    assert "$800" not in response.text
    assert "BKK1" not in response.text
    assert _actions(response.action_rows) == [["details", "book"], ["consult"]]
    assert _labels(response.action_rows) == [
        ["🏠 租赁详情", "📅 预约看房"],
        ["💬 联系中文顾问"],
    ]


def test_photos_response_drops_missing_files_and_uses_locked_fallback_text(tmp_path):
    missing = tmp_path / "missing.jpg"
    response = build_photos_response(_view(gallery=[str(missing)]))

    assert not response.has_media
    assert response.media_groups == ()
    assert response.text == "📸 <b>这套房源目前的实拍已经全部显示。</b>"
    assert _labels(response.action_rows) == [
        ["🏠 租赁详情", "📅 预约看房"],
        ["💬 联系中文顾问"],
    ]


def test_rented_photos_response_keeps_details_and_contact_but_removes_book(tmp_path):
    photo = tmp_path / "room.jpg"
    photo.write_bytes(b"room")
    response = build_photos_response(
        _view(status="rented", offer_status="inactive", gallery=[str(photo)])
    )

    assert _actions(response.action_rows) == [["details"], ["consult"]]
    assert _labels(response.action_rows) == [
        ["🏠 租赁详情"],
        ["💬 联系中文顾问"],
    ]
