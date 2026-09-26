from __future__ import annotations

import json
from pathlib import Path

from v3_core.user_bot.listing_responses import (
    build_detail_caption,
    build_detail_text,
    build_details_response,
    build_photo_caption,
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
    project_name="富力城",
    size_sqm=95,
    floor="19",
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
            "project_name": project_name,
            "property_type": "公寓",
            "layout": "2房1厅",
            "public_location_display": "BKK1",
            "size_sqm": size_sqm,
            "floor": floor,
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


def test_detail_text_shows_public_facts_and_full_publisher_copy():
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

    text = build_detail_text(view)

    assert text.startswith("🏡 项目：富力城\n📍 区域：BKK1\n🛏 户型：2房1厅")
    assert "📐 面积/楼层：95㎡ · 19楼" in text
    assert "💵 租金：$800/月" in text
    assert "🗝 租约：押1付1 · 1年" in text
    assert "🧾 物业费：含物业费" in text
    assert "🧾 水电：水 按表 / 电 0.25$/度" in text
    assert "🧾 配套：泳池、健身房" in text
    assert "💬 侨联说" in text
    assert "采光面宽，适合长期住。" in text
    assert "楼下配套成熟。" in text
    assert "🟢 当前可预约\n🪧 编号：QL-RF-A2B3" in text
    assert text.strip().endswith("楼下配套成熟。")
    assert "钥匙已备" not in text


def test_detail_text_floor_only_does_not_use_area_floor_label():
    text = build_detail_text(_view(size_sqm=None, floor="19"))
    assert "🏙 楼层：19楼" in text
    assert "面积/楼层" not in text
    assert "面积：" not in text


def test_detail_text_omits_missing_bullets_and_adviser_without_copy():
    view = _view(canonical_facts={"highlights": ["采光好", "钥匙已备"]})

    response = build_details_response(view)
    text = response.text

    assert "🏡 项目：富力城" in text
    assert "🪧 编号：QL-RF-A2B3" in text
    assert "💵 租金：$800/月" in text
    assert "物业管理" not in text
    assert "水电费用" not in text
    assert "大楼配套" not in text
    assert "💬 侨联说" not in text
    assert "🟢 当前可预约" in text
    assert _actions(response.action_rows) == [["book", "consult"], ["photos", "similar"]]
    assert _labels(response.action_rows) == [
        ["📅 预约看房", "💬 中文顾问"],
        ["📷 更多实拍", "🔍 继续找房"],
    ]


def test_details_response_uses_live_rented_state_but_keeps_frozen_public_facts():
    view = _view(status="rented", offer_status="inactive")
    response = build_details_response(view)

    assert "💵 租金：$800/月" in response.text
    assert "🔴 已租出" in response.text
    assert _actions(response.action_rows) == [["consult"], ["photos", "similar"]]
    assert _labels(response.action_rows) == [
        ["💬 中文顾问"],
        ["📷 更多实拍", "🔍 继续找房"],
    ]


def test_photo_caption_keeps_rental_essentials_with_photo():
    view = _view()
    caption = build_photo_caption(view, photo_index=0, photo_total=3)
    assert caption == (
        "🏡 项目：富力城\n"
        "📍 区域：BKK1\n"
        "🛏 户型：2房1厅\n"
        "💵 租金：$800/月\n"
        "📐 面积/楼层：95㎡ · 19楼\n"
        "🗝 租约：押1付1 · 1年\n"
        "🟢 当前可预约\n"
        "🪧 编号：QL-RF-A2B3\n\n"
        "📸 1/3"
    )
    assert "基本信息" not in caption
    assert "金边优质房源出租" not in caption
    assert "侨联说" not in caption


def test_photo_caption_uses_location_when_project_is_missing():
    caption = build_photo_caption(_view(project_name=""), photo_index=0, photo_total=10)
    assert caption.startswith("🏡 类型：公寓\n📍 区域：BKK1\n🛏 户型：2房1厅")
    assert caption.endswith("📸 1/10")


def test_villa_caption_keeps_full_frozen_adviser_copy_in_separate_section():
    view = _view(project_name="", adviser_copy="多一个灵活空间，可做书房。\n管理费和停车看房时确认。")
    snapshot = view.snapshot
    snapshot["listing"].update(
        property_type="别墅", layout="6+2房8卫", public_location_display="50米路附近"
    )
    snapshot["offer"].update(monthly_rent_usd=5000, payment_terms="押2付1")
    view.package["snapshot_json"] = json.dumps(snapshot, ensure_ascii=False)

    caption = build_photo_caption(view, photo_index=4, photo_total=10)

    assert "🏡 类型：别墅\n📍 区域：50米路附近\n🛏 户型：6+2房8卫" in caption
    assert "💵 租金：$5,000/月\n" in caption
    assert "🗝 租约：押2付1 · 1年" in caption
    assert "🪧 编号：QL-RF-A2B3\n\n💬 侨联说\n" in caption
    assert "多一个灵活空间，可做书房。\n管理费和停车看房时确认。" in caption
    assert caption.endswith("📸 5/10")
    assert len(caption) < 1024


def test_photos_response_first_batch_album_with_expand(tmp_path):
    from pathlib import Path
    from PIL import Image

    files = []
    for index in range(12):
        path = tmp_path / f"room-{index}.jpg"
        Image.new("RGB", (640, 480), (20 * index, 40, 80)).save(path, quality=85)
        files.append(str(path))
    cover = tmp_path / "cover.jpg"
    Image.new("RGB", (640, 480), (200, 160, 100)).save(cover, quality=85)
    gallery = [str(cover), *files, files[0]]

    first = build_photos_response(_view(gallery=gallery))

    assert first.has_media
    assert not first.expand_only
    # First screen is one side-stack collage JPG (not a 4-frame MediaGroup).
    assert first.photo_index == 0
    assert first.photo_total == 10  # capped at PHOTOS_MAX_TOTAL
    assert len(first.media_groups) == 1
    assert len(first.media_groups[0]) == 1
    assert Path(first.media_groups[0][0]).is_file()
    assert first.text.startswith("🟢 当前可预约")
    assert "以上是这套房" not in first.text
    assert "⬅️ 上一张" not in str(_labels(first.action_rows))
    assert _actions(first.action_rows) == [["photos", "book"], ["consult"]]
    assert _labels(first.action_rows) == [
        ["📷 查看全部实拍", "📅 预约看房"],
        ["💬 中文顾问"],
    ]
    expand_btn = first.action_rows[0][0]
    assert expand_btn.target_index == 4

    expanded = build_photos_response(_view(gallery=gallery), offset=4)
    assert expanded.expand_only
    assert expanded.action_rows == ()
    # Expand sends original frames (cap 10), not "remaining after collage".
    assert len(expanded.media_groups[0]) == 10
    assert expanded.media_groups[0][0] == str(cover)


def test_photos_response_pending_has_no_book_button(tmp_path):
    from PIL import Image

    files = []
    for index in range(8):
        path = tmp_path / f"room-{index}.jpg"
        Image.new("RGB", (640, 480), (10 * index, 50, 90)).save(path, quality=85)
        files.append(str(path))

    response = build_photos_response(_view(status="pending", gallery=files))

    assert response.text.startswith("🔵 房态待确认")
    assert _labels(response.action_rows) == [
        ["📷 查看全部实拍", "💬 中文顾问"],
        ["🏠 帮我找房", "🔎 看相近房源"],
    ]
    assert all(action.action != "book" for row in response.action_rows for action in row)


def test_photos_response_single_photo_uses_details_not_expand(tmp_path):
    from PIL import Image

    one = tmp_path / "only.jpg"
    Image.new("RGB", (640, 480), (90, 90, 90)).save(one, quality=85)
    response = build_photos_response(_view(gallery=[str(one)]))

    assert response.media_groups == ((str(one),),)
    assert response.photo_total == 1
    assert _labels(response.action_rows) == [
        ["📷 房源详情", "📅 预约看房"],
        ["💬 中文顾问"],
    ]


def test_photos_response_drops_missing_files_and_keeps_text_fallback(tmp_path):
    missing = tmp_path / "missing.jpg"
    response = build_photos_response(_view(gallery=[str(missing)]))

    assert not response.has_media
    assert response.media_groups == ()
    assert response.photo_path == ""
    assert response.text.startswith("🟢 当前可预约")
    assert "实拍暂时没有加载出来" in response.text
    assert "🏢 金边优质房源出租" not in response.text
    assert response.detail_text == ""


def test_album_starts_on_package_cover_path(tmp_path):
    cover = tmp_path / "frozen-cover.png"
    cover.write_bytes(b"COVER")
    room = tmp_path / "living.jpg"
    room.write_bytes(b"room")
    view = _view(gallery=[str(room)])
    # Inject package cover_path
    package = dict(view.package)
    package["cover_path"] = str(cover)
    view = PublishedListingView(
        listing=view.listing,
        offer=view.offer,
        publication=view.publication,
        package=package,
    )
    response = build_photos_response(view)
    assert response.photo_total == 2
    assert response.has_media
    # Prefer collage when images are readable; tiny non-image fixtures fall back.
    assert len(response.media_groups[0]) in {1, 2}
    expanded = build_photos_response(view, offset=4)
    assert expanded.expand_only
    assert expanded.has_media
    assert str(Path(cover).resolve()) in {
        str(Path(p).resolve()) for p in expanded.media_groups[0]
    }


def test_album_recovers_rendered_cover_when_package_path_stale(tmp_path):
    """Runtime often has gallery files but a stale absolute cover_path.

    Album frame 1 must still open on the rendered cover under covers_v3, not gallery[0].
    """
    public_id = "QL-RF-A2B3"
    style = "classic_blue"
    media_root = tmp_path / "media"
    covers = media_root / "covers_v3"
    gallery_dir = media_root / "prepared_v3" / "42" / "gallery"
    covers.mkdir(parents=True)
    gallery_dir.mkdir(parents=True)

    rendered = covers / f"{public_id}_{style}.png"
    rendered.write_bytes(b"RENDERED_COVER")
    room = gallery_dir / "classic_blue_abc_gallery.jpg"
    room.write_bytes(b"room")

    stale = tmp_path / "missing-elsewhere" / "old-cover.png"  # does not exist
    view = _view(gallery=[str(room)])
    package = dict(view.package)
    package["cover_path"] = str(stale)
    package["cover_style"] = style
    view = PublishedListingView(
        listing=view.listing,
        offer=view.offer,
        publication=view.publication,
        package=package,
    )

    response = build_photos_response(view)
    # First screen may be a collage; cover must still be in the source set.
    assert response.photo_total == 2
    assert response.has_media
    expanded = build_photos_response(view, offset=4)
    assert expanded.expand_only
    assert expanded.has_media
    assert str(rendered.resolve()) in {
        str(Path(p).resolve()) for p in expanded.media_groups[0]
    }


def test_build_detail_caption_alias_matches_public_fact_body():
    assert build_detail_caption is build_detail_text
    text = build_detail_caption(_view())
    assert "🏡 项目：富力城" in text
    assert "🪧 编号：QL-RF-A2B3" in text


def _with_package(view, **changes):
    package = dict(view.package)
    package.update(changes)
    return PublishedListingView(
        listing=view.listing,
        offer=view.offer,
        publication=view.publication,
        package=package,
    )


def test_cover_fallback_never_crosses_public_listing_ids(tmp_path):
    media = tmp_path / "media"
    covers = media / "covers_v3"
    gallery_dir = media / "prepared_v3" / "1" / "gallery"
    covers.mkdir(parents=True)
    gallery_dir.mkdir(parents=True)
    wrong = covers / "QL-RF-B9C8_classic_blue.png"
    wrong.write_bytes(b"WRONG")
    gallery = gallery_dir / "room.jpg"
    gallery.write_bytes(b"ROOM")
    view = _with_package(
        _view(gallery=[str(gallery)]),
        cover_path=str(tmp_path / "stale.png"),
        cover_style="classic_blue",
    )
    response = build_photos_response(view)
    assert response.photo_path == str(gallery.resolve())
    assert str(wrong.resolve()) not in response.media_groups[0]


def test_stale_cover_path_recovers_current_listing_exact_rendered_cover(tmp_path):
    public_id = "QL-RF-A2B3"
    media = tmp_path / "media"
    covers = media / "covers_v3"
    gallery_dir = media / "prepared_v3" / "1" / "gallery"
    covers.mkdir(parents=True)
    gallery_dir.mkdir(parents=True)
    rendered = covers / f"{public_id}_classic_blue.png"
    rendered.write_bytes(b"RIGHT")
    (covers / "QL-RF-B9C8_classic_blue.png").write_bytes(b"WRONG")
    room = gallery_dir / "room.jpg"
    room.write_bytes(b"ROOM")
    view = _with_package(
        _view(gallery=[str(room)]),
        cover_path=str(tmp_path / "stale.png"),
        cover_style="classic_blue",
    )
    response = build_photos_response(view)
    assert response.photo_path == str(rendered.resolve())


def test_no_rendered_cover_falls_back_only_to_current_listing_gallery(tmp_path):
    gallery = tmp_path / "current-room.jpg"
    gallery.write_bytes(b"ROOM")
    view = _with_package(
        _view(gallery=[str(gallery)]),
        cover_path=str(tmp_path / "missing.png"),
        cover_style="classic_blue",
    )
    response = build_photos_response(view)
    assert response.photo_path == str(gallery.resolve())
