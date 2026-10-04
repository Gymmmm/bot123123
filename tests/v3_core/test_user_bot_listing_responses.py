from __future__ import annotations

import json
from pathlib import Path

from v3_core.user_bot.listing_responses import (
    build_detail_caption,
    build_detail_text,
    build_details_response,
    build_photo_caption,
    build_photos_page_response,
    build_photos_response,
    PHOTOS_PAGE_SIZE,
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

    assert text.startswith("🏠 <b>富力城｜2房1厅</b>\n💰 $800/月\n📍 BKK1\n🏢 公寓｜95㎡｜19楼")
    assert "🔑 押1付1｜1年" in text
    assert "物业费｜含物业费" in text
    assert "水电｜水 按表 / 电 0.25$/度" in text
    assert "配套｜泳池、健身房" in text
    assert "💬 <b>侨联说</b>" in text
    assert "采光面宽，适合长期住。" in text
    assert "楼下配套成熟。" in text
    assert "🟢 当前可预约" in text
    assert text.strip().endswith("楼下配套成熟。")
    assert "钥匙已备" not in text
    # NOTE: public_id is NOT exposed in user-visible details text (privacy)


def test_detail_text_floor_only_does_not_use_area_floor_label():
    text = build_detail_text(_view(size_sqm=None, floor="19"))
    assert "🏢 公寓｜19楼" in text
    assert "㎡" not in text


def test_detail_text_omits_missing_bullets_and_adviser_without_copy():
    view = _view(canonical_facts={"highlights": ["采光好", "钥匙已备"]})

    response = build_details_response(view)
    text = response.text
    
    # New UI uses compact header format
    assert "富力城" in text
    # NOTE: public_id is NOT exposed in user-visible details text (privacy)
    assert "🪧" not in text
    assert "$800" in text
    assert "物业管理" not in text
    assert "水电费用" not in text
    assert "大楼配套" not in text
    assert "侨联说" not in text
    assert "🟢 当前可预约" in text
    # NOTE: details keyboard has no photos button (photos is a separate deep link)
    assert _actions(response.action_rows) == [["book", "consult"], ["similar"]]
    assert _labels(response.action_rows) == [
        ["📅 预约看房", "💬 咨询这套"],
        ["🔍 找相似"],
    ]


def test_details_response_uses_live_rented_state_but_keeps_frozen_public_facts():
    view = _view(status="rented", offer_status="inactive")
    response = build_details_response(view)

    assert "💰 $800/月" in response.text
    assert "🔴 已租出" in response.text
    # NOTE: details keyboard has no photos button (photos is a separate deep link)
    assert _actions(response.action_rows) == [["consult"], ["similar"]]
    assert _labels(response.action_rows) == [
        ["💬 咨询这套"],
        ["🔍 找相似"],
    ]


def test_photo_caption_keeps_rental_essentials_with_photo():
    view = _view()
    caption = build_photo_caption(view, photo_index=0, photo_total=3)
    # NOTE: public_id is NOT exposed in user-visible text (privacy)
    # New UI uses compact header format
    assert "富力城" in caption
    assert "$800" in caption
    assert "BKK1" in caption
    assert "2房1厅" in caption
    assert "🪧" not in caption
    assert "📸 1/3" in caption
    assert "基本信息" not in caption
    assert "金边优质房源出租" not in caption
    assert "侨联说" not in caption


def test_photo_caption_uses_location_when_project_is_missing():
    caption = build_photo_caption(_view(project_name=""), photo_index=0, photo_total=10)
    assert caption.startswith("🏠 <b>BKK1｜2房1厅</b>\n💰 $800/月\n🏢 公寓｜95㎡｜19楼")
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

    # New UI uses compact header format
    assert "50米路附近" in caption
    assert "6+2房8卫" in caption
    assert "$5,000" in caption
    assert "押2付1" in caption
    # NOTE: public_id is NOT exposed in user-visible text (privacy)
    assert "🪧" not in caption
    assert "侨联说" in caption
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
    # First screen is a native Telegram album of four equal-canvas real frames.
    assert first.photo_index == 0
    assert first.photo_total == 10  # capped at PHOTOS_MAX_TOTAL
    assert len(first.media_groups) == 1
    assert len(first.media_groups[0]) == 4
    assert first.media_groups[0][0] == str(cover)
    assert first.text.startswith("🟢 当前可预约")
    assert "以上是这套房" not in first.text
    assert "⬅️ 上一张" not in str(_labels(first.action_rows))
    assert _actions(first.action_rows) == [["photos", "book"], ["consult"]]
    assert _labels(first.action_rows) == [
        ["📷 更多实拍", "📅 预约看房"],
        ["💬 咨询这套"],
    ]
    expand_btn = first.action_rows[0][0]
    assert expand_btn.target_index == 0
    assert expand_btn.label == "📷 更多实拍"

    expanded = build_photos_response(_view(gallery=gallery), offset=4)
    assert expanded.expand_only
    assert expanded.action_rows == ()
    # Expand appends only unseen frames; it does not resend the first four.
    assert len(expanded.media_groups[0]) == 6
    assert expanded.media_groups[0][0] == files[3]


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
        ["📷 更多实拍", "💬 咨询这套"],
        ["🔍 继续找房", "🔍 找相似"],
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
        ["📋 房源详情", "📅 预约看房"],
        ["💬 咨询这套"],
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
    # Native album keeps both real frames when available.
    assert len(response.media_groups[0]) == 2
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
    # First screen is a native album; recovered cover stays in the source set.
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
    # New UI uses compact header format
    assert "富力城" in text
    # NOTE: public_id is NOT exposed in user-visible details text (privacy)
    assert "🪧" not in text


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

def test_customer_photo_surface_never_exposes_public_listing_id():
    response = build_photos_response(_view())
    assert "🆔" not in response.text
    assert "QL-RF-A2B3" not in response.text
    assert "QL-RF-A2B3" not in response.media_caption


def _fake_jpegs(tmp_path, count):
    body = bytes.fromhex(
        "FFD8FFE000104A46494600010101006000600000"
        "FFC00011080001000103012200021101031101"
        "FFDA0008010100003F00D66B00314000"
        "FFD9"
    )
    paths = []
    for i in range(count):
        p = tmp_path / f"photo_{i}.jpg"
        p.write_bytes(body)
        paths.append(str(p))
    return paths


def _view_with_files(files, *, cover_path: str = ""):
    snapshot = {
        "schema": "v3_publication_snapshot.v1",
        "listing_id": "LST_X",
        "public_listing_id": "QL-RF-A2B3",
        "offer_id": "OFF_X",
        "canonical_record_id": "CAN_X",
        "canonical_facts_hash": "h",
        "canonical_facts": {},
        "adviser_copy": "",
        "listing": {
            "project_name": "富力城",
            "property_type": "公寓",
            "layout": "2房1厅",
            "public_location_display": "BKK1",
            "size_sqm": 80,
            "floor": "10",
        },
        "offer": {
            "offer_type": "rent",
            "monthly_rent_usd": 700,
            "deposit_terms": "押2付1",
            "payment_terms": "押1付1",
            "contract_term": "1年",
            "publication_policy": "telegram_rent",
        },
    }
    return PublishedListingView(
        listing={
            "listing_id": "LST_X",
            "public_listing_id": "QL-RF-A2B3",
            "inventory_status": "active",
        },
        offer={
            "offer_id": "OFF_X",
            "monthly_rent_usd": 700,
            "offer_type": "rent",
            "publication_policy": "telegram_rent",
            "offer_status": "active",
        },
        publication={"public_listing_id": "QL-RF-A2B3"},
        package={
            "cover_path": str(cover_path or ""),
            "cover_style": "premium_photo",
            "public_listing_id": "QL-RF-A2B3",
            "snapshot_json": json.dumps(snapshot, ensure_ascii=False),
            "gallery_json": json.dumps(list(files), ensure_ascii=False),
        },
    )


def test_paged_album_handles_zero_one_three_four_photos(tmp_path):
    body = _fake_jpegs(tmp_path, 9)
    for count in (0, 1, 3, 4, 5, 8, 9):
        view = _view_with_files(body[:count])
        # Iterate through every possible page to make sure out-of-range pages
        # clamp to a valid window and produce a stable slice.
        for page in (-1, 0, 1, 2, 99):
            response = build_photos_page_response(view, page=page)
            assert response.photo_total == count
            if count == 0:
                assert response.media_groups == ()
                assert response.media_caption == ""
                continue
            # Out-of-range pages must clamp to the last valid page; same total
            # photos but the slice is whatever the last page actually holds.
            pages = max(1, (count + PHOTOS_PAGE_SIZE - 1) // PHOTOS_PAGE_SIZE)
            clamped_page = max(0, min(page, pages - 1))
            expected_start = clamped_page * PHOTOS_PAGE_SIZE
            expected_end = min(expected_start + PHOTOS_PAGE_SIZE, count)
            assert len(response.media_groups[0]) == expected_end - expected_start


def test_paged_album_buttons_include_prev_page_next_only_when_multi_page(tmp_path):
    body = _fake_jpegs(tmp_path, 10)
    # 3-photo album fits on a single page (no paging controls).
    small = _view_with_files(body[:3])
    single = build_photos_page_response(small, page=0)
    flat = [a.label for row in single.action_rows for a in row]
    assert "下一页 ➡️" not in flat
    assert "⬅️ 上一页" not in flat
    # 2026-10-03 fix: ⬅️ 返回房源 replaces 📋 房源详情 in the paged album.
    assert "📋 房源详情" not in flat
    assert "⬅️ 返回房源" in flat

    multi_view = _view_with_files(body)
    first_page = build_photos_page_response(multi_view, page=0)
    flat0 = [a.label for row in first_page.action_rows for a in row]
    # Paging 2026-10-02 fix: ⬅️ 下一页 ➡️ on a single row + N/total counter.
    assert "⬅️ 上一页" not in flat0  # first page never shows ⬅️
    assert "下一页 ➡️" in flat0
    assert "1/3" in flat0
    # Make sure the three controls sit on a single inline-keyboard row.
    first_inline_row = single_inline_row(first_page)
    assert first_inline_row == ["1/3", "下一页 ➡️"]

    mid_page = build_photos_page_response(multi_view, page=1)
    flat1 = [a.label for row in mid_page.action_rows for a in row]
    assert "⬅️ 上一页" in flat1
    assert "下一页 ➡️" in flat1
    assert "2/3" in flat1
    mid_inline_row = single_inline_row(mid_page)
    assert mid_inline_row == ["⬅️ 上一页", "下一页 ➡️"]

    last_page = build_photos_page_response(multi_view, page=2)
    flat2 = [a.label for row in last_page.action_rows for a in row]
    assert "⬅️ 上一页" in flat2
    assert "下一页 ➡️" not in flat2
    assert "📋 房源详情" not in flat2
    assert "⬅️ 返回房源" in flat2
    assert "📋 房源详情" not in flat2
    assert "⬅️ 返回房源" in flat2


def test_paged_album_is_idempotent_on_same_page(tmp_path):
    body = _fake_jpegs(tmp_path, 8)
    view = _view_with_files(body)
    page0a = build_photos_page_response(view, page=0)
    page0b = build_photos_page_response(view, page=0)
    assert page0a.media_caption == page0b.media_caption
    assert [g for g in page0a.media_groups] == [g for g in page0b.media_groups]
    # Page index out of range clamps to the last page, not an empty album.
    last = build_photos_page_response(view, page=99)
    last_explicit = build_photos_page_response(view, page=1)
    assert last.media_caption == last_explicit.media_caption
    assert [g for g in last.media_groups] == [g for g in last_explicit.media_groups]


def _paging_labels(response) -> list[str]:
    """Return labels of the first action row (the paging row)."""
    if not response or not response.action_rows:
        return []
    return [a.label for a in response.action_rows[0]]


def single_inline_row(response):
    """Helper for paging-row assertions."""
    if response is None or not response.action_rows:
        return None
    return [a.label for a in response.action_rows[0]]


def test_paged_album_uses_only_raw_photos_excludes_cover(tmp_path):
    """Paging 2026-10-02 fix: page 1 must NOT include the cover render.

    ``_raw_photo_paths_for_pages`` skips the cover file by name and by
    package.cover_path. A 9-photo gallery should produce pages of 4/4/1.
    """
    body = _fake_jpegs(tmp_path, 9)
    cover_path = Path(body[0]).parent / "cover_photo.png"
    cover_path.write_bytes(b"\x89PNG\r\n\x1a\n")
    view = _view_with_files(
        body,
        # Override the default package.cover_path so the cover render lives at
        # a different file than the gallery originals.
        cover_path=str(cover_path),
    )

    page0 = build_photos_page_response(view, page=0)
    page1 = build_photos_page_response(view, page=1)
    page2 = build_photos_page_response(view, page=2)

    # First page: exactly the first 4 originals, NOT cover.
    assert page0.photo_total == 9
    assert [path for path in page0.media_groups[0]] == [str(p) for p in body[:4]]
    # Cover is never part of the paged album.
    for page in (page0, page1, page2):
        for group in page.media_groups:
            assert str(cover_path) not in group

    # Page sizes 4 / 4 / 1 for 9 originals.
    assert len(page1.media_groups[0]) == 4
    assert [path for path in page1.media_groups[0]] == [str(p) for p in body[4:8]]
    assert [path for path in page2.media_groups[0]] == [str(p) for p in body[8:9]]


def test_more_photos_button_wires_page0_not_legacy_offset():
    """First-screen album action bar must launch paged album page 0.

    The action's ``target_index`` should be ``0`` so the encoded callback is
    ``v3u:listing:photos:{id}:pg:0`` — that routes through
    ``build_photos_page_response`` (raw originals, no cover).
    """
    from v3_core.user_bot.listing_responses import _photo_actions

    rows = _photo_actions(
        bookable=True,
        inventory_status="active",
        public_listing_id="QL-RF-A2B3",
        has_more=True,
    )
    flat = [a for row in rows for a in row]
    more = next((a for a in flat if a.label == "📷 更多实拍"), None)
    assert more is not None, flat
    assert more.action == "photos"
    assert more.target_index == 0
    # No legacy "查看全部实拍" should remain anywhere.
    assert not any(a.label == "📷 查看全部实拍" for a in flat)
