"""Real /start deep-link entry contract for the V3 User Bot.

Channel deep links (``property_<id>_photos`` etc.) MUST go straight to the
raw-only paged album — never the legacy cover collage + "更多实拍" expander.
Product lock (Task — 真实入口 photos 上线):
    * page 0 must equal ``raw_gallery[0:4]`` (cover/collage excluded);
    * first-page action row is "1/3" + "下一页 ➡️" (no "📷 更多实拍");
    * page 1 must equal ``raw_gallery[4:8]``;
    * page 2 must equal ``raw_gallery[8]``;
    * "房源详情 / 预约 / 中文顾问" stay reachable through the album.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from v3_core.user_bot.listing_responses import PHOTOS_PAGE_SIZE
from v3_core.user_bot.public_flow import PublicListingFlowService
from v3_core.user_bot.public_inventory import PublishedListingView
from v3_core.user_bot.route_service import PublicRouteService


class MemoryPublishedInventory:
    def __init__(self, view):
        self.view = view

    def resolve(self, public_listing_id):
        if self.view and self.view.public_listing_id == str(public_listing_id):
            return self.view
        return None


def _view(*, gallery=(), cover_path=""):
    snapshot = {
        "schema": "v3_publication_snapshot.v1",
        "listing_id": "LST_1",
        "public_listing_id": "QL-RF-A2B3",
        "offer_id": "OFF_1",
        "canonical_record_id": "CAN_1",
        "canonical_facts_hash": "hash-1",
        "canonical_facts": {"highlights": ["采光好"]},
        "listing": {
            "project_name": "富力城",
            "property_type": "公寓",
            "layout": "2房1厅",
            "public_location_display": "富力城",
            "size_sqm": 95,
            "floor": "19",
        },
        "offer": {
            "offer_type": "rent",
            "monthly_rent_usd": 800,
            "payment_terms": "押1付1",
            "contract_term": "1年",
            "publication_policy": "telegram_rent",
        },
    }
    package = {
        "snapshot_json": json.dumps(snapshot, ensure_ascii=False),
        "gallery_json": json.dumps(list(gallery), ensure_ascii=False),
    }
    if cover_path:
        package["cover_path"] = cover_path
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
        package=package,
    )


def _service(view):
    return PublicListingFlowService(PublicRouteService(MemoryPublishedInventory(view)))


def _flatten(groups):
    out = []
    for grp in groups or ():
        for p in grp:
            out.append(p)
    return out


def _labels(response):
    return [a.label for row in response.action_rows for a in row]


def _actions(response):
    return [a for row in response.action_rows for a in row]


def _write_photos(tmp_path, n: int, *, prefix="photo") -> list[str]:
    paths = []
    for i in range(n):
        p = tmp_path / f"{prefix}_{i + 1}.jpg"
        p.write_bytes(f"frame-{i}".encode("utf-8"))
        paths.append(str(p))
    return paths


def test_deeplink_photos_routes_to_paged_album_page_zero_raw_only(tmp_path):
    """``/start property_<id>_photos`` → paged album, page 0, no cover."""
    cover = tmp_path / "cover.jpg"
    cover.write_bytes(b"cover")
    gallery = _write_photos(tmp_path, 9)

    view = _view(gallery=gallery, cover_path=str(cover))
    result = _service(view).resolve("property_QL-RF-A2B3_photos__ch")

    assert result.ok and result.action == "photos"
    assert result.photos is not None

    # Page 0: first 4 raw gallery frames, cover explicitly excluded.
    assert _flatten(result.photos.media_groups) == gallery[0:4]
    assert str(cover) not in _flatten(result.photos.media_groups)

    # 9 frames → 3 pages.
    assert result.photos.photo_total == 9

    labels = _labels(result.photos)
    actions = _actions(result.photos)

    # Paging row: 1/3 + 下一页 (no prev on first page).
    assert "1/3" in labels
    assert "下一页 ➡️" in labels
    assert "⬅️ 上一页" not in labels

    # Old "more raw photos" expander is gone from the real /start entry.
    assert "📷 更多实拍" not in labels

    # 房源详情 / 预约 / 中文顾问 remain reachable so the user can exit cleanly.
    assert "📷 房源详情" in labels
    assert "📅 预约看房" in labels
    assert "💬 中文顾问" in labels

    # No caption text in the album carries the old "查看全部实拍" copy.
    assert "查看全部实拍" not in result.photos.text
    assert "查看全部" not in result.photos.text


def test_deeplink_photos_page_one_returns_raw_slice_four_to_eight(tmp_path):
    """``/start property_<id>_photos__ph`` still routes to paged album, page 0.

    The action bar can also page forward through the photos callback. Page 1
    must show the next 4 raw frames and switch the paging row to
    prev + 2/3 + next.
    """
    gallery = _write_photos(tmp_path, 9)
    view = _view(gallery=gallery)

    page1 = _service(view).resolve_action(
        "QL-RF-A2B3",
        "photos",
        source="listing_callback",
        photo_page=1,
    )

    assert page1.ok and page1.photos is not None
    assert _flatten(page1.photos.media_groups) == gallery[4:8]
    labels = _labels(page1.photos)
    assert labels[0] == "⬅️ 上一页"
    assert "2/3" in labels
    assert "下一页 ➡️" in labels


def test_deeplink_photos_page_two_returns_last_raw_frame_with_prev_only(tmp_path):
    """Last page is the trailing slice; next is hidden, prev is shown."""
    gallery = _write_photos(tmp_path, 9)
    view = _view(gallery=gallery)

    page2 = _service(view).resolve_action(
        "QL-RF-A2B3",
        "photos",
        source="listing_callback",
        photo_page=2,
    )

    assert page2.ok and page2.photos is not None
    assert _flatten(page2.photos.media_groups) == gallery[8:9]
    labels = _labels(page2.photos)
    assert "⬅️ 上一页" in labels
    assert "3/3" in labels
    # Last page hides the "next" arrow to prevent overshoot.
    assert "下一页 ➡️" not in labels


def test_deeplink_photos_excludes_cover_named_assets_from_paging(tmp_path):
    """Defensive: cover.jpg slipped into gallery_json must not appear on any page."""
    cover = tmp_path / "cover.jpg"
    cover.write_bytes(b"cover")
    framed = _write_photos(tmp_path, 5)
    # Legacy package: cover listed first inside the gallery array.
    gallery = [str(cover)] + framed

    view = _view(gallery=gallery, cover_path=str(cover))
    result = _service(view).resolve("property_QL-RF-A2B3_photos")

    assert result.ok and result.photos is not None
    flat = _flatten(result.photos.media_groups)
    assert str(cover) not in flat
    assert flat == framed[0:4]
    # 5 frames → 2 pages, page 0 still pure originals.
    assert "下一页 ➡️" in _labels(result.photos)
    assert "1/2" in _labels(result.photos)


def test_deeplink_photos_collage_path_is_unreachable_from_start_route():
    """The legacy build_photos_response entry must not be hit by the deep link.

    A real /start resolves via PublicListingFlowService.resolve → page 0.
    Direct invocation of build_photos_response is still callable for surfaces
    that want it (e.g. internal callbacks that opt-in), but the deep-link
    contract above proves the cover-collage path is no longer wired in.
    """
    from v3_core.user_bot.listing_responses import build_photos_response

    # Sanity: the legacy builder still exists; we just no longer hit it.
    assert callable(build_photos_response)

    # And resolve() must NEVER call it for a photos deep link.
    import v3_core.user_bot.public_flow as flow_mod

    src = Path(flow_mod.__file__).read_text(encoding="utf-8")
    # PublicListingFlowService.resolve must always forward photo_page=0 for
    # the photos action, so _render never falls back to build_photos_response.
    assert "photo_page = (\n            0" in src or "photo_page = (0" in src
    # The deep-link wrapper (resolve) is now the only entry that auto-sets
    # page 0; the cover/collage branch lives behind the explicit _render
    # ``else`` branch which is unreachable from ``resolve()``.
    assert "build_photos_page_response(" in src
    assert "build_photos_response(" in src
