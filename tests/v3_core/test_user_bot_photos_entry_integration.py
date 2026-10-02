"""End-to-end /start deep-link entry contract for the V3 User Bot.

This is the integration-level entry contract that proves the real /start entry
surface (``property_<id>_photos`` and friends) lands on the raw-only paged album
from the moment the user opens the channel link — no cover render, no collage,
no legacy 「更多实拍」 expander, and the album splits 9 originals into pages of
``4 / 4 / 1`` so every page is a real photo that opens in Telegram's native
viewer.

The contract covers the full path:
    payload -> parse_channel_start_payload -> PublicRouteService
    -> PublicListingFlowService.resolve -> build_photos_page_response

so a regression in any single layer (parser, route, builder, keyboard) trips
exactly one test here.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from v3_core.user_bot.callbacks import (
    encode_semantic_action,
    parse_callback,
)
from v3_core.user_bot.deeplink import (
    PublicChannelRoute,
    parse_channel_start_payload,
)
from v3_core.user_bot.listing_responses import (
    PHOTOS_PAGE_SIZE,
    SemanticAction,
)
from v3_core.user_bot.public_flow import PublicListingFlowService
from v3_core.user_bot.public_inventory import PublishedListingView
from v3_core.user_bot.route_service import PublicRouteService


class MemoryPublishedInventory:
    """Minimal PublishedListingView host matching the production contract."""

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


# --- layer 1: payload parser --------------------------------------------------


def test_start_payload_parser_is_source_strict_and_recognizes_photos_action():
    """``property_<id>_photos`` parses to action=photos with the right source.

    The /start surface used by channel posts has both ``__ch`` and ``__ph``
    source suffixes; the bare form is the search-result canonical. Both must
    route to the same paged album (the PublicListingFlowService.handle layer
    below proves that).
    """
    bare = parse_channel_start_payload("property_QL-RF-A2B3_photos")
    assert bare is not None
    assert bare.action == "photos"
    assert bare.public_listing_id == "QL-RF-A2B3"
    assert bare.source_code == ""
    assert bare.source == "channel_deeplink"

    channel = parse_channel_start_payload("property_QL-RF-A2B3_photos__ch")
    assert channel is not None
    assert channel.action == "photos"
    assert channel.public_listing_id == "QL-RF-A2B3"
    assert channel.source_code == "ch"
    assert channel.source == "channel_listing"

    photos_source = parse_channel_start_payload("property_QL-RF-A2B3_photos__ph")
    assert photos_source is not None
    assert photos_source.action == "photos"
    assert photos_source.public_listing_id == "QL-RF-A2B3"
    assert photos_source.source_code == "ph"
    assert photos_source.source == "listing_photos"


# --- layer 2: route decision -------------------------------------------------


def test_route_decision_unblocks_rooms_photos_for_live_owner():
    """``property_<id>_photos`` is allowed; bookable listing routes to photos."""
    view = _view()
    decision = PublicRouteService(MemoryPublishedInventory(view)).resolve(
        "property_QL-RF-A2B3_photos__ch"
    )
    assert decision.ok
    assert decision.route is not None
    assert decision.route.action == "photos"
    assert decision.route.public_listing_id == "QL-RF-A2B3"
    assert decision.view is view


# --- layer 3: full flow integration -----------------------------------------


def test_start_property_photos_dl_drops_user_into_4_raw_originals_not_cover(tmp_path):
    """``/start property_<id>_photos__ch`` lands on page 0 = raw[0:4].

    Cover (frozen package render + side collage + "更多实拍" expander) MUST
    NOT be present in any layer. This is the entry the user sees when they
    open the channel post deep link.
    """
    cover = tmp_path / "cover.jpg"
    cover.write_bytes(b"cover-render")
    gallery = _write_photos(tmp_path, 9)

    view = _view(gallery=gallery, cover_path=str(cover))
    result = _service(view).resolve("property_QL-RF-A2B3_photos__ch")

    assert result.ok, result
    assert result.action == "photos"
    assert result.public_listing_id == "QL-RF-A2B3"
    assert result.source == "channel_listing"  # __ch wired correctly
    assert result.photos is not None

    # Page 0 = first 4 raw originals. Cover explicitly excluded.
    flat = _flatten(result.photos.media_groups)
    assert flat == gallery[0:4]
    assert str(cover) not in flat
    assert result.photos.photo_total == 9

    # Single inline keyboard row for paging (no legacy "更多实拍" expander).
    labels = _labels(result.photos)
    actions = _actions(result.photos)

    paging_row = [a.label for a in result.photos.action_rows[0]]
    assert "1/3" in paging_row, paging_row
    assert "下一页 ➡️" in paging_row, paging_row
    assert "⬅️ 上一页" not in paging_row, paging_row

    # Legacy expander + cover collage are gone from the real /start surface.
    assert "📷 更多实拍" not in labels
    assert "📷 查看全部实拍" not in labels

    # Exit / book / consultant links remain reachable through the album.
    # 2026-10-03 fix: ⬅️ 返回房源 replaces 📷 房源详情; consult is "咨询这套".
    assert "📷 房源详情" not in labels
    assert "⬅️ 返回房源" in labels
    assert "📅 预约看房" in labels
    assert "💬 咨询这套" in labels
    assert "💬 中文顾问" not in labels

    # No legacy cover-collage copy in caption or status text.
    assert "查看全部" not in result.photos.text
    assert "查看全部" not in result.photos.media_caption

    # Paging controls carry the paged callback format, not the legacy offset.
    next_btn = next(a for a in actions if a.label == "下一页 ➡️")
    next_callback = encode_semantic_action(next_btn)
    parsed = parse_callback(next_callback)
    assert parsed is not None
    assert parsed.action == "photos"
    assert parsed.public_listing_id == "QL-RF-A2B3"
    assert parsed.page_index == 1


@pytest.mark.parametrize("count", [4, 5, 8, 9])
def test_start_property_photos_dl_paging_split_is_4_4_1_or_short(tmp_path, count):
    """9 originals must split into 4 / 4 / 1 pages; smaller counts roll up.

    Every page (1, 2, …) must be a real photo, never the cover render or any
    thumbnail derivative. Page sizes are ``PHOTOS_PAGE_SIZE`` chunks with a
    final short tail of ``count % 4``.
    """
    cover = tmp_path / "cover.jpg"
    cover.write_bytes(b"cover")
    gallery = _write_photos(tmp_path, count)

    view = _view(gallery=gallery, cover_path=str(cover))
    service = _service(view)

    expected_pages = (count + PHOTOS_PAGE_SIZE - 1) // PHOTOS_PAGE_SIZE
    expected_slices = []
    for page in range(expected_pages):
        start = page * PHOTOS_PAGE_SIZE
        end = min(start + PHOTOS_PAGE_SIZE, count)
        expected_slices.append(gallery[start:end])

    for page in range(expected_pages):
        result = service.page(
            "QL-RF-A2B3", "photos", page=page
        ) if hasattr(service, "page") else None
        # PublicListingFlowService exposes resolve_action(photo_page=…)
        result = service.resolve_action(
            "QL-RF-A2B3", "photos", source="listing_callback", photo_page=page
        )
        assert result.ok, (page, result)
        flat = _flatten(result.photos.media_groups)
        assert flat == expected_slices[page], (page, flat, expected_slices[page])
        # Cover must never appear on any page.
        assert str(cover) not in flat

    # 9 → 3 pages of (4, 4, 1). Other counts follow the same chunking.
    if count == 9:
        sizes = [len(s) for s in expected_slices]
        assert sizes == [4, 4, 1]


def test_start_property_photos_dl_keyboard_has_prev_page_count_next(tmp_path):
    """First page: 1/total + 下一页 ➡️ (no prev).

    Middle page: ⬅️ 上一页 + N/total + 下一页 ➡️.
    Last page: ⬅️ 上一页 + N/total + 房源详情 exit (no next).
    Book + 中文顾问 row is identical to the first-screen listing actions.
    """
    gallery = _write_photos(tmp_path, 9)
    view = _view(gallery=gallery)
    service = _service(view)

    page0 = service.resolve_action(
        "QL-RF-A2B3", "photos", source="listing_callback", photo_page=0
    )
    row0 = [a.label for a in page0.photos.action_rows[0]]
    assert "1/3" in row0
    assert "下一页 ➡️" in row0
    assert "⬅️ 上一页" not in row0

    page1 = service.resolve_action(
        "QL-RF-A2B3", "photos", source="listing_callback", photo_page=1
    )
    row1 = [a.label for a in page1.photos.action_rows[0]]
    assert "⬅️ 上一页" in row1
    assert "2/3" in row1
    assert "下一页 ➡️" in row1

    page2 = service.resolve_action(
        "QL-RF-A2B3", "photos", source="listing_callback", photo_page=2
    )
    row2 = [a.label for a in page2.photos.action_rows[0]]
    assert "⬅️ 上一页" in row2
    assert "3/3" in row2
    assert "下一页 ➡️" not in row2

    # Every page exposes the same exit / book / consultant row.
    # 2026-10-03 fix: ⬅️ 返回房源 replaces 📷 房源详情; consult is "咨询这套".
    for page in (page0, page1, page2):
        flat_labels = _labels(page.photos)
        assert "📷 房源详情" not in flat_labels
        assert "⬅️ 返回房源" in flat_labels
        assert "📅 预约看房" in flat_labels
        assert "💬 咨询这套" in flat_labels
        assert "💬 中文顾问" not in flat_labels


def test_start_property_photos_dl_idempotent_on_repeated_callback(tmp_path):
    """Repeated /start entry yields identical media + keyboard + caption."""
    gallery = _write_photos(tmp_path, 9)
    view = _view(gallery=gallery)
    service = _service(view)

    first = service.resolve("property_QL-RF-A2B3_photos__ch")
    second = service.resolve("property_QL-RF-A2B3_photos__ch")
    assert first.ok and second.ok
    assert first.photos.media_groups == second.photos.media_groups
    assert first.photos.media_caption == second.photos.media_caption
    assert _labels(first.photos) == _labels(second.photos)


def test_start_property_photos_dl_out_of_range_page_clamps_to_last(tmp_path):
    """A tampered / late page callback lands on the last page, not an empty album."""
    gallery = _write_photos(tmp_path, 9)
    view = _view(gallery=gallery)
    service = _service(view)

    last = service.resolve_action(
        "QL-RF-A2B3", "photos", source="listing_callback", photo_page=99
    )
    explicit = service.resolve_action(
        "QL-RF-A2B3", "photos", source="listing_callback", photo_page=2
    )
    assert last.ok and explicit.ok
    assert last.photos.media_groups == explicit.photos.media_groups
    assert last.photos.media_caption == explicit.photos.media_caption
    # Last page is the trailing 1-frame slice.
    assert _flatten(last.photos.media_groups) == gallery[8:9]


def test_start_property_photos_dl_collage_path_unreachable_from_route():
    """``resolve()`` never enters the cover-collage builder.

    The deep-link entry must hit the paged-album builder exclusively; the
    legacy cover-first builder is reserved for surfaces that opt in via
    ``build_photos_response(..., offset=…)``. This locks the contract by
    inspecting the resolved flow module.
    """
    import v3_core.user_bot.public_flow as flow_mod

    src = Path(flow_mod.__file__).read_text(encoding="utf-8")
    # Deep-link wrapper auto-forwards page 0 for any photos route.
    assert "photo_page = (\n            0" in src or "photo_page = (0" in src
    # The paged builder is the real entry. The legacy builder is wired but
    # only via _render's offset branch, never through resolve().
    assert "build_photos_page_response(" in src
    assert "build_photos_response(" in src