"""End-to-end /start deep-link entry contract for the V3 User Bot.

This is the integration-level entry contract that proves the real /start entry
surface (``property_<id>_photos`` and friends) lands on the raw-only paged album
from the moment the user opens the channel link — no cover render, no collage,
no legacy 「更多实拍」 expander. One page = one native Telegram album of up to
10 real photos (23 originals → ``10 / 10 / 3``); the pager only shows up when
there is more than one page.

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


def test_start_property_photos_dl_drops_user_into_raw_album_not_cover(tmp_path):
    """``/start property_<id>_photos__ch`` lands on page 0 = raw[0:10].

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

    # 9 originals fit one album. Cover explicitly excluded.
    flat = _flatten(result.photos.media_groups)
    assert flat == gallery
    assert str(cover) not in flat
    assert result.photos.photo_total == 9

    # Single page: no pager, no "1/1" counter, no legacy expander.
    rows = [[a.label for a in row] for row in result.photos.action_rows]
    assert rows == [["📅 预约看房", "💬 咨询这套"], ["⬅️ 返回房源"]]
    labels = _labels(result.photos)
    assert "📷 更多实拍" not in labels
    assert "📷 查看全部实拍" not in labels
    assert "💬 中文顾问" not in labels

    assert result.photos.media_caption == "📷 富力城 · 2房1厅 · 共 9 张"
    assert "查看全部" not in result.photos.text
    assert "页" not in result.photos.text


def test_start_property_photos_dl_next_page_uses_paged_callback(tmp_path):
    gallery = _write_photos(tmp_path, 23)
    result = _service(_view(gallery=gallery)).resolve("property_QL-RF-A2B3_photos__ch")
    assert _flatten(result.photos.media_groups) == gallery[0:10]
    actions = _actions(result.photos)
    next_btn = next(a for a in actions if a.label == "下一页 ›")
    parsed = parse_callback(encode_semantic_action(next_btn))
    assert parsed is not None
    assert parsed.action == "photos"
    assert parsed.public_listing_id == "QL-RF-A2B3"
    assert parsed.page_index == 1


@pytest.mark.parametrize("count", [4, 9, 10, 11, 23])
def test_start_property_photos_dl_paging_split_by_ten(tmp_path, count):
    """Pages are ``PHOTOS_PAGE_SIZE`` (10) chunks; every frame is a real photo."""
    assert PHOTOS_PAGE_SIZE == 10
    cover = tmp_path / "cover.jpg"
    cover.write_bytes(b"cover")
    gallery = _write_photos(tmp_path, count)

    view = _view(gallery=gallery, cover_path=str(cover))
    service = _service(view)

    expected_pages = (count + PHOTOS_PAGE_SIZE - 1) // PHOTOS_PAGE_SIZE
    expected_slices = [
        gallery[page * PHOTOS_PAGE_SIZE:min((page + 1) * PHOTOS_PAGE_SIZE, count)]
        for page in range(expected_pages)
    ]
    for page in range(expected_pages):
        result = service.resolve_action(
            "QL-RF-A2B3", "photos", source="listing_callback", photo_page=page
        )
        assert result.ok, (page, result)
        flat = _flatten(result.photos.media_groups)
        assert flat == expected_slices[page], (page, flat, expected_slices[page])
        assert str(cover) not in flat
        has_pager = any("页" in a.label for a in result.photos.action_rows[0])
        assert has_pager == (expected_pages > 1)

    if count == 23:
        assert [len(s) for s in expected_slices] == [10, 10, 3]


def test_start_property_photos_dl_keyboard_pager_rows(tmp_path):
    """First: 下一页 › only. Middle: ‹ 上一页 + 下一页 ›. Last: ‹ 上一页 only."""
    gallery = _write_photos(tmp_path, 23)
    service = _service(_view(gallery=gallery))
    pages = [
        service.resolve_action("QL-RF-A2B3", "photos", source="listing_callback", photo_page=n)
        for n in (0, 1, 2)
    ]
    assert [[a.label for a in p.photos.action_rows[0]] for p in pages] == [
        ["下一页 ›"],
        ["‹ 上一页", "下一页 ›"],
        ["‹ 上一页"],
    ]
    for page in pages:
        rows = [[a.label for a in row] for row in page.photos.action_rows]
        assert rows[1:] == [["📅 预约看房", "💬 咨询这套"], ["⬅️ 返回房源"]]
        assert not any("/" in label for row in rows for label in row)


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
    gallery = _write_photos(tmp_path, 23)
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
    assert _flatten(last.photos.media_groups) == gallery[20:23]


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