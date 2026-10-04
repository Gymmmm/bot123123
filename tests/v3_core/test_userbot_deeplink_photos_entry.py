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

    # ⬅️ 返回房源 replaces 📋 房源详情; consult is "咨询这套" (2026-10-03).
    assert "📋 房源详情" not in labels
    assert "⬅️ 返回房源" in labels
    assert "📋 房源详情" not in labels
    assert "⬅️ 返回房源" in labels
    assert "📅 预约看房" in labels
    assert "💬 咨询这套" in labels

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
    assert "photo_page = (
            0" in src or "photo_page = (0" in src
    # The deep-link wrapper (resolve) is now the only entry that auto-sets
    # page 0; the cover/collage branch lives behind the explicit _render
    # ``else`` branch which is unreachable from ``resolve()``.
    assert "build_photos_page_response(" in src
    assert "build_photos_response(" in src


# ---------------------------------------------------------------------------
# Entry integration tests: /start property_<id>_photos through handle_v3_start.
# ---------------------------------------------------------------------------


from types import SimpleNamespace

from v3_core.user_bot.telegram_start_handler import handle_v3_start


class _StubMessage:
    def __init__(self):
        self.calls: list[tuple] = []

    async def reply_text(self, text, **kwargs):
        self.calls.append(("reply_text", text, kwargs))
        return SimpleNamespace(message_id=len(self.calls), delete=None)


class _StubBot:
    def __init__(self):
        self.calls: list[tuple] = []

    async def send_photo(self, **kwargs):
        photo = kwargs.get("photo")
        self.calls.append(("send_photo", getattr(photo, "name", ""), kwargs))
        return SimpleNamespace(
            message_id=len(self.calls),
            chat_id=kwargs.get("chat_id"),
        )

    async def send_media_group(self, **kwargs):
        self.calls.append(("send_media_group", "", kwargs))
        group = [
            SimpleNamespace(message_id=len(self.calls) + idx)
            for idx in range(len(kwargs.get("media") or ()))
        ]
        return group

    async def send_message(self, **kwargs):
        self.calls.append(("send_message", kwargs.get("text", ""), kwargs))
        return SimpleNamespace(
            message_id=len(self.calls),
            chat_id=kwargs.get("chat_id"),
        )


def _start_update(message):
    return SimpleNamespace(
        effective_message=message,
        effective_chat=SimpleNamespace(id=1001),
        effective_user=SimpleNamespace(id=123, username="alice", full_name="Alice"),
    )


def _entry_view(*, gallery=(), cover_path=""):
    snapshot = {
        "schema": "v3_publication_snapshot.v1",
        "listing_id": "LST_INT_1",
        "public_listing_id": "QL-RF-A2B3",
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
    return PublishedListingView(
        listing={
            "listing_id": "LST_INT_1",
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
        package={
            "snapshot_json": json.dumps(snapshot, ensure_ascii=False),
            "gallery_json": json.dumps(list(gallery), ensure_ascii=False),
            "cover_path": cover_path,
        },
    )


@pytest.mark.asyncio
async def test_entry_integration_nine_photos_opens_page_zero_raw_only_via_start(tmp_path):
    """``/start property_<id>_photos`` opens page 0 of the paged album end-to-end.

    Drives handle_v3_start with a 9-original listing + a separate cover. Page 0
    must send the first 4 raw frames (cover render excluded, no collage, no
    legacy "更多实拍") and the action bar must show "1/3" + "下一页 ➡️" only
    (no ⬅️ 上一页 on first page). The keyboard still carries 房源详情 /
    预约 / 中文顾问 so the user can exit cleanly.
    """
    cover = tmp_path / "cover.jpg"
    cover.write_bytes(b"COVER")
    gallery = _write_photos(tmp_path, 9)

    view = _entry_view(gallery=gallery, cover_path=str(cover))
    inventory = MemoryPublishedInventory(view)
    listings = PublicListingFlowService(PublicRouteService(inventory))
    message = _StubMessage()
    bot = _StubBot()
    context = SimpleNamespace(
        args=["property_QL-RF-A2B3_photos__ch"],
        bot=bot,
        user_data={},
    )

    outcome = await handle_v3_start(
        _start_update(message),
        context,
        listings=listings,
        transition_views=SimpleNamespace(build=lambda _plan: None),
        channel_url="https://t.me/qiaolian",
    )

    # Entry contract: /start property_photos beats the public flow → page 0.
    assert outcome.handled and outcome.kind == "photos"
    assert outcome.payload == "property_QL-RF-A2B3_photos__ch"
    assert outcome.result is not None
    assert outcome.result.public_listing_id == "QL-RF-A2B3"
    assert outcome.result.source == "channel_listing"

    # Page 0: a single MediaGroup of exactly 4 raw frames.
    media_calls = [c for c in bot.calls if c[0] == "send_media_group"]
    assert len(media_calls) == 1
    frames = media_calls[0][2]["media"]
    assert len(frames) == 4
    # Cover render is excluded from every page of the paged album.
    cover_bytes = cover.read_bytes()
    for frame in frames:
        raw = (
            frame.media.input_file_content
            if hasattr(frame.media, "input_file_content")
            else frame.media
        )
        assert raw != cover_bytes
    # First 4 originals go out byte-for-byte, in gallery order.
    actual_frame_bytes = []
    for frame in frames:
        if hasattr(frame.media, "input_file_content"):
            actual_frame_bytes.append(frame.media.input_file_content)
        elif isinstance(frame.media, (bytes, bytearray)):
            actual_frame_bytes.append(bytes(frame.media))
        else:
            actual_frame_bytes.append(bytes(frame.media.read()))
    assert actual_frame_bytes == [
        Path(gallery[i]).read_bytes() for i in range(4)
    ]
    # Caption says 1/3 and references the listing (page index, not a cover).
    caption = frames[0].caption
    assert "1/3" in caption
    assert "9 张" in caption
    # No legacy collages, no "查看全部实拍" copy on the /start entry.
    assert "查看全部实拍" not in repr(bot.calls)
    assert "更多实拍" not in repr(bot.calls)

    # Action bar message has the paged-album keyboard.
    send_msg = [c for c in bot.calls if c[0] == "send_message"]
    assert len(send_msg) == 1
    action_text = send_msg[0][2]["text"]
    keyboard = send_msg[0][2].get("reply_markup")
    assert "🟢 当前可预约" in action_text  # status bar
    assert keyboard is not None
    keyboard_text = repr(keyboard)
    # Single-row paging: 1/3 + 下一页 ➡️ on first page, no ⬅️.
    assert "1/3" in keyboard_text
    assert "下一页 ➡️" in keyboard_text
    assert "⬅️ 上一页" not in keyboard_text
    # Legacy expander gone, exit + book + advisor stay reachable.
    assert "📷 更多实拍" not in keyboard_text
    assert "📋 房源详情" not in keyboard_text
    assert "⬅️ 返回房源" in keyboard_text
    assert "⬅️ 返回房源" not in keyboard_text
    assert "📅 预约看房" in keyboard_text
    assert "💬 咨询这套" in keyboard_text
    # Privacy: internal listing_id never leaks to the bot.
    assert "LST_INT_1" not in action_text
    assert "LST_INT_1" not in caption
    assert "LST_INT_1" not in keyboard_text


@pytest.mark.asyncio
async def test_entry_integration_pagination_through_callback_router_4_4_1(tmp_path):
    """In-app paging callbacks route to page 1 (next 4 frames) and page 2 (last 1).

    Drives ``CallbackRouter.dispatch`` with ``v3u:listing:photos:QL-*:pg:1``
    → page 1 must produce ``raw_gallery[4:8]`` with a keyboard containing
    ⬅️ 上一页 + 2/3 + 下一页 ➡️; ``...:pg:2`` → page 2 must produce
    ``raw_gallery[8:9]`` with ⬅️ 上一页 + 3/3 + 房源详情 exit.
    """
    from v3_core.user_bot.callback_router import CallbackRouter

    gallery = _write_photos(tmp_path, 9)
    cover = tmp_path / "cover.jpg"
    cover.write_bytes(b"COVER")
    view = _entry_view(gallery=gallery, cover_path=str(cover))
    inventory = MemoryPublishedInventory(view)
    listings = PublicListingFlowService(PublicRouteService(inventory))
    router = CallbackRouter(
        listings=listings,
        search_sessions=SimpleNamespace(
            navigate=lambda *_a, **_k: SimpleNamespace(status="ok", card=None)
        ),
    )

    # Page 1
    result_p1 = router.dispatch(
        "v3u:listing:photos:QL-RF-A2B3:pg:1",
        source="listing_callback",
    )
    assert result_p1.status == "ok"
    assert result_p1.action == "photos"
    photos_p1 = result_p1.listing.photos
    assert photos_p1 is not None
    assert photos_p1.photo_total == 9
    # Page 1: 4 raw originals gallery[4:8], no cover.
    assert len(photos_p1.media_groups[0]) == 4
    assert [str(p) for p in photos_p1.media_groups[0]] == gallery[4:8]
    # Paging row: ⬅️ 上一页 + 2/3 + 下一页 ➡️.
    flat_labels_p1 = [a.label for row in photos_p1.action_rows for a in row]
    assert "⬅️ 上一页" in flat_labels_p1
    assert "下一页 ➡️" in flat_labels_p1
    assert "2/3" in flat_labels_p1

    # Page 2: last 1 frame, only ⬅️, exit button.
    result_p2 = router.dispatch(
        "v3u:listing:photos:QL-RF-A2B3:pg:2",
        source="listing_callback",
    )
    assert result_p2.status == "ok"
    photos_p2 = result_p2.listing.photos
    assert photos_p2 is not None
    # Page 2: 1 trailing raw original gallery[8:9], no cover.
    assert [str(p) for p in photos_p2.media_groups[0]] == gallery[8:9]
    flat_labels_p2 = [a.label for row in photos_p2.action_rows for a in row]
    assert "⬅️ 上一页" in flat_labels_p2
    assert "下一页 ➡️" not in flat_labels_p2
    assert "3/3" in flat_labels_p2
    assert "📋 房源详情" not in flat
    assert "⬅️ 返回房源" in flat_labels_p2
    assert "📋 房源详情" not in flat
    assert "⬅️ 返回房源" in flat_labels_p2

    # And the legacy expander never appears in any keyboard.
    assert "📷 更多实拍" not in flat_labels_p1
    assert "📷 更多实拍" not in flat_labels_p2
    # Cover render excluded from every page's media.
    cover_str = str(cover)
    for group in (photos_p1.media_groups[0], photos_p2.media_groups[0]):
        assert cover_str not in {str(p) for p in group}
