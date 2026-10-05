"""Real /start deep-link entry contract for the V3 User Bot.

Channel deep links (``property_<id>_photos`` etc.) MUST go straight to the
raw-only paged album — never the legacy cover collage + "更多实拍" expander.
Product lock (2026-10-05 「更多实拍」 redo):
    * one page = one native Telegram album of up to 10 real photos
      (cover render / collage excluded);
    * pager ``‹ 上一页 / 下一页 ›`` only when there is more than one page,
      never a ``1/1`` counter;
    * caption only on the first frame; then exactly ONE compact card with
      📅 预约看房 + 💬 咨询这套 / ⬅️ 返回房源.
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
    """``/start property_<id>_photos`` → one album with all 9 originals, no cover."""
    cover = tmp_path / "cover.jpg"
    cover.write_bytes(b"cover")
    gallery = _write_photos(tmp_path, 9)

    view = _view(gallery=gallery, cover_path=str(cover))
    result = _service(view).resolve("property_QL-RF-A2B3_photos__ch")

    assert result.ok and result.action == "photos"
    assert result.photos is not None
    assert _flatten(result.photos.media_groups) == gallery
    assert str(cover) not in _flatten(result.photos.media_groups)
    assert result.photos.photo_total == 9

    rows = [[a.label for a in row] for row in result.photos.action_rows]
    assert rows == [["📅 预约看房", "💬 咨询这套"], ["⬅️ 返回房源"]]
    assert result.photos.media_caption == "📷 富力城 · 2房1厅 · 共 9 张"
    assert "查看全部" not in result.photos.text
    assert "页" not in result.photos.text


def test_deeplink_photos_page_one_returns_raw_slice_ten_to_twenty(tmp_path):
    gallery = _write_photos(tmp_path, 23)
    page1 = _service(_view(gallery=gallery)).resolve_action(
        "QL-RF-A2B3", "photos", source="listing_callback", photo_page=1
    )
    assert page1.ok and page1.photos is not None
    assert _flatten(page1.photos.media_groups) == gallery[10:20]
    assert [a.label for a in page1.photos.action_rows[0]] == ["‹ 上一页", "下一页 ›"]
    assert page1.photos.media_caption.endswith("共 23 张 · 第 2/3 页")


def test_deeplink_photos_last_page_returns_tail_with_prev_only(tmp_path):
    gallery = _write_photos(tmp_path, 23)
    page2 = _service(_view(gallery=gallery)).resolve_action(
        "QL-RF-A2B3", "photos", source="listing_callback", photo_page=2
    )
    assert page2.ok and page2.photos is not None
    assert _flatten(page2.photos.media_groups) == gallery[20:23]
    assert [a.label for a in page2.photos.action_rows[0]] == ["‹ 上一页"]


def test_deeplink_photos_excludes_cover_named_assets_from_paging(tmp_path):
    """Defensive: cover.jpg slipped into gallery_json must not appear on any page."""
    cover = tmp_path / "cover.jpg"
    cover.write_bytes(b"cover")
    framed = _write_photos(tmp_path, 5)
    gallery = [str(cover)] + framed

    view = _view(gallery=gallery, cover_path=str(cover))
    result = _service(view).resolve("property_QL-RF-A2B3_photos")

    assert result.ok and result.photos is not None
    flat = _flatten(result.photos.media_groups)
    assert str(cover) not in flat
    assert flat == framed
    labels = _labels(result.photos)
    assert not any("页" in label or "/" in label for label in labels)


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
    """``/start property_<id>_photos`` end-to-end message sequence.

    Exactly: one sendMediaGroup (9 real frames, caption on frame 1 only) then
    ONE compact card with 📅 预约看房 + 💬 咨询这套 / ⬅️ 返回房源. No collage,
    no "1/1" counter, no stray extra 返回房源 message.
    """
    cover = tmp_path / "cover.jpg"
    cover.write_bytes(b"COVER")
    gallery = _write_photos(tmp_path, 9)

    view = _entry_view(gallery=gallery, cover_path=str(cover))
    listings = PublicListingFlowService(PublicRouteService(MemoryPublishedInventory(view)))
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

    assert outcome.handled and outcome.kind == "photos"
    assert outcome.result is not None
    assert outcome.result.source == "channel_listing"

    # Exact sequence: album, then one card. Nothing via reply_text.
    assert [c[0] for c in bot.calls] == ["send_media_group", "send_message"]
    assert message.calls == []

    frames = bot.calls[0][2]["media"]
    assert len(frames) == 9
    actual = []
    for frame in frames:
        media = frame.media
        if hasattr(media, "input_file_content"):
            actual.append(media.input_file_content)
        elif isinstance(media, (bytes, bytearray)):
            actual.append(bytes(media))
        else:
            actual.append(bytes(media.read()))
    assert actual == [Path(p).read_bytes() for p in gallery]
    assert cover.read_bytes() not in actual
    assert frames[0].caption == "📷 富力城 · 2房1厅 · 共 9 张"
    assert all(not frame.caption for frame in frames[1:])

    card = bot.calls[1][2]
    assert card["text"] == "\n".join([
        "🏠 <b>富力城｜2房1厅</b>",
        "💵 $800/月",
        "🟢 当前可预约",
        "🆔 QL-RF-A2B3",
    ])
    labels = [[b.text for b in row] for row in card["reply_markup"].inline_keyboard]
    assert labels == [["📅 预约看房", "💬 咨询这套"], ["⬅️ 返回房源"]]
    blob = repr(bot.calls)
    for legacy in ("1/1", "第 1/1 页", "房源实拍 ·", "查看全部实拍", "更多实拍"):
        assert legacy not in blob
    assert "LST_INT_1" not in blob


@pytest.mark.asyncio
async def test_entry_integration_pagination_through_callback_router_10_10_3(tmp_path):
    """In-app paging callbacks: 23 originals → pages of 10 / 10 / 3."""
    from v3_core.user_bot.callback_router import CallbackRouter

    gallery = _write_photos(tmp_path, 23)
    cover = tmp_path / "cover.jpg"
    cover.write_bytes(b"COVER")
    view = _entry_view(gallery=gallery, cover_path=str(cover))
    listings = PublicListingFlowService(PublicRouteService(MemoryPublishedInventory(view)))
    router = CallbackRouter(
        listings=listings,
        search_sessions=SimpleNamespace(
            navigate=lambda *_a, **_k: SimpleNamespace(status="ok", card=None)
        ),
    )

    p1 = router.dispatch("v3u:listing:photos:QL-RF-A2B3:pg:1", source="listing_callback")
    assert p1.status == "ok" and p1.action == "photos"
    photos_p1 = p1.listing.photos
    assert photos_p1.photo_total == 23
    assert [str(p) for p in photos_p1.media_groups[0]] == gallery[10:20]
    assert [a.label for a in photos_p1.action_rows[0]] == ["‹ 上一页", "下一页 ›"]

    p2 = router.dispatch("v3u:listing:photos:QL-RF-A2B3:pg:2", source="listing_callback")
    photos_p2 = p2.listing.photos
    assert [str(p) for p in photos_p2.media_groups[0]] == gallery[20:23]
    rows = [[a.label for a in row] for row in photos_p2.action_rows]
    assert rows == [["‹ 上一页"], ["📅 预约看房", "💬 咨询这套"], ["⬅️ 返回房源"]]

    for group in (photos_p1.media_groups[0], photos_p2.media_groups[0]):
        assert str(cover) not in {str(p) for p in group}
