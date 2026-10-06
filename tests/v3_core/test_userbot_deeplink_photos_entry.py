"""Real /start deep-link entry contract for the V3 User Bot.

Channel deep links (``property_<id>_photos`` etc.) open one compact thumbnail
overview first. 「查看全部原图」 then enters a raw-only Telegram album with up
to 10 originals per page. Multi-page albums keep the visible N/M counter and
prev/next controls; single-page albums do not show a useless 1/1 row.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from v3_core.user_bot.callbacks import encode_semantic_action, parse_callback
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
        from PIL import Image
        Image.new("RGB", (800 + i * 5, 600 + i * 3), (40 + i, 70, 100)).save(p)
        paths.append(str(p))
    return paths


def test_deeplink_photos_routes_to_thumbnail_overview_first(tmp_path):
    gallery = _write_photos(tmp_path, 9)
    result = _service(_view(gallery=gallery)).resolve("property_QL-RF-A2B3_photos__ch")
    assert result.ok and result.action == "photos"
    assert result.photos is not None
    assert result.photos.photo_total == 9
    assert len(_flatten(result.photos.media_groups)) == 1
    assert "共 9 张实拍 · 当前预览 8 张" in result.photos.text
    rows = [[a.label for a in row] for row in result.photos.action_rows]
    assert rows[0] == ["📸 查看全部原图"]
    assert rows[-1] == ["⬅️ 返回房源"]
    parsed = parse_callback(encode_semantic_action(result.photos.action_rows[0][0]))
    assert parsed is not None and parsed.page_index == 0

def test_deeplink_photos_page_one_returns_raw_slice_ten_to_twenty(tmp_path):
    gallery = _write_photos(tmp_path, 23)
    page1 = _service(_view(gallery=gallery)).resolve_action(
        "QL-RF-A2B3", "photos", source="listing_callback", photo_page=1
    )
    assert page1.ok and page1.photos is not None
    assert _flatten(page1.photos.media_groups) == gallery[10:20]
    assert [a.label for a in page1.photos.action_rows[0]] == ["⬅️ 上一页", "2/3", "下一页 ➡️"]
    assert page1.photos.media_caption.endswith("共 23 张 · 第 2/3 页")


def test_deeplink_photos_last_page_returns_tail_with_prev_only(tmp_path):
    gallery = _write_photos(tmp_path, 23)
    page2 = _service(_view(gallery=gallery)).resolve_action(
        "QL-RF-A2B3", "photos", source="listing_callback", photo_page=2
    )
    assert page2.ok and page2.photos is not None
    assert _flatten(page2.photos.media_groups) == gallery[20:23]
    assert [a.label for a in page2.photos.action_rows[0]] == ["⬅️ 上一页", "3/3"]


def test_deeplink_photos_overview_excludes_cover_named_asset(tmp_path):
    cover = tmp_path / "cover.jpg"
    cover.write_bytes(b"cover")
    framed = _write_photos(tmp_path, 5)
    result = _service(_view(gallery=[str(cover), *framed], cover_path=str(cover))).resolve("property_QL-RF-A2B3_photos")
    assert result.ok and result.photos is not None
    assert result.photos.photo_total == 5
    assert "共 5 张实拍 · 当前预览 5 张" in result.photos.text


def test_deeplink_photos_route_uses_overview_before_raw_pages():
    import v3_core.user_bot.public_flow as flow_mod
    src = Path(flow_mod.__file__).read_text(encoding="utf-8")
    assert "photo_page = (\n            -1" in src or "photo_page = (-1" in src
    assert "build_photos_overview_response(" in src
    assert "build_photos_page_response(" in src


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
@pytest.mark.asyncio
async def test_entry_integration_nine_photos_opens_overview_via_start(tmp_path):
    gallery = _write_photos(tmp_path, 9)
    view = _entry_view(gallery=gallery, cover_path="")
    listings = PublicListingFlowService(PublicRouteService(MemoryPublishedInventory(view)))
    message = _StubMessage()
    bot = _StubBot()
    context = SimpleNamespace(args=["property_QL-RF-A2B3_photos__ch"], bot=bot, user_data={})

    outcome = await handle_v3_start(
        _start_update(message), context, listings=listings,
        transition_views=SimpleNamespace(build=lambda _plan: None),
        channel_url="https://t.me/qiaolian",
    )
    assert outcome.handled and outcome.kind == "photos"
    assert outcome.result is not None and outcome.result.source == "channel_listing"
    assert [c[0] for c in bot.calls] == ["send_photo", "send_message"]
    assert message.calls == []
    card = bot.calls[1][2]
    assert "共 9 张实拍 · 当前预览 8 张" in card["text"]
    labels = [[b.text for b in row] for row in card["reply_markup"].inline_keyboard]
    assert labels[0] == ["📸 查看全部原图"]
    assert labels[-1] == ["⬅️ 返回房源"]


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
    assert [a.label for a in photos_p1.action_rows[0]] == ["⬅️ 上一页", "2/3", "下一页 ➡️"]

    p2 = router.dispatch("v3u:listing:photos:QL-RF-A2B3:pg:2", source="listing_callback")
    photos_p2 = p2.listing.photos
    assert [str(p) for p in photos_p2.media_groups[0]] == gallery[20:23]
    rows = [[a.label for a in row] for row in photos_p2.action_rows]
    assert rows == [["⬅️ 上一页", "3/3"], ["📅 预约看房", "💬 咨询这套"], ["⬅️ 返回房源"]]

    for group in (photos_p1.media_groups[0], photos_p2.media_groups[0]):
        assert str(cover) not in {str(p) for p in group}
