from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from PIL import Image
from telegram.error import BadRequest

from v3_core.user_bot.callback_router import CallbackDispatchResult
from v3_core.user_bot.callbacks import encode_semantic_action, parse_callback
from v3_core.user_bot.listing_responses import build_photos_response
from v3_core.user_bot.public_flow import PublicListingFlowResult
from v3_core.user_bot.public_inventory import PublishedListingView
from v3_core.user_bot.telegram_callback_handler import handle_v3_callback


def _view(paths: list[str], *, cover: str = "") -> PublishedListingView:
    snapshot = {"schema": "v3_publication_snapshot.v1",
                "listing": {"project_name": "富力城", "public_location_display": "BKK1", "layout": "2房"},
                "offer": {"monthly_rent_usd": 800}}
    return PublishedListingView(
        listing={"listing_id": "LST_INTERNAL_1", "public_listing_id": "QL-RF-A2B3", "inventory_status": "active"},
        offer={"offer_type": "rent", "offer_status": "active", "publication_policy": "telegram_rent"},
        publication={},
        package={"snapshot_json": json.dumps(snapshot), "gallery_json": json.dumps(paths), "cover_path": cover},
    )


@pytest.mark.parametrize("count,pages", [(1, 1), (2, 1), (3, 1), (4, 1), (5, 2), (8, 2), (9, 3), (10, 3)])
def test_real_photo_pages_have_single_large_frame_and_boundaries(tmp_path: Path, monkeypatch, count: int, pages: int):
    monkeypatch.setenv("HOME", str(tmp_path))
    paths = []
    for number in range(count):
        source = tmp_path / f"room{number}.jpg"
        Image.new("RGB", (800, 600), (number * 20, 40, 90)).save(source)
        paths.append(str(source))
    cover = tmp_path / "cover.jpg"
    Image.new("RGB", (800, 600), (255, 0, 0)).save(cover)
    view = _view([str(cover), *paths], cover=str(cover))
    for page in range(pages):
        response = build_photos_response(view, offset=page * 4)
        assert response.photo_total == count
        assert response.photo_index == page
        assert response.text.endswith(f"第 {page + 1} / {pages} 页")
        assert "LST_INTERNAL_1" not in response.text and str(tmp_path) not in response.text
        assert len(response.media_groups) == 1 and len(response.media_groups[0]) == 1
        with Image.open(response.photo_path) as frame:
            assert frame.size == (1200, 1600)
        navigation = [action for action in response.action_rows[0] if action.action == "photos"]
        assert len(navigation) == int(page > 0) + int(page + 1 < pages)
        assert all(encode_semantic_action(item).startswith("v3u:listing:photos:QL-RF-A2B3") for item in navigation)
        assert encode_semantic_action(next(item for item in response.action_rows[0] if item.action == "noop")) == "v3u:noop"
        assert response.photo_path == build_photos_response(view, offset=page * 4).photo_path
    assert build_photos_response(view, offset=400).photo_index == pages - 1


def test_gallery_cache_changes_when_a_photo_changes(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    photo = tmp_path / "room.jpg"
    Image.new("RGB", (800, 600), "red").save(photo)
    view = _view([str(photo)])
    first = build_photos_response(view).photo_path
    Image.new("RGB", (800, 600), "blue").save(photo)
    assert build_photos_response(view).photo_path != first


class Query:
    def __init__(self, data: str, *, fail_edit: bool = False):
        self.data = data
        self.message = SimpleNamespace(photo=[object()])
        self.calls = []
        self.fail_edit = fail_edit

    async def answer(self, *args, **kwargs):
        self.calls.append("answer")

    async def edit_message_media(self, *args, **kwargs):
        self.calls.append("edit_media")
        if self.fail_edit:
            raise BadRequest("message can't be edited")


class Router:
    def __init__(self, result):
        self.result = result
        self.calls = []

    def dispatch(self, raw, **kwargs):
        self.calls.append(raw)
        return self.result


@pytest.mark.asyncio
async def test_page_flip_edits_and_failure_sends_only_one_fallback(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    paths = []
    for i in range(5):
        path = tmp_path / f"room{i}.jpg"
        Image.new("RGB", (800, 600), (i * 30, 60, 90)).save(path)
        paths.append(str(path))
    photos = build_photos_response(_view(paths), offset=4)
    callback = "v3u:listing:photos:QL-RF-A2B3:4"
    result = CallbackDispatchResult(
        status="ok", action="photos", callback=parse_callback(callback),
        listing=PublicListingFlowResult(status="ok", action="photos", public_listing_id="QL-RF-A2B3", photos=photos),
    )
    calls = []

    async def send_photo(**kwargs):
        calls.append("send_photo")

    context = SimpleNamespace(bot=SimpleNamespace(send_photo=send_photo), user_data={})
    update = lambda query: SimpleNamespace(callback_query=query, effective_chat=SimpleNamespace(id=1))
    query = Query(callback)
    await handle_v3_callback(update(query), context, router=Router(result))
    assert query.calls == ["answer", "edit_media"] and calls == []
    query = Query(callback, fail_edit=True)
    await handle_v3_callback(update(query), context, router=Router(result))
    assert query.calls == ["answer", "edit_media"] and calls == ["send_photo"]

    noop = Query("v3u:noop")
    await handle_v3_callback(update(noop), context, router=Router(result))
    assert noop.calls == ["answer"] and calls == ["send_photo"]
