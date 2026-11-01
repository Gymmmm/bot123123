import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from telegram.error import BadRequest, TimedOut
from v3_core.user_bot.telegram_photos_render import send_listing_photos_album
from v3_core.user_bot.transition_views import _search_area_view, AREA_OPTIONS


def test_all_configured_area_choices_remain_accessible():
    view = _search_area_view()
    values = {choice.value for row in view.rows for choice in row if choice.kind == "area_choice"}
    assert values == {code for code, _ in AREA_OPTIONS}


def _paths(tmp_path, count):
    paths = []
    for i in range(count):
        path = tmp_path / f"{i}.jpg"
        path.write_bytes(f"photo-{i}".encode())
        paths.append(str(path))
    return (tuple(paths),)


@pytest.mark.parametrize("count", [11, 21])
def test_large_album_sends_every_photo_without_truncation(tmp_path, count):
    bot = SimpleNamespace(send_media_group=AsyncMock(return_value=[SimpleNamespace(message_id=1)]),
                          send_photo=AsyncMock(return_value=SimpleNamespace(message_id=2)))
    asyncio.run(send_listing_photos_album(bot, chat_id=1, media_groups=_paths(tmp_path, count), expand_only=True))
    assert sum(len(call.kwargs["media"]) for call in bot.send_media_group.await_args_list) + bot.send_photo.await_count == count
    assert all(2 <= len(call.kwargs["media"]) <= 10 for call in bot.send_media_group.await_args_list)


def test_rejected_later_batch_falls_back_only_to_that_batch(tmp_path):
    captured = []
    async def photo(**kwargs):
        captured.append(kwargs["photo"].read())
        return SimpleNamespace(message_id=99)
    bot = SimpleNamespace(
        send_media_group=AsyncMock(side_effect=[[SimpleNamespace(message_id=i) for i in range(10)], BadRequest("rejected")]),
        send_photo=photo)
    ids = asyncio.run(send_listing_photos_album(bot, chat_id=1, media_groups=_paths(tmp_path, 12), expand_only=True))
    assert captured == [b"photo-10", b"photo-11"]
    assert len(ids) == 12


def test_ambiguous_timeout_does_not_resend_photos(tmp_path):
    bot = SimpleNamespace(send_media_group=AsyncMock(side_effect=TimedOut()), send_photo=AsyncMock())
    with pytest.raises(TimedOut):
        asyncio.run(send_listing_photos_album(bot, chat_id=1, media_groups=_paths(tmp_path, 2), expand_only=True))
    bot.send_photo.assert_not_awaited()
