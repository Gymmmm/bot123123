import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from telegram.error import BadRequest
from v3_core.user_bot.telegram_edit import edit_query_panel


@pytest.mark.parametrize("kind", ["photo", "video", "animation", "document", "audio", "voice", None])
def test_panel_uses_caption_for_media_and_text_for_plain_messages(kind):
    message = SimpleNamespace(**({kind: object()} if kind else {}))
    query = SimpleNamespace(message=message, edit_message_caption=AsyncMock(), edit_message_text=AsyncMock())
    assert asyncio.run(edit_query_panel(query, text="返回服务", reply_markup=None))
    chosen = query.edit_message_caption if kind else query.edit_message_text
    other = query.edit_message_text if kind else query.edit_message_caption
    assert chosen.await_count == 1
    assert other.await_count == 0


def test_repeated_video_panel_is_noop_but_real_errors_propagate():
    query = SimpleNamespace(message=SimpleNamespace(video=object()), edit_message_caption=AsyncMock(side_effect=BadRequest("Message is not modified")), edit_message_text=AsyncMock())
    assert asyncio.run(edit_query_panel(query, text="same")) is False
    query.edit_message_caption.side_effect = BadRequest("Message to edit not found")
    with pytest.raises(BadRequest):
        asyncio.run(edit_query_panel(query, text="missing"))
