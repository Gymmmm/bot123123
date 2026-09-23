from __future__ import annotations

import asyncio
from types import SimpleNamespace

from telegram.error import BadRequest

from v3_core.user_bot.home_views import HomeView
from v3_core.user_bot.service_views import ServiceView, local_life_view
from v3_core.user_bot.telegram_home_handler import handle_v3_home_callback
from v3_core.user_bot.telegram_service_handler import _send_view
from v3_core.user_bot.telegram_edit import edit_query_panel
from v3_core.user_bot.transition_views import TransitionView


class _Message:
    def __init__(self):
        self.photo = None
        self.chat_id = 100
        self.message_id = 200
        self.replies = []

    async def reply_text(self, *args, **kwargs):
        self.replies.append((args, kwargs))
        return SimpleNamespace(chat_id=self.chat_id, message_id=201)


class _Query:
    def __init__(self, data: str):
        self.data = data
        self.message = _Message()
        self.edits = []
        self.answers = 0

    async def answer(self, *args, **kwargs):
        self.answers += 1

    async def edit_message_text(self, text, **kwargs):
        self.edits.append(("text", text, kwargs))

    async def edit_message_caption(self, caption, **kwargs):
        self.edits.append(("caption", caption, kwargs))


class _Context:
    def __init__(self):
        self.user_data = {}
        self.bot = SimpleNamespace()


class _SearchViews:
    def build(self, plan):
        return TransitionView(
            kind="search_entry",
            text="SEARCH",
            rows=(),
        )


def _run(coro):
    return asyncio.run(coro)


def test_home_search_edits_existing_panel_instead_of_replying_new_message():
    query = _Query("v3u:home:search")
    update = SimpleNamespace(callback_query=query)
    context = _Context()

    outcome = _run(
        handle_v3_home_callback(
            update,
            context,
            appointment_history=SimpleNamespace(),
            search_views=_SearchViews(),
        )
    )

    assert outcome.handled is True
    assert outcome.rendered is True
    assert query.edits and query.edits[-1][1] == "SEARCH"
    assert query.message.replies == []


def test_service_send_view_edits_callback_panel_and_keeps_same_anchor():
    query = _Query("v3u:service:repair")
    update = SimpleNamespace(callback_query=query, effective_chat=SimpleNamespace(id=100))
    context = _Context()
    view = ServiceView("repair", "REPAIR", ())

    sent = _run(
        _send_view(
            update,
            context,
            view,
            anchor_key="anchor",
        )
    )

    assert sent is query.message
    assert query.edits and query.edits[-1][1] == "REPAIR"
    assert query.message.replies == []
    assert context.user_data["anchor"] == {"chat_id": 100, "message_id": 200}


def test_home_about_edits_brand_page_instead_of_assurance_or_new_message():
    query = _Query("v3u:home:about")
    update = SimpleNamespace(callback_query=query)
    context = _Context()

    outcome = _run(
        handle_v3_home_callback(
            update,
            context,
            appointment_history=SimpleNamespace(),
        )
    )

    assert outcome.handled is True
    assert outcome.rendered is True
    assert query.message.replies == []
    assert query.edits
    assert "侨联地产｜金边中文租房" in query.edits[-1][1]
    assert "真实房源" in query.edits[-1][1]


class _NoopQuery(_Query):
    async def edit_message_text(self, text, **kwargs):
        raise BadRequest("Message is not modified")


class _FailQuery(_Query):
    async def edit_message_text(self, text, **kwargs):
        raise BadRequest("Message to edit not found")


def test_edit_first_swallows_only_message_not_modified():
    query = _NoopQuery("v3u:home:search")
    assert _run(edit_query_panel(query, text="SAME")) is False


def test_edit_first_does_not_swallow_other_bad_request():
    query = _FailQuery("v3u:home:search")
    try:
        _run(edit_query_panel(query, text="BROKEN"))
    except BadRequest as exc:
        assert "Message to edit not found" in str(exc)
    else:
        raise AssertionError("non-noop BadRequest must be re-raised")


def test_home_handler_no_longer_constructs_v3_tenant_shadow_repository():
    from pathlib import Path
    source = Path("v3_core/user_bot/telegram_home_handler.py").read_text(encoding="utf-8")
    assert "SQLiteTenantServiceRepository" not in source
    assert "_tenant_service_from_history" not in source


def test_home_local_life_can_return_directly_to_home_without_changing_service_path():
    home_view = local_life_view(back_to_home=True)
    service_view = local_life_view()
    assert home_view.rows[-1][0].label == "返回首页"
    assert home_view.rows[-1][0].callback_data == "v3u:t:home"
    assert service_view.rows[-1][0].label == "返回侨联服务"
    assert service_view.rows[-1][0].callback_data == "v3u:home:service"
