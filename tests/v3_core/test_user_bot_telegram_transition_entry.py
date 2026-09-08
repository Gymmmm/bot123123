from __future__ import annotations

from types import SimpleNamespace

import pytest

from v3_core.user_bot.callback_router import CallbackDispatchResult
from v3_core.user_bot.consult import ConsultIntent, ConsultResult
from v3_core.user_bot.public_flow import PublicBookIntent, PublicListingFlowResult
from v3_core.user_bot.telegram_callback_handler import handle_v3_callback
from v3_core.user_bot.transition_session import APPOINTMENT_SESSION_KEY
from v3_core.user_bot.transition_views import TransitionChoice, TransitionView


PUBLIC_ID = "QL-RF-A2B3"


class RouterStub:
    def __init__(self, result):
        self.result = result
        self.calls = []

    def dispatch(self, raw, *, session_public_listing_ids=()):
        self.calls.append((raw, tuple(session_public_listing_ids)))
        return self.result


class ViewServiceStub:
    def __init__(self, view):
        self.view = view
        self.plans = []

    def build(self, plan):
        self.plans.append(plan)
        return self.view


class FakeQuery:
    def __init__(self, data, *, has_photo=False, fail_edit=False):
        self.data = data
        self.message = SimpleNamespace(photo=[object()] if has_photo else [])
        self.fail_edit = fail_edit
        self.calls = []

    async def answer(self, *args, **kwargs):
        self.calls.append(("answer", args, kwargs))

    async def edit_message_text(self, *args, **kwargs):
        self.calls.append(("edit_text", args, kwargs))
        if self.fail_edit:
            raise RuntimeError("telegram_edit_failed")

    async def edit_message_caption(self, *args, **kwargs):
        self.calls.append(("edit_caption", args, kwargs))
        if self.fail_edit:
            raise RuntimeError("telegram_edit_failed")


class FakeBot:
    pass


def _update(query):
    return SimpleNamespace(
        callback_query=query,
        effective_chat=SimpleNamespace(id=12345),
    )


def _context():
    return SimpleNamespace(bot=FakeBot(), user_data={})


def _book_dispatch():
    return CallbackDispatchResult(
        status="ok",
        action="book",
        listing=PublicListingFlowResult(
            status="ok",
            action="book",
            public_listing_id=PUBLIC_ID,
            book=PublicBookIntent(
                listing_id="LST_1",
                public_listing_id=PUBLIC_ID,
                source="listing_callback",
                start_payload="",
            ),
        ),
    )


def _appointment_view():
    return TransitionView(
        kind="appointment_date",
        text="📅 预约看房",
        rows=(
            (
                TransitionChoice("今天", "appointment_date", "09-08"),
                TransitionChoice("明天", "appointment_date", "09-09"),
            ),
        ),
    )


@pytest.mark.asyncio
async def test_child_transition_callback_is_not_claimed_by_listing_router_handler():
    query = FakeQuery("v3u:t:appointment_date:09-08")
    router = RouterStub(_book_dispatch())
    context = _context()

    outcome = await handle_v3_callback(_update(query), context, router=router)

    assert not outcome.handled
    assert outcome.response is None
    assert router.calls == []
    assert query.calls == []
    assert context.user_data == {}


@pytest.mark.asyncio
async def test_book_entry_renders_transition_and_persists_public_only_session_after_edit():
    query = FakeQuery(f"v3u:listing:book:{PUBLIC_ID}", has_photo=True)
    router = RouterStub(_book_dispatch())
    views = ViewServiceStub(_appointment_view())
    context = _context()

    outcome = await handle_v3_callback(
        _update(query),
        context,
        router=router,
        transition_views=views,
    )

    assert outcome.handled and outcome.response is not None
    assert outcome.response.transition == "book"
    assert [call[0] for call in query.calls] == ["answer", "edit_caption"]
    assert len(views.plans) == 1 and views.plans[0].kind == "book"
    assert context.user_data[APPOINTMENT_SESSION_KEY] == {
        "public_listing_id": PUBLIC_ID,
        "mode": "offline",
        "date": "",
        "time": "",
        "source": "listing_callback",
    }
    assert "LST_1" not in repr(context.user_data)
    markup = query.calls[-1][2]["reply_markup"]
    assert markup.inline_keyboard[0][0].callback_data == "v3u:t:appointment_date:09-08"


@pytest.mark.asyncio
async def test_transition_session_does_not_advance_when_telegram_edit_fails():
    query = FakeQuery(f"v3u:listing:book:{PUBLIC_ID}", fail_edit=True)
    router = RouterStub(_book_dispatch())
    views = ViewServiceStub(_appointment_view())
    context = _context()

    with pytest.raises(RuntimeError, match="telegram_edit_failed"):
        await handle_v3_callback(
            _update(query),
            context,
            router=router,
            transition_views=views,
        )

    assert APPOINTMENT_SESSION_KEY not in context.user_data


@pytest.mark.asyncio
async def test_consult_remains_intent_only_even_when_transition_views_are_injected():
    query = FakeQuery(f"v3u:listing:consult:{PUBLIC_ID}")
    router = RouterStub(
        CallbackDispatchResult(
            status="ok",
            action="consult",
            consult=ConsultResult(
                status="ok",
                public_listing_id=PUBLIC_ID,
                intent=ConsultIntent(
                    listing_id="LST_1",
                    public_listing_id=PUBLIC_ID,
                    source="listing_callback",
                    inventory_status="rented",
                    offer_status="inactive",
                    publication_instance_id="PUB_1",
                ),
            ),
        )
    )
    views = ViewServiceStub(_appointment_view())
    context = _context()

    outcome = await handle_v3_callback(
        _update(query),
        context,
        router=router,
        transition_views=views,
    )

    assert outcome.handled and outcome.response is not None
    assert outcome.response.transition == "consult"
    assert [call[0] for call in query.calls] == ["answer"]
    assert views.plans == []
    assert context.user_data == {}
