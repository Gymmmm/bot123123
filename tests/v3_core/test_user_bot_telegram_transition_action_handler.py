from __future__ import annotations

from types import SimpleNamespace

import pytest

from v3_core.user_bot.telegram_transition_action_handler import handle_v3_transition_action
from v3_core.user_bot.transition_actions import (
    APPOINTMENT_AWAITING_DATE_KEY,
    TransitionActionService,
)
from v3_core.user_bot.transition_session import APPOINTMENT_SESSION_KEY, SEARCH_PREF_SESSION_KEY
from v3_core.user_bot.transition_views import TransitionChoice, TransitionView


PUBLIC_ID = "QL-RF-A2B3"


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


class _InventoryStub:
    def resolve(self, public_listing_id):
        if public_listing_id != PUBLIC_ID:
            return None
        return SimpleNamespace(
            snapshot={"schema": "v3_publication_snapshot.v1"},
            frozen_listing={
                "project_name": "富力城",
                "property_type": "公寓",
                "layout": "一房",
                "public_location_display": "富力城",
            },
            frozen_offer={"monthly_rent_usd": 680},
            listing={"inventory_status": "active"},
            listing_id="l_1",
            public_listing_id=PUBLIC_ID,
            bookable=True,
            gallery=(),
        )


class ViewsStub:
    inventory = _InventoryStub()

    def appointment_date(self, draft):
        return TransitionView(
            kind="appointment_date",
            text=f"date:{draft.mode}",
            rows=((TransitionChoice("今天", "appointment_date", "09-08"),),),
        )

    def appointment_time(self, draft):
        return TransitionView(
            kind="appointment_time",
            text=f"time:{draft.date}",
            rows=((TransitionChoice("下午", "appointment_time", "pm"),),),
        )

    def custom_date_prompt(self):
        return TransitionView(kind="appointment_custom_date", text="custom-date", rows=())

    def custom_time_prompt(self):
        return TransitionView(kind="appointment_custom_time", text="custom-time", rows=())


def _update(query):
    return SimpleNamespace(callback_query=query)


def _appt_user_data(*, mode="offline", date="", time=""):
    return {
        APPOINTMENT_SESSION_KEY: {
            "public_listing_id": PUBLIC_ID,
            "mode": mode,
            "date": date,
            "time": time,
            "source": "listing_callback",
        }
    }


def _context(user_data):
    return SimpleNamespace(user_data=user_data)


@pytest.mark.asyncio
async def test_non_transition_callback_is_not_claimed():
    query = FakeQuery(f"v3u:listing:details:{PUBLIC_ID}")
    context = _context(_appt_user_data())

    outcome = await handle_v3_transition_action(
        _update(query),
        context,
        actions=TransitionActionService(),
        views=ViewsStub(),
    )

    assert not outcome.handled
    assert query.calls == []


@pytest.mark.asyncio
async def test_date_choice_renders_time_then_advances_public_session():
    query = FakeQuery("v3u:t:appointment_date:09-10", has_photo=True)
    user_data = _appt_user_data()

    outcome = await handle_v3_transition_action(
        _update(query),
        _context(user_data),
        actions=TransitionActionService(),
        views=ViewsStub(),
    )

    assert outcome.handled and outcome.result is not None
    assert outcome.result.next_step == "appointment_time"
    assert [call[0] for call in query.calls] == ["answer", "edit_caption"]
    assert user_data[APPOINTMENT_SESSION_KEY]["date"] == "09-10"
    assert "LST_" not in repr(user_data)
    markup = query.calls[-1][2]["reply_markup"]
    assert markup.inline_keyboard[0][0].callback_data == "v3u:t:appointment_time:pm"


@pytest.mark.asyncio
async def test_custom_date_prompt_has_no_keyboard_and_sets_waiting_flag_after_edit():
    query = FakeQuery("v3u:t:appointment_other_date")
    user_data = _appt_user_data()

    outcome = await handle_v3_transition_action(
        _update(query),
        _context(user_data),
        actions=TransitionActionService(),
        views=ViewsStub(),
    )

    assert outcome.handled
    assert [call[0] for call in query.calls] == ["answer", "edit_text"]
    assert query.calls[-1][2]["reply_markup"] is None
    assert user_data[APPOINTMENT_AWAITING_DATE_KEY] is True


@pytest.mark.asyncio
async def test_render_failure_does_not_advance_session():
    query = FakeQuery("v3u:t:appointment_date:09-10", fail_edit=True)
    user_data = _appt_user_data()

    with pytest.raises(RuntimeError, match="telegram_edit_failed"):
        await handle_v3_transition_action(
            _update(query),
            _context(user_data),
            actions=TransitionActionService(),
            views=ViewsStub(),
        )

    assert user_data[APPOINTMENT_SESSION_KEY]["date"] == ""


@pytest.mark.asyncio
async def test_time_choice_renders_confirmation_and_does_not_persist():
    query = FakeQuery("v3u:t:appointment_time:pm")
    user_data = _appt_user_data(date="09-10")

    outcome = await handle_v3_transition_action(
        _update(query),
        _context(user_data),
        actions=TransitionActionService(),
        views=ViewsStub(),
    )

    assert outcome.handled and outcome.result is not None
    assert outcome.result.next_step == "appointment_submit"
    assert outcome.result.appointment is not None
    assert outcome.result.appointment.time == "pm"
    assert outcome.appointment_execution is None
    assert [call[0] for call in query.calls] == ["answer", "edit_text"]
    assert user_data[APPOINTMENT_SESSION_KEY]["time"] == "pm"
    rendered = query.calls[-1][1][0]
    assert "确认看房预约" in rendered
    markup = query.calls[-1][2]["reply_markup"]
    assert markup.inline_keyboard[0][0].callback_data == "v3u:t:appointment_submit"
    assert markup.inline_keyboard[1][0].callback_data == "v3u:t:appointment_back_time"


@pytest.mark.asyncio
async def test_budget_choice_stops_at_search_executor_boundary_without_popping_pref():
    query = FakeQuery("v3u:t:budget_choice:b2")
    user_data = {
        SEARCH_PREF_SESSION_KEY: {
            "source": "similar_listing",
            "goal": "any",
            "location_keys": ["BKK1"],
            "area_display": "BKK1",
            "touch_payload": {"from_public_listing_id": PUBLIC_ID},
        }
    }

    outcome = await handle_v3_transition_action(
        _update(query),
        _context(user_data),
        actions=TransitionActionService(),
        views=ViewsStub(),
    )

    assert outcome.handled and outcome.result is not None
    assert outcome.result.next_step == "search_submit"
    assert outcome.result.search is not None
    assert [call[0] for call in query.calls] == ["answer"]
    assert SEARCH_PREF_SESSION_KEY in user_data


@pytest.mark.asyncio
async def test_stale_transition_is_owned_but_does_not_mutate_or_render():
    query = FakeQuery("v3u:t:appointment_date:09-10")
    user_data = {}

    outcome = await handle_v3_transition_action(
        _update(query),
        _context(user_data),
        actions=TransitionActionService(),
        views=ViewsStub(),
    )

    assert outcome.handled and outcome.result is not None
    assert outcome.result.status == "expired"
    assert [call[0] for call in query.calls] == ["answer"]
    assert user_data == {}
