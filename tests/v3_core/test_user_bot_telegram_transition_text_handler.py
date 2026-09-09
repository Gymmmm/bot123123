from __future__ import annotations

from types import SimpleNamespace

import pytest

from v3_core.user_bot.appointment_service import AppointmentSubmissionResult
from v3_core.user_bot.appointment_side_effects import AppointmentEffectPlan
from v3_core.user_bot.appointment_submit_executor import AppointmentSubmitExecution
from v3_core.user_bot.listing_responses import SemanticAction
from v3_core.user_bot.search_cards import SearchCardResponse
from v3_core.user_bot.search_flow import SearchFlowResult
from v3_core.user_bot.search_submit_executor import SearchSubmitExecution
from v3_core.user_bot.telegram_callback_handler import SEARCH_SESSION_KEY
from v3_core.user_bot.telegram_transition_text_handler import handle_v3_transition_text
from v3_core.user_bot.transition_actions import (
    APPOINTMENT_AWAITING_DATE_KEY,
    APPOINTMENT_AWAITING_TIME_KEY,
    LAST_SEARCH_PREF_KEY,
    SEARCH_AWAITING_BUDGET_KEY,
)
from v3_core.user_bot.transition_session import APPOINTMENT_SESSION_KEY, SEARCH_PREF_SESSION_KEY
from v3_core.user_bot.transition_text_actions import TransitionTextActionService
from v3_core.user_bot.transition_views import TransitionChoice, TransitionView


PUBLIC_ID = "QL-RF-A2B3"


class FakeMessage:
    def __init__(self, text, *, fail_reply=False):
        self.text = text
        self.fail_reply = fail_reply
        self.calls = []

    async def reply_text(self, *args, **kwargs):
        self.calls.append(("reply_text", args, kwargs))
        if self.fail_reply:
            raise RuntimeError("telegram_reply_failed")
        return SimpleNamespace(chat_id=12345, message_id=88)


class FakeBot:
    def __init__(self, *, fail_send=False):
        self.fail_send = fail_send
        self.calls = []

    async def send_message(self, *args, **kwargs):
        self.calls.append(("send_message", args, kwargs))
        if self.fail_send:
            raise RuntimeError("telegram_send_failed")
        return SimpleNamespace(chat_id=kwargs.get("chat_id", 12345), message_id=99)

    async def send_photo(self, *args, **kwargs):
        raise AssertionError("test cards do not use photo files")


class EmptyInventory:
    def resolve(self, public_listing_id):
        return None


class ViewsStub:
    inventory = EmptyInventory()

    def appointment_time(self, draft):
        return TransitionView(
            kind="appointment_time",
            text=f"time:{draft.date}",
            rows=((TransitionChoice("下午", "appointment_time", "pm"),),),
        )


class FakeAppointmentExecutor:
    def __init__(self, *, fail=False):
        self.fail = fail
        self.calls = []

    def execute(self, *, user, draft, edit_appointment_id=0, created_at=""):
        self.calls.append((user, draft))
        if self.fail:
            raise ValueError("listing_not_bookable")
        submission = AppointmentSubmissionResult(
            kind="created",
            appointment_id=51,
            lead_action="appointment_submit",
            lead_source=draft.source,
        )
        return AppointmentSubmitExecution(
            public_listing_id=draft.public_listing_id,
            listing_id="LST_INTERNAL_1",
            submission=submission,
            effects=AppointmentEffectPlan(("record_lead", "notify_admin")),
        )


class FakeSearchExecutor:
    def __init__(self, *, matched=True):
        self.matched = matched
        self.calls = []

    def execute(self, intent, *, limit=5):
        self.calls.append(intent)
        if self.matched:
            cards = (
                SearchCardResponse(
                    public_listing_id=PUBLIC_ID,
                    text="🏠 <b>BKK1｜1房</b>",
                    photo_path="",
                    action_rows=((SemanticAction("房源详情", "details", PUBLIC_ID),),),
                    index=0,
                    total=1,
                ),
            )
            mode = "strict"
            ids = (PUBLIC_ID,)
        else:
            cards = ()
            mode = "no_match"
            ids = ()
        return SearchSubmitExecution(
            intent=intent,
            result=SearchFlowResult(
                criteria=intent.criteria,
                mode=mode,
                has_similar=False,
                cards=cards,
                public_listing_ids=ids,
            ),
        )


def _update(message):
    return SimpleNamespace(
        effective_message=message,
        effective_user=SimpleNamespace(
            id=123,
            username="alice",
            full_name="Alice",
            first_name="Alice",
            last_name="",
        ),
        effective_chat=SimpleNamespace(id=12345),
        callback_query=None,
    )


def _context(user_data, *, bot=None):
    return SimpleNamespace(user_data=user_data, bot=bot or FakeBot())


def _appt(*, awaiting_date=False, awaiting_time=False, date=""):
    data = {
        APPOINTMENT_SESSION_KEY: {
            "public_listing_id": PUBLIC_ID,
            "mode": "offline",
            "date": date,
            "time": "",
            "source": "listing_callback",
        }
    }
    if awaiting_date:
        data[APPOINTMENT_AWAITING_DATE_KEY] = True
    if awaiting_time:
        data[APPOINTMENT_AWAITING_TIME_KEY] = True
    return data


def _budget_session():
    return {
        SEARCH_AWAITING_BUDGET_KEY: True,
        SEARCH_PREF_SESSION_KEY: {
            "source": "similar_listing",
            "goal": "any",
            "location_keys": ["BKK1"],
            "area_display": "BKK1",
            "touch_payload": {"from_public_listing_id": PUBLIC_ID},
        },
    }


@pytest.mark.asyncio
async def test_plain_text_is_not_claimed_without_explicit_awaiting_state():
    message = FakeMessage("BKK1 一房 600")
    outcome = await handle_v3_transition_text(
        _update(message),
        _context({}),
        actions=TransitionTextActionService(),
        views=ViewsStub(),
    )

    assert not outcome.handled
    assert message.calls == []


@pytest.mark.asyncio
async def test_custom_date_reply_succeeds_before_waiting_state_advances():
    message = FakeMessage("0905")
    user_data = _appt(awaiting_date=True)

    outcome = await handle_v3_transition_text(
        _update(message),
        _context(user_data),
        actions=TransitionTextActionService(),
        views=ViewsStub(),
    )

    assert outcome.handled and outcome.result is not None
    assert outcome.result.next_step == "appointment_time"
    assert APPOINTMENT_AWAITING_DATE_KEY not in user_data
    assert user_data[APPOINTMENT_SESSION_KEY]["date"] == "9月5日"
    markup = message.calls[-1][2]["reply_markup"]
    assert markup.inline_keyboard[0][0].callback_data == "v3u:t:appointment_time:pm"


@pytest.mark.asyncio
async def test_invalid_custom_date_keeps_waiting_state_and_returns_fixed_copy():
    message = FakeMessage("hello")
    user_data = _appt(awaiting_date=True)

    outcome = await handle_v3_transition_text(
        _update(message),
        _context(user_data),
        actions=TransitionTextActionService(),
        views=ViewsStub(),
    )

    assert outcome.result is not None and outcome.result.status == "invalid"
    assert APPOINTMENT_AWAITING_DATE_KEY in user_data
    assert "0905" in message.calls[-1][1][0]


@pytest.mark.asyncio
async def test_custom_time_persists_then_replies_success_and_clears_appointment_session():
    message = FakeMessage("20:00")
    user_data = _appt(awaiting_time=True, date="9月5日")
    executor = FakeAppointmentExecutor()

    outcome = await handle_v3_transition_text(
        _update(message),
        _context(user_data),
        actions=TransitionTextActionService(),
        views=ViewsStub(),
        appointment_executor=executor,
    )

    assert outcome.appointment_execution is not None
    assert APPOINTMENT_SESSION_KEY not in user_data
    assert APPOINTMENT_AWAITING_TIME_KEY not in user_data
    rendered = message.calls[-1][1][0]
    assert "预约申请已提交" in rendered
    assert PUBLIC_ID in rendered
    assert "LST_INTERNAL_1" not in rendered


@pytest.mark.asyncio
async def test_custom_time_executor_failure_preserves_retry_state():
    message = FakeMessage("20:00")
    user_data = _appt(awaiting_time=True, date="9月5日")

    with pytest.raises(ValueError, match="listing_not_bookable"):
        await handle_v3_transition_text(
            _update(message),
            _context(user_data),
            actions=TransitionTextActionService(),
            views=ViewsStub(),
            appointment_executor=FakeAppointmentExecutor(fail=True),
        )

    assert APPOINTMENT_SESSION_KEY in user_data
    assert APPOINTMENT_AWAITING_TIME_KEY in user_data
    assert message.calls == []


@pytest.mark.asyncio
async def test_custom_budget_matched_search_sends_public_card_then_consumes_pref():
    message = FakeMessage("600-900")
    user_data = _budget_session()
    bot = FakeBot()

    outcome = await handle_v3_transition_text(
        _update(message),
        _context(user_data, bot=bot),
        actions=TransitionTextActionService(),
        views=ViewsStub(),
        search_executor=FakeSearchExecutor(matched=True),
    )

    assert outcome.search_presentation is not None and outcome.search_presentation.matched
    assert SEARCH_PREF_SESSION_KEY not in user_data
    assert SEARCH_AWAITING_BUDGET_KEY not in user_data
    assert user_data[LAST_SEARCH_PREF_KEY]["budget_min"] == 600
    assert user_data[LAST_SEARCH_PREF_KEY]["budget_max"] == 900
    assert user_data[SEARCH_SESSION_KEY] == [PUBLIC_ID]
    assert "LST_" not in repr(user_data)
    assert [call[0] for call in bot.calls] == ["send_message"]


@pytest.mark.asyncio
async def test_custom_budget_telegram_failure_preserves_pref_and_waiting_state():
    message = FakeMessage("600-900")
    user_data = _budget_session()
    bot = FakeBot(fail_send=True)

    with pytest.raises(RuntimeError, match="telegram_send_failed"):
        await handle_v3_transition_text(
            _update(message),
            _context(user_data, bot=bot),
            actions=TransitionTextActionService(),
            views=ViewsStub(),
            search_executor=FakeSearchExecutor(matched=True),
        )

    assert SEARCH_PREF_SESSION_KEY in user_data
    assert SEARCH_AWAITING_BUDGET_KEY in user_data
    assert LAST_SEARCH_PREF_KEY not in user_data
    assert SEARCH_SESSION_KEY not in user_data


@pytest.mark.asyncio
async def test_custom_budget_no_match_uses_contact_followup_callback():
    message = FakeMessage("600-900")
    user_data = _budget_session()

    outcome = await handle_v3_transition_text(
        _update(message),
        _context(user_data),
        actions=TransitionTextActionService(),
        views=ViewsStub(),
        search_executor=FakeSearchExecutor(matched=False),
    )

    assert outcome.search_presentation is not None and not outcome.search_presentation.matched
    markup = message.calls[-1][2]["reply_markup"]
    callbacks = [button.callback_data for row in markup.inline_keyboard for button in row]
    assert callbacks == ["v3u:change_search", "v3u:home:contact", "v3u:t:home"]
    assert SEARCH_PREF_SESSION_KEY not in user_data
    assert SEARCH_AWAITING_BUDGET_KEY not in user_data
