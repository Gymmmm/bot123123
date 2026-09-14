from __future__ import annotations

from types import SimpleNamespace

import pytest

from v3_core.user_bot.appointment_service import AppointmentSubmissionResult
from v3_core.user_bot.appointment_side_effects import AppointmentEffectPlan
from v3_core.user_bot.appointment_submit_executor import AppointmentSubmitExecution
from v3_core.user_bot.telegram_transition_action_handler import handle_v3_transition_action
from v3_core.user_bot.transition_actions import TransitionActionService
from v3_core.user_bot.transition_session import APPOINTMENT_SESSION_KEY


PUBLIC_ID = "QL-RF-A2B3"


class FakeQuery:
    def __init__(self, data, *, fail_edit=False):
        self.data = data
        self.message = SimpleNamespace(photo=[])
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


class EmptyInventory:
    def resolve(self, public_listing_id):
        return None


class ViewsStub:
    inventory = EmptyInventory()

    def appointment_date(self, draft):
        raise AssertionError("submit must not rerender date")

    def appointment_time(self, draft):
        raise AssertionError("submit must not rerender time")

    def custom_date_prompt(self):
        raise AssertionError("submit must not render custom date")

    def custom_time_prompt(self):
        raise AssertionError("submit must not render custom time")


class FakeAppointmentExecutor:
    def __init__(self, *, fail=False):
        self.fail = fail
        self.calls = []

    def execute(self, *, user, draft, edit_appointment_id=0, created_at=""):
        self.calls.append((user, draft, edit_appointment_id, created_at))
        if self.fail:
            raise ValueError("listing_not_bookable")
        submission = AppointmentSubmissionResult(
            kind="created",
            appointment_id=41,
            lead_action="appointment_submit",
            lead_source=draft.source,
        )
        return AppointmentSubmitExecution(
            public_listing_id=draft.public_listing_id,
            listing_id="LST_INTERNAL_1",
            submission=submission,
            effects=AppointmentEffectPlan(
                (
                    "recompute_listing_availability",
                    "record_lead",
                    "sync_channel",
                    "notify_admin",
                )
            ),
        )


def _user_data():
    return {
        APPOINTMENT_SESSION_KEY: {
            "public_listing_id": PUBLIC_ID,
            "mode": "offline",
            "date": "09-10",
            "time": "pm",
            "source": "listing_callback",
        }
    }


def _update(query):
    return SimpleNamespace(
        callback_query=query,
        effective_user=SimpleNamespace(
            id=123,
            username="alice",
            full_name="Alice",
            first_name="Alice",
            last_name="",
        ),
    )


@pytest.mark.asyncio
async def test_submit_executor_success_renders_public_success_then_clears_session():
    query = FakeQuery("v3u:t:appointment_submit")
    user_data = _user_data()
    executor = FakeAppointmentExecutor()

    outcome = await handle_v3_transition_action(
        _update(query),
        SimpleNamespace(user_data=user_data),
        actions=TransitionActionService(),
        views=ViewsStub(),
        appointment_executor=executor,
    )

    assert outcome.handled
    assert outcome.appointment_execution is not None
    assert outcome.appointment_execution.submission.appointment_id == 41
    assert outcome.appointment_execution.effects.includes("notify_admin")
    assert APPOINTMENT_SESSION_KEY not in user_data
    assert [call[0] for call in query.calls] == ["answer", "edit_text"]
    rendered = query.calls[-1][1][0]
    assert "预约申请已提交" in rendered
    assert PUBLIC_ID in rendered
    assert "LST_INTERNAL_1" not in rendered
    assert executor.calls[0][0].contact_value == "@alice"
    assert executor.calls[0][1].time == "pm"


@pytest.mark.asyncio
async def test_persistence_failure_keeps_session_and_never_renders_success():
    query = FakeQuery("v3u:t:appointment_submit")
    user_data = _user_data()
    executor = FakeAppointmentExecutor(fail=True)

    with pytest.raises(ValueError, match="listing_not_bookable"):
        await handle_v3_transition_action(
            _update(query),
            SimpleNamespace(user_data=user_data),
            actions=TransitionActionService(),
            views=ViewsStub(),
            appointment_executor=executor,
        )

    assert APPOINTMENT_SESSION_KEY in user_data
    assert user_data[APPOINTMENT_SESSION_KEY]["time"] == "pm"
    assert [call[0] for call in query.calls] == ["answer"]


@pytest.mark.asyncio
async def test_telegram_failure_after_persistence_keeps_session_for_idempotent_retry():
    query = FakeQuery("v3u:t:appointment_submit", fail_edit=True)
    user_data = _user_data()
    executor = FakeAppointmentExecutor()

    with pytest.raises(RuntimeError, match="telegram_edit_failed"):
        await handle_v3_transition_action(
            _update(query),
            SimpleNamespace(user_data=user_data),
            actions=TransitionActionService(),
            views=ViewsStub(),
            appointment_executor=executor,
        )

    assert len(executor.calls) == 1
    assert APPOINTMENT_SESSION_KEY in user_data
    assert user_data[APPOINTMENT_SESSION_KEY]["time"] == "pm"
    assert [call[0] for call in query.calls] == ["answer", "edit_text"]
