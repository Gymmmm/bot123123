from __future__ import annotations

from types import SimpleNamespace

import pytest

from v3_core.user_bot.appointment_service import AppointmentSubmissionResult
from v3_core.user_bot.appointment_side_effects import AppointmentEffectPlan
from v3_core.user_bot.appointment_submit_executor import AppointmentSubmitExecution
from v3_core.user_bot.lead_effects import LeadEffectResult
from v3_core.user_bot.listing_responses import SemanticAction
from v3_core.user_bot.search_cards import SearchCardResponse
from v3_core.user_bot.search_flow import SearchFlowResult
from v3_core.user_bot.search_submit_executor import SearchSubmitExecution
from v3_core.user_bot.telegram_transition_action_handler import handle_v3_transition_action
from v3_core.user_bot.transition_actions import TransitionActionService
from v3_core.user_bot.transition_session import APPOINTMENT_SESSION_KEY, SEARCH_PREF_SESSION_KEY


PUBLIC_ID = "QL-RF-A2B3"


class FakeQuery:
    def __init__(self, data):
        self.data = data
        self.message = SimpleNamespace(photo=[])
        self.calls = []

    async def answer(self, *args, **kwargs):
        self.calls.append(("answer", args, kwargs))

    async def edit_message_text(self, *args, **kwargs):
        self.calls.append(("edit_text", args, kwargs))

    async def edit_message_caption(self, *args, **kwargs):
        self.calls.append(("edit_caption", args, kwargs))


class EmptyInventory:
    def resolve(self, public_listing_id):
        return None


class ViewsStub:
    inventory = EmptyInventory()

    def appointment_date(self, draft):
        raise AssertionError

    def appointment_time(self, draft):
        raise AssertionError

    def custom_date_prompt(self):
        raise AssertionError

    def custom_time_prompt(self):
        raise AssertionError


class FakeSearchExecutor:
    def execute(self, intent, *, limit=5):
        card = SearchCardResponse(
            public_listing_id=PUBLIC_ID,
            text="🏠 <b>BKK1｜1房</b>",
            photo_path="",
            action_rows=((SemanticAction("房源详情", "details", PUBLIC_ID),),),
            index=0,
            total=1,
        )
        return SearchSubmitExecution(
            intent=intent,
            result=SearchFlowResult(
                criteria=intent.criteria,
                mode="strict",
                has_similar=False,
                cards=(card,),
                public_listing_ids=(PUBLIC_ID,),
            ),
        )


class FakeAppointmentExecutor:
    def execute(self, *, user, draft, edit_appointment_id=0, created_at=""):
        return AppointmentSubmitExecution(
            public_listing_id=draft.public_listing_id,
            listing_id="l_1001",
            submission=AppointmentSubmissionResult(
                kind="created",
                appointment_id=51,
                lead_action="appointment_submit",
                lead_source=draft.source,
            ),
            effects=AppointmentEffectPlan(("record_lead", "notify_admin")),
        )


class FakeLeadEffects:
    def __init__(self):
        self.search_calls = []
        self.appointment_calls = []

    def record_search(self, *, user, intent):
        self.search_calls.append((user, intent))
        return LeadEffectResult(status="recorded")

    def record_appointment(self, *, user, execution, draft):
        self.appointment_calls.append((user, execution, draft))
        return LeadEffectResult(status="recorded")


def _update(query):
    return SimpleNamespace(
        callback_query=query,
        effective_chat=SimpleNamespace(id=12345),
        effective_user=SimpleNamespace(
            id=123,
            username="alice",
            full_name="Alice",
            first_name="Alice",
            last_name="",
        ),
    )


def _context(user_data):
    return SimpleNamespace(user_data=user_data, bot=SimpleNamespace())


@pytest.mark.asyncio
async def test_search_records_lead_only_after_successful_telegram_presentation():
    query = FakeQuery("v3u:t:budget_choice:b2")
    user_data = {
        SEARCH_PREF_SESSION_KEY: {
            "source": "user_search",
            "goal": "any",
            "location_keys": ["BKK1"],
            "area_display": "BKK1",
            "touch_payload": {},
        }
    }
    effects = FakeLeadEffects()

    outcome = await handle_v3_transition_action(
        _update(query),
        _context(user_data),
        actions=TransitionActionService(),
        views=ViewsStub(),
        search_executor=FakeSearchExecutor(),
        lead_effects=effects,
    )

    assert outcome.lead_effect is not None
    assert outcome.lead_effect.status == "recorded"
    assert len(effects.search_calls) == 1
    user, intent = effects.search_calls[0]
    assert user.user_id == 123
    assert user.username == "alice"
    assert intent.criteria.location_keys == ("BKK1",)
    assert SEARCH_PREF_SESSION_KEY not in user_data


@pytest.mark.asyncio
async def test_appointment_persists_then_records_lead_before_success_render_cleanup():
    query = FakeQuery("v3u:t:appointment_time:pm")
    user_data = {
        APPOINTMENT_SESSION_KEY: {
            "public_listing_id": PUBLIC_ID,
            "mode": "offline",
            "date": "09-10",
            "time": "",
            "source": "listing_callback",
        }
    }
    effects = FakeLeadEffects()

    outcome = await handle_v3_transition_action(
        _update(query),
        _context(user_data),
        actions=TransitionActionService(),
        views=ViewsStub(),
        appointment_executor=FakeAppointmentExecutor(),
        lead_effects=effects,
    )

    assert outcome.appointment_execution is not None
    assert outcome.lead_effect is not None and outcome.lead_effect.status == "recorded"
    assert len(effects.appointment_calls) == 1
    user, execution, draft = effects.appointment_calls[0]
    assert user.user_id == 123
    assert execution.submission.appointment_id == 51
    assert draft.public_listing_id == PUBLIC_ID
    assert APPOINTMENT_SESSION_KEY not in user_data
    assert [call[0] for call in query.calls] == ["answer", "edit_text"]
