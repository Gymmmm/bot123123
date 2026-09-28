from __future__ import annotations

from v3_core.user_bot.appointment_history import AppointmentHistoryService
from v3_core.user_bot.appointment_submit_executor import AppointmentSubmitExecutor
from v3_core.user_bot.keyword_search_actions import KeywordSearchActionService
from v3_core.user_bot.lead_effects import LeadEffectExecutor
from v3_core.user_bot.lead_service import LeadService
from v3_core.user_bot.search_submit_executor import SearchSubmitExecutor
from v3_core.user_bot.service_flow import TenantService
from v3_core.user_bot.transition_actions import TransitionActionService
from v3_core.user_bot.transition_runtime import build_transition_runtime
from v3_core.user_bot.transition_text_actions import TransitionTextActionService
from v3_core.user_bot.transition_views import TransitionViewService


def test_transition_runtime_wires_submit_and_read_boundaries_without_creating_database(tmp_path):
    db = tmp_path / "missing.db"

    runtime = build_transition_runtime(db)

    assert runtime.db_path == db.resolve()
    assert isinstance(runtime.views, TransitionViewService)
    assert isinstance(runtime.callback_actions, TransitionActionService)
    assert isinstance(runtime.text_actions, TransitionTextActionService)
    assert isinstance(runtime.keyword_actions, KeywordSearchActionService)
    assert isinstance(runtime.appointments, AppointmentSubmitExecutor)
    assert isinstance(runtime.appointment_history, AppointmentHistoryService)
    assert isinstance(runtime.searches, SearchSubmitExecutor)
    assert isinstance(runtime.leads, LeadService)
    assert isinstance(runtime.lead_effects, LeadEffectExecutor)
    assert isinstance(runtime.tenant_service, TenantService)
    assert not db.exists()


def test_transition_runtime_shares_public_inventory_and_lead_service_instances(tmp_path):
    runtime = build_transition_runtime(tmp_path / "v3.db")

    assert runtime.views.inventory is runtime.inventory
    assert runtime.appointment_history.inventory is runtime.inventory
    assert runtime.lead_effects.service is runtime.leads
