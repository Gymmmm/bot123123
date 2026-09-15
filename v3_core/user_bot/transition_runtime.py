"""Dependency wiring for V3 User Bot transition and read-side boundaries.

Construction is side-effect free: no SQLite schema initialization, Telegram
Application creation, handler registration, or legacy runtime imports occur here.
The returned runtime can later be injected into callback/text/home adapters.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from v3_core.storage.service_repository import SQLiteTenantServiceRepository

from .appointment_history import (
    AppointmentHistoryService,
    SQLiteAppointmentHistoryReader,
)
from .appointment_submit_executor import (
    AppointmentSubmitExecutor,
    build_sqlite_appointment_submit_executor,
)
from .keyword_search_actions import KeywordSearchActionService
from .lead_effects import LeadEffectExecutor
from .lead_service import LeadService, build_sqlite_lead_service
from .public_inventory import PublicInventoryReader
from .search_submit_executor import (
    SearchSubmitExecutor,
    build_sqlite_search_submit_executor,
)
from .service_flow import TenantService
from .transition_actions import TransitionActionService
from .transition_text_actions import TransitionTextActionService
from .transition_views import TransitionViewService


@dataclass(frozen=True)
class UserBotTransitionRuntime:
    db_path: Path
    inventory: PublicInventoryReader
    views: TransitionViewService
    callback_actions: TransitionActionService
    text_actions: TransitionTextActionService
    keyword_actions: KeywordSearchActionService
    appointments: AppointmentSubmitExecutor
    appointment_history: AppointmentHistoryService
    searches: SearchSubmitExecutor
    leads: LeadService
    lead_effects: LeadEffectExecutor
    tenant_service: TenantService


def build_transition_runtime(db_path: str | Path) -> UserBotTransitionRuntime:
    path = Path(db_path).expanduser().resolve()
    inventory = PublicInventoryReader(path)
    leads = build_sqlite_lead_service(path)
    return UserBotTransitionRuntime(
        db_path=path,
        inventory=inventory,
        views=TransitionViewService(inventory),
        callback_actions=TransitionActionService(),
        text_actions=TransitionTextActionService(),
        keyword_actions=KeywordSearchActionService(),
        appointments=build_sqlite_appointment_submit_executor(path),
        appointment_history=AppointmentHistoryService(
            reader=SQLiteAppointmentHistoryReader(path),
            inventory=inventory,
        ),
        searches=build_sqlite_search_submit_executor(path),
        leads=leads,
        lead_effects=LeadEffectExecutor(leads),
        tenant_service=TenantService(SQLiteTenantServiceRepository(path)),
    )


__all__ = ["UserBotTransitionRuntime", "build_transition_runtime"]
