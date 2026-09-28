"""Dependency wiring for the production V3 User Bot takeover."""

from __future__ import annotations

from pathlib import Path

from v3_core.storage.appointment_repository import V3ListingBookabilityChecker

from .appointment_history import AppointmentHistoryService
from .appointment_service import AppointmentSubmissionService
from .appointment_submit_executor import AppointmentSubmitExecutor
from .keyword_search_actions import KeywordSearchActionService
from .lead_effects import LeadEffectExecutor
from .lead_service import LeadService
from .public_appointment import PublicAppointmentResolver
from .public_inventory import PublicInventoryReader
from .search_submit_executor import build_sqlite_search_submit_executor
from .service_flow import TenantService
from .takeover_storage import (
    ProductionAppointmentHistoryReader,
    ProductionAppointmentRepository,
    ProductionLeadRepository,
    ProductionTenantServiceRepository,
)
from .transition_actions import TransitionActionService
from .transition_runtime import UserBotTransitionRuntime
from .transition_text_actions import TransitionTextActionService
from .transition_views import TransitionViewService


def build_takeover_transition_runtime(db_path: str | Path) -> UserBotTransitionRuntime:
    """Build V3 UX over live 3858 operational persistence boundaries."""
    path = Path(db_path).expanduser().resolve()
    inventory = PublicInventoryReader(path)
    appointment_repo = ProductionAppointmentRepository(path)
    leads = LeadService(ProductionLeadRepository(path))
    appointments = AppointmentSubmitExecutor(
        resolver=PublicAppointmentResolver(inventory),
        submissions=AppointmentSubmissionService(
            repository=appointment_repo,
            bookability=V3ListingBookabilityChecker(path),
        ),
    )
    return UserBotTransitionRuntime(
        db_path=path,
        inventory=inventory,
        views=TransitionViewService(inventory),
        callback_actions=TransitionActionService(),
        text_actions=TransitionTextActionService(),
        keyword_actions=KeywordSearchActionService(),
        appointments=appointments,
        appointment_history=AppointmentHistoryService(
            reader=ProductionAppointmentHistoryReader(path),
            inventory=inventory,
        ),
        searches=build_sqlite_search_submit_executor(path),
        leads=leads,
        lead_effects=LeadEffectExecutor(leads),
        tenant_service=TenantService(ProductionTenantServiceRepository(path)),
    )


__all__ = ["build_takeover_transition_runtime"]
