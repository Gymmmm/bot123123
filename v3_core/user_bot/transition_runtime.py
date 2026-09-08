"""Dependency wiring for V3 User Bot transition submit boundaries.

Construction is side-effect free: no SQLite schema initialization, Telegram
Application creation, handler registration, or legacy runtime imports occur here.
The returned runtime can later be injected into callback/text adapters.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .appointment_submit_executor import (
    AppointmentSubmitExecutor,
    build_sqlite_appointment_submit_executor,
)
from .public_inventory import PublicInventoryReader
from .search_submit_executor import (
    SearchSubmitExecutor,
    build_sqlite_search_submit_executor,
)
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
    appointments: AppointmentSubmitExecutor
    searches: SearchSubmitExecutor


def build_transition_runtime(db_path: str | Path) -> UserBotTransitionRuntime:
    path = Path(db_path).expanduser().resolve()
    inventory = PublicInventoryReader(path)
    return UserBotTransitionRuntime(
        db_path=path,
        inventory=inventory,
        views=TransitionViewService(inventory),
        callback_actions=TransitionActionService(),
        text_actions=TransitionTextActionService(),
        appointments=build_sqlite_appointment_submit_executor(path),
        searches=build_sqlite_search_submit_executor(path),
    )


__all__ = ["UserBotTransitionRuntime", "build_transition_runtime"]
