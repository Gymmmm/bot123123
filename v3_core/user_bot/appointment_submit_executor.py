"""V3 appointment persistence executor.

This is the submit boundary between public Telegram/session state and durable
``appointments_v3`` persistence. It resolves the public listing identity,
re-validates the durable publication/bookability boundary, performs only the
appointment transaction, and returns an explicit follow-up side-effect plan.

It deliberately does not initialize schema, write leads, notify admins, edit
Telegram messages, or sync channel posts.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from v3_core.storage.appointment_repository import (
    SQLiteAppointmentRepository,
    V3ListingBookabilityChecker,
)

from .appointment_service import (
    AppointmentSubmissionResult,
    AppointmentSubmissionService,
    AppointmentUser,
)
from .appointment_side_effects import AppointmentEffectPlan, submission_effect_plan
from .public_appointment import PublicAppointmentDraft, PublicAppointmentResolver
from .public_inventory import PublicInventoryReader


@dataclass(frozen=True)
class AppointmentSubmitExecution:
    public_listing_id: str
    listing_id: str
    submission: AppointmentSubmissionResult
    effects: AppointmentEffectPlan


class AppointmentSubmitExecutor:
    def __init__(
        self,
        *,
        resolver: PublicAppointmentResolver,
        submissions: AppointmentSubmissionService,
    ):
        self.resolver = resolver
        self.submissions = submissions

    def execute(
        self,
        *,
        user: AppointmentUser,
        draft: PublicAppointmentDraft,
        edit_appointment_id: int = 0,
        created_at: str = "",
    ) -> AppointmentSubmitExecution:
        internal = self.resolver.resolve(draft)
        result = self.submissions.submit(
            user=user,
            draft=internal,
            edit_appointment_id=int(edit_appointment_id or 0),
            created_at=str(created_at or ""),
        )
        return AppointmentSubmitExecution(
            public_listing_id=draft.public_listing_id,
            listing_id=internal.listing_id,
            submission=result,
            effects=submission_effect_plan(result),
        )


def build_sqlite_appointment_submit_executor(
    db_path: str | Path,
) -> AppointmentSubmitExecutor:
    """Wire the V3 executor without creating or migrating any database tables."""
    repository = SQLiteAppointmentRepository(db_path)
    resolver = PublicAppointmentResolver(PublicInventoryReader(db_path))
    submissions = AppointmentSubmissionService(
        repository=repository,
        bookability=V3ListingBookabilityChecker(db_path),
    )
    return AppointmentSubmitExecutor(
        resolver=resolver,
        submissions=submissions,
    )


__all__ = [
    "AppointmentSubmitExecution",
    "AppointmentSubmitExecutor",
    "build_sqlite_appointment_submit_executor",
]
