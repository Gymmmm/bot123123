from __future__ import annotations

from dataclasses import dataclass

from .repository import AppointmentRepository, AppointmentView


class AppointmentFlowBlocked(RuntimeError):
    pass


@dataclass(frozen=True)
class AppointmentDraft:
    user_id: int
    public_listing_id: str
    appointment_date: str | None = None
    appointment_time: str | None = None


class AppointmentFlow:
    def __init__(self, repository: AppointmentRepository) -> None:
        self.repository = repository

    def start(self, *, user_id: int, public_listing_id: str) -> AppointmentDraft:
        return AppointmentDraft(user_id=int(user_id), public_listing_id=str(public_listing_id))

    def select_date(self, draft: AppointmentDraft, appointment_date: str) -> AppointmentDraft:
        if not str(appointment_date).strip():
            raise AppointmentFlowBlocked('appointment_date_required')
        return AppointmentDraft(
            user_id=draft.user_id,
            public_listing_id=draft.public_listing_id,
            appointment_date=str(appointment_date),
            appointment_time=draft.appointment_time,
        )

    def select_time(self, draft: AppointmentDraft, appointment_time: str) -> AppointmentDraft:
        if not draft.appointment_date:
            raise AppointmentFlowBlocked('select_date_first')
        if not str(appointment_time).strip():
            raise AppointmentFlowBlocked('appointment_time_required')
        return AppointmentDraft(
            user_id=draft.user_id,
            public_listing_id=draft.public_listing_id,
            appointment_date=draft.appointment_date,
            appointment_time=str(appointment_time),
        )

    def submit(
        self,
        draft: AppointmentDraft,
        *,
        username: str = '',
        display_name: str = '',
        contact_value: str = '',
        note: str = '',
    ) -> AppointmentView:
        if not draft.appointment_date or not draft.appointment_time:
            raise AppointmentFlowBlocked('date_and_time_required_before_submit')
        return self.repository.create(
            user_id=draft.user_id,
            username=username,
            display_name=display_name,
            public_listing_id=draft.public_listing_id,
            appointment_date=draft.appointment_date,
            appointment_time=draft.appointment_time,
            contact_value=contact_value,
            note=note,
        )


__all__ = ['AppointmentDraft', 'AppointmentFlow', 'AppointmentFlowBlocked']
