from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

from .appointments import AppointmentDraft
from .deeplinks import require_ql_identity
from .service import UserBotService, UserRouteResult


@dataclass(frozen=True)
class UserUpdate:
    user_id: int
    callback_data: str = ''
    start_payload: str = ''
    data: Mapping[str, Any] = field(default_factory=dict)


class UserBotRouter:
    """Transport-neutral V3 User Bot router with fake-update friendly state."""

    def __init__(self, service: UserBotService) -> None:
        self.service = service
        self._appointment_drafts: dict[int, AppointmentDraft] = {}

    def start(self, *, user_id: int, payload: str = '') -> UserRouteResult:
        result = self.service.start(payload)
        if result.route == 'book' and isinstance(result.payload, dict) and result.payload.get('allowed'):
            listing = result.payload['listing']
            draft = self.service.appointment_flow.start(
                user_id=int(user_id), public_listing_id=listing.public_listing_id,
            )
            self._appointment_drafts[int(user_id)] = draft
            return UserRouteResult('appointment_date', {'listing': listing, 'draft': draft})
        return result

    def dispatch(self, *, user_id: int, callback_data: str, **data: Any) -> UserRouteResult:
        callback = str(callback_data or '').strip()
        uid = int(user_id)
        if callback in {'', 'home', '/start'}:
            return self.service.start()
        if callback == 'find_home':
            return UserRouteResult('find_home', self.service.listings.search(query=str(data.get('query', ''))))
        if callback == 'my_appointments':
            return UserRouteResult('my_appointments', self.service.my_appointments(uid))

        for action in ('details', 'photos', 'book'):
            prefix = action + ':'
            if callback.startswith(prefix):
                ql = require_ql_identity(callback[len(prefix):])
                target = self.service.start(f'{action}_{ql}')
                if action == 'book':
                    if not target.payload['allowed']:
                        return target
                    draft = self.service.appointment_flow.start(user_id=uid, public_listing_id=ql)
                    self._appointment_drafts[uid] = draft
                    return UserRouteResult('appointment_date', {'listing': target.payload['listing'], 'draft': draft})
                return target

        if callback.startswith('appointment_date:'):
            draft = self._appointment_drafts.get(uid)
            if draft is None:
                raise ValueError('appointment_flow_not_started')
            draft = self.service.appointment_flow.select_date(draft, callback.split(':', 1)[1])
            self._appointment_drafts[uid] = draft
            return UserRouteResult('appointment_time', draft)

        if callback.startswith('appointment_time:'):
            draft = self._appointment_drafts.get(uid)
            if draft is None:
                raise ValueError('appointment_flow_not_started')
            draft = self.service.appointment_flow.select_time(draft, callback.split(':', 1)[1])
            self._appointment_drafts[uid] = draft
            return UserRouteResult('appointment_submit', draft)

        if callback == 'appointment_submit':
            draft = self._appointment_drafts.get(uid)
            if draft is None:
                raise ValueError('appointment_flow_not_started')
            appointment = self.service.appointment_flow.submit(
                draft,
                username=str(data.get('username', '')),
                display_name=str(data.get('display_name', '')),
                contact_value=str(data.get('contact_value', '')),
                note=str(data.get('note', '')),
            )
            self._appointment_drafts.pop(uid, None)
            return UserRouteResult('appointment_confirmed', appointment)

        services = self.service.service_routes()
        if callback in services:
            return UserRouteResult(callback, {'label': services[callback]})
        if callback == 'channel':
            return UserRouteResult('channel', {'action': 'open_channel'})
        raise ValueError('unknown_user_route')


class UserBotHandler:
    def __init__(self, router: UserBotRouter) -> None:
        self.router = router

    def handle(self, update: UserUpdate) -> UserRouteResult:
        if update.start_payload:
            return self.router.start(user_id=update.user_id, payload=update.start_payload)
        return self.router.dispatch(
            user_id=update.user_id,
            callback_data=update.callback_data,
            **dict(update.data),
        )


__all__ = ['UserBotHandler', 'UserBotRouter', 'UserUpdate']
