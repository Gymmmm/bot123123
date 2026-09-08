from __future__ import annotations

from dataclasses import dataclass

from qiaolian_v3.legacy.callback_compat import LegacyCallback, resolve_legacy_callback

from . import start_screen
from .appointments import AppointmentFlow
from .deeplinks import DeepLinkTarget, parse_start_payload
from .listing_actions import action_policy
from .photos import PhotoService
from .repository import AppointmentRepository, UserListingRepository


@dataclass(frozen=True)
class UserRouteResult:
    route: str
    payload: object


class UserBotService:
    """Domain-facing V3 user experience; Telegram transport is outside this module."""

    def __init__(self, listings: UserListingRepository, appointments: AppointmentRepository) -> None:
        self.listings = listings
        self.appointments = appointments
        self.photos = PhotoService(listings)
        self.appointment_flow = AppointmentFlow(appointments)

    def start(self, payload: str = '') -> UserRouteResult:
        target = parse_start_payload(payload)
        if target is None:
            return UserRouteResult('home', start_screen())
        return self.route_target(target)

    def route_target(self, target: DeepLinkTarget) -> UserRouteResult:
        if target.legacy is not None:
            return UserRouteResult('legacy_compat', target.legacy)
        assert target.public_listing_id is not None
        listing = self.listings.get_by_public_id(target.public_listing_id)
        policy = action_policy(listing.listing_status)
        if target.action == 'details':
            return UserRouteResult('details', {'listing': listing, 'allowed': policy.details})
        if target.action == 'photos':
            return UserRouteResult('photos', {'listing': listing, 'allowed': policy.photos, 'gallery': self.photos.more_photos(target.public_listing_id)})
        if target.action == 'book':
            return UserRouteResult('book', {'listing': listing, 'allowed': policy.book})
        raise ValueError('unsupported_user_route')

    def legacy_callback(self, payload: str) -> LegacyCallback | None:
        return resolve_legacy_callback(payload)

    def my_appointments(self, user_id: int):
        return self.appointments.list_for_user(user_id)

    def lease_reminders(self, user_id: int, **kwargs):
        return self.appointments.lease_reminders(user_id, **kwargs)

    @staticmethod
    def service_routes() -> dict[str, str]:
        return {
            'contact_us': '联系我们',
            'guarantee': '侨联保障',
            'move_in_services': '入住服务',
            'lease': '租约',
            'repair': '报修',
            'property_coordination': '物业协调',
            'life_services': '生活服务',
            'nearby_needs': '周边需求',
            'local:rfcity': 'local:rfcity',
        }


__all__ = ['UserBotService', 'UserRouteResult']
