from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from typing import Any, Mapping

from qiaolian_v3.db.repositories.reviews import ReviewRepository

from . import ADMIN_ROUTES, home_screen
from .broadcast import BroadcastService
from .collector_management import CollectorManagementService
from .listing_management import ListingManagementService
from .manual_intake import AdminManualIntakeController
from .publication_history import PublicationHistoryService
from .review import AdminReviewService
from .settings import AdminSettingsService


@dataclass(frozen=True)
class AdminUpdate:
    callback_data: str
    data: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class AdminRouteResult:
    route: str
    payload: Any


class AdminBotRouter:
    """Transport-neutral Admin Bot handler layer; no polling or Telegram writes."""

    def __init__(self, conn: sqlite3.Connection, *, gateway: Any, dry_run: bool = True) -> None:
        self.conn = conn
        self.gateway = gateway
        self.dry_run = bool(dry_run)
        self.manual_intake = AdminManualIntakeController(conn)
        self.listings = ListingManagementService(conn)
        self.reviews = AdminReviewService(ReviewRepository(conn))
        self.collector = CollectorManagementService(None)
        self.broadcast = BroadcastService(gateway=gateway, dry_run=self.dry_run)
        self.history = PublicationHistoryService(conn)
        self.settings = AdminSettingsService()

    def home(self) -> AdminRouteResult:
        return AdminRouteResult('home', home_screen())

    def dispatch(self, callback_data: str, **data: Any) -> AdminRouteResult:
        route = str(callback_data or '').strip()
        if route in {'', 'home'}:
            return self.home()
        if route not in ADMIN_ROUTES:
            raise ValueError('unknown_admin_route')

        action = str(data.pop('action', '') or '')
        if route == 'manual_intake':
            if action == 'preview':
                return AdminRouteResult(route, {
                    'action': 'preview',
                    'preview': self.manual_intake.preview(**data),
                })
            if action == 'confirm':
                return AdminRouteResult(route, {
                    'action': 'confirm',
                    'result': self.manual_intake.confirm(gateway=self.gateway, dry_run=self.dry_run, **data),
                })
            return AdminRouteResult(route, {'action': 'page'})

        if route == 'listing_management':
            return AdminRouteResult(route, self.listings.list_recent(limit=int(data.get('limit', 50))))
        if route == 'review':
            return AdminRouteResult(route, self.reviews.open_items())
        if route == 'collector_management':
            return AdminRouteResult(route, self.collector.describe())
        if route == 'broadcast':
            if action == 'preview':
                return AdminRouteResult(route, {'action': 'preview', 'preview': self.broadcast.preview(**data)})
            if action == 'confirm':
                preview = data.pop('preview')
                return AdminRouteResult(route, {
                    'action': 'send',
                    'result': self.broadcast.send(preview, confirm=True),
                })
            return AdminRouteResult(route, {'action': 'page'})
        if route == 'publication_history':
            return AdminRouteResult(route, self.history.recent(limit=int(data.get('limit', 50))))
        if route == 'settings':
            return AdminRouteResult(route, self.settings.get_all())
        raise AssertionError('unreachable_admin_route')


class AdminBotHandler:
    def __init__(self, router: AdminBotRouter) -> None:
        self.router = router

    def handle(self, update: AdminUpdate) -> AdminRouteResult:
        return self.router.dispatch(update.callback_data, **dict(update.data))


__all__ = ['AdminBotHandler', 'AdminBotRouter', 'AdminRouteResult', 'AdminUpdate']
