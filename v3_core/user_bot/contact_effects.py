"""General contact effect orchestration for the V3 User Bot.

The locked production flow records a lead, sends a best-effort admin notice, then
renders the user handoff. This module performs only the first two effects and
returns their outcomes; user-facing Telegram rendering stays in the adapter.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .admin_notification_plans import general_contact_notification
from .admin_notifications import AdminNotificationResult, TelegramAdminNotifier
from .lead_effects import LeadEffectExecutor, LeadEffectResult
from .lead_service import LeadUser


@dataclass(frozen=True)
class ContactEffectResult:
    lead: LeadEffectResult
    admin: AdminNotificationResult


class ContactEffectExecutor:
    def __init__(
        self,
        *,
        leads: LeadEffectExecutor,
        admins: TelegramAdminNotifier,
    ):
        self.leads = leads
        self.admins = admins

    async def execute_general(
        self,
        *,
        bot: Any,
        user: LeadUser,
        source: str = "hub",
    ) -> ContactEffectResult:
        lead = self.leads.record_general_contact(user=user, source=source)
        admin = await self.admins.send(
            bot,
            general_contact_notification(user, source=source),
        )
        return ContactEffectResult(lead=lead, admin=admin)


__all__ = ["ContactEffectExecutor", "ContactEffectResult"]
