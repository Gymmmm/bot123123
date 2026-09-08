"""Best-effort lead/admin effects for V3 tenant-service submissions."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from html import escape as he
from typing import Any, Callable

from .admin_notification_plans import user_contact_text, user_mention_html
from .admin_notifications import AdminNotification, AdminNotificationResult, TelegramAdminNotifier
from .lead_effects import LeadEffectResult
from .lead_service import LeadRequest, LeadService, LeadUser
from .service_flow import RepairSubmission


@dataclass(frozen=True)
class ServiceEffectResult:
    lead: LeadEffectResult
    admin: AdminNotificationResult


class ServiceEffectExecutor:
    def __init__(
        self,
        *,
        leads: LeadService,
        admins: TelegramAdminNotifier,
        now: Callable[[], str] | None = None,
    ):
        self.leads = leads
        self.admins = admins
        self.now = now or (lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    def _record(self, *, user: LeadUser, request: LeadRequest) -> LeadEffectResult:
        try:
            record = self.leads.record(user=user, request=request, created_at=self.now())
        except Exception as exc:
            return LeadEffectResult(status="failed", error=str(exc))
        return LeadEffectResult(status="recorded", record=record)

    async def repair(
        self,
        *,
        bot: Any,
        user: LeadUser,
        submission: RepairSubmission,
    ) -> ServiceEffectResult:
        ticket = submission.ticket
        lead = self._record(
            user=user,
            request=LeadRequest(
                action="service_request_submit",
                source="service_hub",
                listing_id=ticket.property_name,
                payload={
                    "ticket_id": ticket.id,
                    "issue_key": ticket.issue_key,
                    "issue_label": ticket.issue_type,
                    "time_slot": ticket.time_slot,
                    "detail": ticket.description,
                    "binding_id": ticket.binding_id,
                },
            ),
        )
        admin = await self.admins.send(
            bot,
            AdminNotification(
                title="新报修请求",
                lines=(
                    f"客户：{user_mention_html(user)}",
                    f"联系方式：{he(user_contact_text(user))}",
                    f"房源：{he(ticket.property_name or '-')}",
                    f"问题：{he(ticket.issue_type)}",
                    f"说明：{he(ticket.description.split(chr(10), 1)[0])}",
                    f"希望时间：{he(submission.slot_label)}",
                ),
            ),
        )
        return ServiceEffectResult(lead=lead, admin=admin)

    async def general(
        self,
        *,
        bot: Any,
        user: LeadUser,
        details: str,
        nearby: bool = False,
    ) -> ServiceEffectResult:
        kind = "周边需求" if nearby else "通用服务咨询"
        action = "nearby_request" if nearby else "service_general"
        source = "nearby_service" if nearby else "service_hub"
        clean = str(details or "").strip()[:800]
        lead = self._record(
            user=user,
            request=LeadRequest(
                action=action,
                source=source,
                payload={"kind": kind, "details": clean},
            ),
        )
        admin = await self.admins.send(
            bot,
            AdminNotification(
                title=kind,
                lines=(
                    f"用户：{user_mention_html(user)}",
                    f"联系方式：{he(user_contact_text(user))}",
                    f"需求：<code>{he(clean[:700])}</code>",
                ),
            ),
        )
        return ServiceEffectResult(lead=lead, admin=admin)


__all__ = ["ServiceEffectExecutor", "ServiceEffectResult"]
