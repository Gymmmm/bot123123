"""Published-listing contact effects and user-facing handoff for V3."""
from __future__ import annotations

from dataclasses import dataclass
from html import escape as he
from typing import Any

from .admin_notification_plans import user_contact_text, user_mention_html
from .admin_notifications import AdminNotification, AdminNotificationResult, TelegramAdminNotifier
from .consult import ConsultIntent
from .lead_effects import LeadEffectExecutor, LeadEffectResult
from .lead_service import LeadUser
from .listing_presenter import build_public_listing_details
from .public_inventory import PublicInventoryReader
from .source_display import source_display_label


@dataclass(frozen=True)
class ListingContactEffectResult:
    lead: LeadEffectResult
    admin: AdminNotificationResult


@dataclass(frozen=True)
class ListingContactView:
    text: str
    advisor_url: str = ""
    public_listing_id: str = ""


class ListingContactEffectExecutor:
    def __init__(self, *, leads: LeadEffectExecutor, admins: TelegramAdminNotifier):
        self.leads = leads
        self.admins = admins

    async def execute(self, *, bot: Any, user: LeadUser, intent: ConsultIntent) -> ListingContactEffectResult:
        lead = self.leads.record_listing_contact(user=user, intent=intent)
        lines = [
            f"用户：{user_mention_html(user)}",
            f"联系方式：{he(user_contact_text(user))}",
            f"来源：{he(source_display_label(intent.source or 'listing_callback'))}",
        ]
        if str(intent.touchpoint or "").strip():
            lines.append(f"转化页：{he(source_display_label(intent.touchpoint))}")
        lines.append(f"咨询房源：{he(intent.public_listing_id)}")
        admin = await self.admins.send(
            bot,
            AdminNotification(
                title="用户咨询房源",
                lines=tuple(lines),
            ),
        )
        return ListingContactEffectResult(lead=lead, admin=admin)


def build_listing_contact_view(
    intent: ConsultIntent,
    inventory: PublicInventoryReader,
    *,
    advisor_url: str = "",
) -> ListingContactView:
    view = inventory.resolve(intent.public_listing_id)
    if view is None:
        raise ValueError("listing_contact_not_publicly_published")
    details = build_public_listing_details(view)
    subject = details.subject or details.location or "这套房"
    price = (
        f"${int(details.monthly_rent_usd):,}/月"
        if details.monthly_rent_usd is not None and int(details.monthly_rent_usd) > 0
        else ""
    )
    identity = "｜".join(part for part in (subject, price) if part)
    lines = [
        "<b>咨询这套</b>",
        "",
        he(identity),
        he(intent.public_listing_id),
        "",
        "这套房的信息已经带上。",
        "直接说想确认的事情即可。",
    ]
    return ListingContactView(
        text="\n".join(lines),
        advisor_url=str(advisor_url or "").strip(),
        public_listing_id=intent.public_listing_id,
    )


__all__ = [
    "ListingContactEffectExecutor",
    "ListingContactEffectResult",
    "ListingContactView",
    "build_listing_contact_view",
]
