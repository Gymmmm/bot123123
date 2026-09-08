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
    def __init__(
        self,
        *,
        leads: LeadEffectExecutor,
        admins: TelegramAdminNotifier,
    ):
        self.leads = leads
        self.admins = admins

    async def execute(
        self,
        *,
        bot: Any,
        user: LeadUser,
        intent: ConsultIntent,
    ) -> ListingContactEffectResult:
        lead = self.leads.record_listing_contact(user=user, intent=intent)
        admin = await self.admins.send(
            bot,
            AdminNotification(
                title="用户联系我们",
                lines=(
                    f"用户：{user_mention_html(user)}",
                    f"联系方式：{he(user_contact_text(user))}",
                    f"入口：{he(intent.source or 'listing_callback')}",
                    f"咨询房源：{he(intent.public_listing_id)}",
                ),
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
    price_line = f"\n💰 <b>{he(price)}</b>" if price else ""
    return ListingContactView(
        text=(
            "💬 <b>已记录您咨询的房源</b>\n\n"
            f"🏠 <b>{he(subject)}</b>{price_line}\n"
            f"🆔 {he(intent.public_listing_id)}\n\n"
            "点击下方即可联系我们。\n"
            "这套房的信息已经带上，不用重新说明。"
        ),
        advisor_url=str(advisor_url or "").strip(),
        public_listing_id=intent.public_listing_id,
    )


__all__ = [
    "ListingContactEffectExecutor",
    "ListingContactEffectResult",
    "ListingContactView",
    "build_listing_contact_view",
]
