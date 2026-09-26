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
from v3_core.status_labels import inventory_status_presentation


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


def build_structured_advisor_clues(
    intent: ConsultIntent,
    inventory: PublicInventoryReader,
) -> list[str]:
    """Build structured clues for the advisor notification.
    
    Includes:
    - 来源 (source)
    - 当前房源内部ID (internal listing_id - hidden from customer)
    - 价格户型房态 (price/layout/status)
    - 客户 (customer)
    - 预约和行为 (appointment and action)
    """
    view = inventory.resolve(intent.public_listing_id)
    if view is None:
        return []
    
    details = build_public_listing_details(view)
    source = source_display_label(intent.source or "listing_callback")
    inventory_status = str(intent.inventory_status or "").strip().lower()
    status_icon, status_label = inventory_status_presentation(inventory_status)
    
    price = (
        f"${int(details.monthly_rent_usd):,}/月"
        if details.monthly_rent_usd is not None and int(details.monthly_rent_usd) > 0
        else "价格待确认"
    )
    layout = str(details.layout or "").strip() or "户型待确认"
    
    clues = [
        f"📍 来源：{source}",
        f"🏠 房源：{details.subject or '待确认'}",
        f"💰 价格：{price}",
        f"🗺️ 户型：{layout}",
        f"📊 房态：{status_icon} {status_label}",
        f"🔧 内部ID：{intent.listing_id}",
        f"👤 客户：{{customer}}",
        f"📝 行为：用户咨询这套房源",
    ]
    
    if str(intent.touchpoint or "").strip():
        clues.append(f"🎯 转化页：{source_display_label(intent.touchpoint)}")
    
    return clues


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
        "💬 <b>咨询这套</b>",
        "",
        he(identity),
        "",
        "房源信息已经带上。",
        "直接说想确认的问题就可以。",
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
    "build_structured_advisor_clues",
]