"""Published-listing contact effects and user-facing handoff for V3."""
from __future__ import annotations

from dataclasses import dataclass
from html import escape as he
from typing import Any, Mapping

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


LISTING_QUESTION_SESSION_KEY = "v3_listing_question"


def remember_listing_question_context(
    user_data: dict[str, Any],
    intent: ConsultIntent,
) -> None:
    user_data[LISTING_QUESTION_SESSION_KEY] = {
        "listing_id": intent.listing_id,
        "public_listing_id": intent.public_listing_id,
        "source": intent.source,
        "inventory_status": intent.inventory_status,
        "offer_status": intent.offer_status,
        "publication_instance_id": intent.publication_instance_id,
        "touchpoint": intent.touchpoint,
    }


def clear_listing_question_context(user_data: dict[str, Any]) -> None:
    user_data.pop(LISTING_QUESTION_SESSION_KEY, None)


def listing_question_intent(session: Mapping[str, Any]) -> ConsultIntent | None:
    raw = session.get(LISTING_QUESTION_SESSION_KEY)
    if not isinstance(raw, Mapping):
        return None
    public_id = str(raw.get("public_listing_id") or "").strip()
    listing_id = str(raw.get("listing_id") or "").strip()
    if not public_id or not listing_id:
        return None
    return ConsultIntent(
        listing_id=listing_id,
        public_listing_id=public_id,
        source=str(raw.get("source") or "listing_callback").strip(),
        inventory_status=str(raw.get("inventory_status") or "").strip(),
        offer_status=str(raw.get("offer_status") or "").strip(),
        publication_instance_id=str(raw.get("publication_instance_id") or "").strip(),
        touchpoint=str(raw.get("touchpoint") or "").strip(),
    )


class ListingContactEffectExecutor:
    def __init__(
        self,
        *,
        leads: LeadEffectExecutor,
        admins: TelegramAdminNotifier,
        inventory: PublicInventoryReader | None = None,
    ):
        self.leads = leads
        self.admins = admins
        self.inventory = inventory

    def _admin_lines(self, user: LeadUser, intent: ConsultIntent) -> list[str]:
        if self.inventory is not None:
            clues = build_structured_advisor_clues(intent, self.inventory)
            if clues:
                customer = user_mention_html(user)
                lines = [line.replace("{customer}", customer) for line in clues]
                lines.append(f"💬 Telegram：{he(user_contact_text(user))}")
                return lines
        lines = [
            f"用户：{user_mention_html(user)}",
            f"联系方式：{he(user_contact_text(user))}",
            f"来源：{he(source_display_label(intent.source or 'listing_callback'))}",
        ]
        if str(intent.touchpoint or "").strip():
            lines.append(f"转化页：{he(source_display_label(intent.touchpoint))}")
        lines.append(f"咨询房源：{he(intent.public_listing_id)}")
        return lines

    async def execute(self, *, bot: Any, user: LeadUser, intent: ConsultIntent) -> ListingContactEffectResult:
        lead = self.leads.record_listing_contact(user=user, intent=intent)
        admin = await self.admins.send(
            bot,
            AdminNotification(
                title="用户咨询房源",
                lines=tuple(self._admin_lines(user, intent)),
            ),
        )
        return ListingContactEffectResult(lead=lead, admin=admin)

    def answer_project_question(self, *, intent: ConsultIntent, question: object) -> str | None:
        if self.inventory is None:
            return None
        view = self.inventory.resolve(intent.public_listing_id)
        if view is None:
            return None
        snapshot = dict(getattr(view, "snapshot", None) or {})
        canonical = dict(snapshot.get("canonical_facts") or {})
        project_key = canonical.get("project_key")
        if not project_key:
            return None
        from v3_core.inventory.project_knowledge import answer_project_question
        return answer_project_question(project_key, question)

    async def execute_question(
        self,
        *,
        bot: Any,
        user: LeadUser,
        intent: ConsultIntent,
        question: object,
    ) -> AdminNotificationResult:
        clean = str(question or "").strip()
        if not clean:
            raise ValueError("listing_question_required")
        lines = self._admin_lines(user, intent)
        lines.append(f"❓ 客户问题：{he(clean[:1200])}")
        return await self.admins.send(
            bot,
            AdminNotification(
                title="用户提问房源",
                lines=tuple(lines),
            ),
        )


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
        f"📍 来源：{he(source)}",
        f"🏠 房源：{he(details.subject or '待确认')}",
        f"💰 价格：{he(price)}",
        f"🗺️ 户型：{he(layout)}",
        f"📊 房态：{status_icon} {he(status_label)}",
        f"🔧 内部ID：{he(intent.listing_id)}",
        f"👤 客户：{{customer}}",
        f"📝 行为：用户咨询这套房源",
    ]
    
    if str(intent.touchpoint or "").strip():
        clues.append(f"🎯 转化页：{he(source_display_label(intent.touchpoint))}")
    
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
        "💬 <b>问这套</b>",
        "",
        he(identity),
        "",
        "房源信息已经带上。",
        "直接在这里发问题就可以，例如：最低多少？明天下午能看吗？",
    ]
    return ListingContactView(
        text="\n".join(lines),
        advisor_url=str(advisor_url or "").strip(),
        public_listing_id=intent.public_listing_id,
    )


__all__ = [
    "LISTING_QUESTION_SESSION_KEY",
    "ListingContactEffectExecutor",
    "ListingContactEffectResult",
    "ListingContactView",
    "build_listing_contact_view",
    "build_structured_advisor_clues",
    "clear_listing_question_context",
    "listing_question_intent",
    "remember_listing_question_context",
]