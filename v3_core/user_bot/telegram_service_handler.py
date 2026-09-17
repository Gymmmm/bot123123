"""Telegram callback/text adapters for the V3 tenant-service flow."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode

from .lead_service import LeadUser
from .service_effects import ServiceEffectExecutor, ServiceEffectResult
from .service_flow import ServiceRequestDraft, TenantService
from .service_views import (
    ServiceChoice, ServiceView, general_prompt_view, general_success_view,
    issue_prompt_view, local_life_view, nearby_view, property_view,
    repair_home_view, repair_success_view, rfcity_category_view, rfcity_home_view,
    slot_view,
)
from .tenant_v1 import (
    deposit_view, guide_view, handover_view, lease_view, renew_view, submit_request,
    tenant_home_view, terminate_view,
)

SERVICE_REQUEST_SESSION_KEY = "v3_service_request"
SERVICE_GENERAL_WAIT_KEY = "v3_service_general_wait"
SERVICE_NEARBY_WAIT_KEY = "v3_service_nearby_wait"


@dataclass(frozen=True)
class TelegramServiceOutcome:
    handled: bool
    action: str = ""
    rendered: bool = False
    effect: ServiceEffectResult | None = None
    ticket_id: int | None = None


def _service_home_with_tenant_entry() -> ServiceView:
    return ServiceView(
        "service_entry",
        "🛠 <b>入住服务</b>\n\n系统会按当前账号的有效租约开放对应服务。",
        ((ServiceChoice("🛠 查看入住服务", "v3u:service:tenant"),),),
    )


def _tenant_binding_view(service: TenantService, user_id: int) -> ServiceView:
    return tenant_home_view(service, user_id)


def build_service_keyboard(view: ServiceView, *, advisor_url: str = "") -> InlineKeyboardMarkup | None:
    if not view.rows:
        return None
    rows: list[list[InlineKeyboardButton]] = []
    for row in view.rows:
        buttons: list[InlineKeyboardButton] = []
        for choice in row:
            label = "💬 中文顾问" if str(choice.label or "") in {"💬 联系我们", "💬 联系中文顾问", "💬 联系顾问"} else str(choice.label or "")
            buttons.append(InlineKeyboardButton(label, callback_data=choice.callback_data))
        rows.append(buttons)
    return InlineKeyboardMarkup(rows)


async def render_service_view(query: Any, view: ServiceView, *, advisor_url: str = "") -> None:
    markup = build_service_keyboard(view, advisor_url=advisor_url)
    message = getattr(query, "message", None)
    if getattr(message, "photo", None):
        await query.edit_message_caption(caption=view.text, parse_mode=ParseMode.HTML, reply_markup=markup)
        return
    await query.edit_message_text(view.text, parse_mode=ParseMode.HTML, reply_markup=markup)


async def _reply(message: Any, view: ServiceView, *, advisor_url: str = "") -> None:
    await message.reply_text(view.text, parse_mode=ParseMode.HTML, reply_markup=build_service_keyboard(view, advisor_url=advisor_url))


def _lead_user(update: Any) -> LeadUser:
    user = getattr(update, "effective_user", None)
    if user is None or getattr(user, "id", None) is None:
        raise ValueError("telegram_effective_user_missing_for_service")
    return LeadUser(
        user_id=int(user.id),
        username=str(getattr(user, "username", "") or ""),
        display_name=str(getattr(user, "full_name", "") or ""),
    )


def _draft_from_session(value: Any) -> ServiceRequestDraft | None:
    if not isinstance(value, dict):
        return None
    required = [str(value.get(key) or "").strip() for key in ("issue_key", "issue_label", "request_token")]
    if not all(required):
        return None
    return ServiceRequestDraft(
        issue_key=required[0],
        issue_label=required[1],
        detail=str(value.get("detail") or "").strip(),
        request_token=required[2],
    )


def _store_draft(user_data: dict[str, Any], draft: ServiceRequestDraft) -> None:
    user_data[SERVICE_REQUEST_SESSION_KEY] = {
        "issue_key": draft.issue_key,
        "issue_label": draft.issue_label,
        "detail": draft.detail,
        "request_token": draft.request_token,
    }


def _rfcity_category_product_view(category: str) -> ServiceView:
    view = rfcity_category_view(category)
    return ServiceView(view.kind, view.text, ((ServiceChoice("⬅️ 返回富力导航", "v3u:service:rfcity"),),))


def _requires_binding(action: str) -> bool:
    if action.startswith(("issue:", "slot:")):
        return True
    return action in {
        "repair", "property", "tenant_lease", "tenant_renew", "tenant_terminate",
        "tenant_renew_submit", "tenant_terminate_submit",
    }


def _updated_entry_view() -> ServiceView:
    return ServiceView(
        "historical_updated",
        "⚠️ 这个入口已经更新\n请使用下面的最新服务入口。",
        ((ServiceChoice("⬅️ 回首页", "v3u:t:home"), ServiceChoice("💬 中文顾问", "v3u:home:contact")),),
    )


async def handle_v3_service_callback(
    update: Any,
    context: Any,
    *,
    service: TenantService,
    effects: ServiceEffectExecutor | None = None,
    advisor_url: str = "",
) -> TelegramServiceOutcome:
    query = getattr(update, "callback_query", None)
    raw = str(getattr(query, "data", "") or "") if query is not None else ""
    prefix = "v3u:service:"
    if query is None or not raw.startswith(prefix):
        return TelegramServiceOutcome(False)
    action = raw[len(prefix):].strip()
    user_data = getattr(context, "user_data", None)
    if not isinstance(user_data, dict):
        raise ValueError("telegram_user_data_missing_for_service")
    user = _lead_user(update)
    await query.answer()

    # P0: do not trust a menu rendered earlier. Recheck the active binding at
    # the moment every lease/repair/property/renew/terminate action is clicked.
    if _requires_binding(action) and service.active_binding(user.user_id) is None:
        user_data.pop(SERVICE_REQUEST_SESSION_KEY, None)
        await render_service_view(query, tenant_home_view(service, user.user_id), advisor_url=advisor_url)
        return TelegramServiceOutcome(True, action, True)

    if action == "tenant":
        await render_service_view(query, tenant_home_view(service, user.user_id), advisor_url=advisor_url)
        return TelegramServiceOutcome(True, action, True)
    if action == "tenant_lease":
        await render_service_view(query, lease_view(service, user.user_id), advisor_url=advisor_url)
        return TelegramServiceOutcome(True, action, True)
    if action == "tenant_renew":
        await render_service_view(query, renew_view(service, user.user_id), advisor_url=advisor_url)
        return TelegramServiceOutcome(True, action, True)
    if action == "tenant_terminate":
        await render_service_view(query, terminate_view(service, user.user_id), advisor_url=advisor_url)
        return TelegramServiceOutcome(True, action, True)
    if action in {"tenant_renew_submit", "tenant_terminate_submit"}:
        kind = "renew" if action == "tenant_renew_submit" else "terminate"
        view, effect = await submit_request(kind=kind, service=service, effects=effects, update=update, context=context)
        await render_service_view(query, view, advisor_url=advisor_url)
        return TelegramServiceOutcome(True, action, True, effect=effect)

    # Deprecated guide/file callbacks never restore the old business flow.
    if action in {"tenant_guide", "tenant_handover", "tenant_deposit", "tenant_handover_pdf", "tenant_deposit_pdf"}:
        await render_service_view(query, _updated_entry_view(), advisor_url=advisor_url)
        return TelegramServiceOutcome(True, action, True)

    if action == "repair":
        await render_service_view(query, repair_home_view(), advisor_url=advisor_url)
        return TelegramServiceOutcome(True, action, True)
    if action == "property":
        await render_service_view(query, property_view(), advisor_url=advisor_url)
        return TelegramServiceOutcome(True, action, True)
    if action == "local":
        await render_service_view(query, local_life_view(), advisor_url=advisor_url)
        return TelegramServiceOutcome(True, action, True)
    if action == "general":
        await render_service_view(query, general_prompt_view(), advisor_url=advisor_url)
        user_data[SERVICE_GENERAL_WAIT_KEY] = True
        user_data.pop(SERVICE_NEARBY_WAIT_KEY, None)
        return TelegramServiceOutcome(True, action, True)
    if action == "nearby":
        await render_service_view(query, nearby_view(), advisor_url=advisor_url)
        return TelegramServiceOutcome(True, action, True)
    if action == "nearby_other":
        await render_service_view(query, general_prompt_view(nearby=True), advisor_url=advisor_url)
        user_data[SERVICE_NEARBY_WAIT_KEY] = True
        user_data.pop(SERVICE_GENERAL_WAIT_KEY, None)
        return TelegramServiceOutcome(True, action, True)
    if action == "rfcity":
        await render_service_view(query, rfcity_home_view(), advisor_url=advisor_url)
        return TelegramServiceOutcome(True, action, True)
    if action.startswith("rfcity:"):
        await render_service_view(query, _rfcity_category_product_view(action.split(":", 1)[1]), advisor_url=advisor_url)
        return TelegramServiceOutcome(True, action, True)
    if action.startswith("issue:"):
        draft = service.begin_request(action.split(":", 1)[1], request_token=uuid4().hex)
        await render_service_view(query, issue_prompt_view(draft), advisor_url=advisor_url)
        _store_draft(user_data, draft)
        user_data.pop(SERVICE_GENERAL_WAIT_KEY, None)
        user_data.pop(SERVICE_NEARBY_WAIT_KEY, None)
        return TelegramServiceOutcome(True, action, True)
    if action.startswith("slot:"):
        draft = _draft_from_session(user_data.get(SERVICE_REQUEST_SESSION_KEY))
        if draft is None or not draft.detail:
            return TelegramServiceOutcome(True, action, False)
        submission = service.submit_repair(user_id=user.user_id, draft=draft, slot=action.split(":", 1)[1])
        effect = await effects.repair(bot=getattr(context, "bot", None), user=user, submission=submission) if effects else None
        await render_service_view(query, repair_success_view(urgent=submission.urgent), advisor_url=advisor_url)
        user_data.pop(SERVICE_REQUEST_SESSION_KEY, None)
        return TelegramServiceOutcome(True, action, True, effect, submission.ticket.id)
    return TelegramServiceOutcome(False)


async def handle_v3_service_text(
    update: Any,
    context: Any,
    *,
    service: TenantService,
    effects: ServiceEffectExecutor | None = None,
    advisor_url: str = "",
) -> TelegramServiceOutcome:
    user_data = getattr(context, "user_data", None)
    message = getattr(update, "effective_message", None)
    if not isinstance(user_data, dict) or message is None:
        return TelegramServiceOutcome(False)
    text = str(getattr(message, "text", "") or "").strip()
    draft = _draft_from_session(user_data.get(SERVICE_REQUEST_SESSION_KEY))
    if draft is not None and not draft.detail:
        user = _lead_user(update)
        if service.active_binding(user.user_id) is None:
            user_data.pop(SERVICE_REQUEST_SESSION_KEY, None)
            await _reply(message, tenant_home_view(service, user.user_id), advisor_url=advisor_url)
            return TelegramServiceOutcome(True, "repair_denied", True)
        if len(text) < 4:
            await message.reply_text("请简单描述发生了什么，例如：空调可以启动，但一直不制冷。")
            return TelegramServiceOutcome(True, "repair_detail", True)
        updated = service.with_detail(draft, text)
        await _reply(message, slot_view(updated), advisor_url=advisor_url)
        _store_draft(user_data, updated)
        return TelegramServiceOutcome(True, "repair_detail", True)

    nearby = bool(user_data.get(SERVICE_NEARBY_WAIT_KEY))
    general = bool(user_data.get(SERVICE_GENERAL_WAIT_KEY))
    if not nearby and not general:
        return TelegramServiceOutcome(False)
    if len(text) < 2:
        await message.reply_text("请简单说一下需要什么帮助。")
        return TelegramServiceOutcome(True, "nearby_text" if nearby else "general_text", True)
    await _reply(message, general_success_view(nearby=nearby), advisor_url=advisor_url)
    effect = await effects.general(
        bot=getattr(context, "bot", None), user=_lead_user(update), details=text, nearby=nearby
    ) if effects else None
    user_data.pop(SERVICE_NEARBY_WAIT_KEY, None)
    user_data.pop(SERVICE_GENERAL_WAIT_KEY, None)
    return TelegramServiceOutcome(True, "nearby_text" if nearby else "general_text", True, effect)


__all__ = [
    "SERVICE_GENERAL_WAIT_KEY", "SERVICE_NEARBY_WAIT_KEY", "SERVICE_REQUEST_SESSION_KEY",
    "TelegramServiceOutcome", "build_service_keyboard", "handle_v3_service_callback",
    "handle_v3_service_text", "render_service_view", "_service_home_with_tenant_entry",
    "_tenant_binding_view",
]
