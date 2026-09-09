"""Telegram callback/text adapters for the current V3 tenant-service flow."""
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
    ServiceChoice,
    ServiceView,
    general_prompt_view,
    general_success_view,
    issue_prompt_view,
    local_life_view,
    nearby_view,
    property_view,
    repair_home_view,
    repair_success_view,
    rfcity_category_view,
    rfcity_home_view,
    slot_view,
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


def build_service_keyboard(view: ServiceView) -> InlineKeyboardMarkup | None:
    if not view.rows:
        return None
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(choice.label, callback_data=choice.callback_data) for choice in row]
            for row in view.rows
        ]
    )


async def render_service_view(query: Any, view: ServiceView) -> None:
    markup = build_service_keyboard(view)
    message = getattr(query, "message", None)
    if getattr(message, "photo", None):
        await query.edit_message_caption(
            caption=view.text,
            parse_mode=ParseMode.HTML,
            reply_markup=markup,
        )
        return
    await query.edit_message_text(
        view.text,
        parse_mode=ParseMode.HTML,
        reply_markup=markup,
    )


async def _reply(message: Any, view: ServiceView) -> None:
    await message.reply_text(
        view.text,
        parse_mode=ParseMode.HTML,
        reply_markup=build_service_keyboard(view),
    )


def _lead_user(update: Any) -> LeadUser:
    user = getattr(update, "effective_user", None)
    if user is None or getattr(user, "id", None) is None:
        raise ValueError("telegram_effective_user_missing_for_service")
    display_name = str(getattr(user, "full_name", "") or "").strip()
    if not display_name:
        display_name = " ".join(
            part
            for part in (
                str(getattr(user, "first_name", "") or "").strip(),
                str(getattr(user, "last_name", "") or "").strip(),
            )
            if part
        )
    return LeadUser(
        user_id=int(user.id),
        username=str(getattr(user, "username", "") or ""),
        display_name=display_name,
    )


def _draft_from_session(value: Any) -> ServiceRequestDraft | None:
    if not isinstance(value, dict):
        return None
    issue_key = str(value.get("issue_key") or "").strip()
    issue_label = str(value.get("issue_label") or "").strip()
    request_token = str(value.get("request_token") or "").strip()
    if not issue_key or not issue_label or not request_token:
        return None
    return ServiceRequestDraft(
        issue_key=issue_key,
        issue_label=issue_label,
        detail=str(value.get("detail") or "").strip(),
        request_token=request_token,
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
    return ServiceView(
        kind=view.kind,
        text=view.text,
        rows=((ServiceChoice("⬅️ 返回富力导航", "v3u:service:rfcity"),),),
    )


async def handle_v3_service_callback(
    update: Any,
    context: Any,
    *,
    service: TenantService,
    effects: ServiceEffectExecutor | None = None,
) -> TelegramServiceOutcome:
    query = getattr(update, "callback_query", None)
    raw = str(getattr(query, "data", "") or "") if query is not None else ""
    prefix = "v3u:service:"
    if query is None or not raw.startswith(prefix):
        return TelegramServiceOutcome(handled=False)
    action = raw[len(prefix):].strip()
    user_data = getattr(context, "user_data", None)
    if not isinstance(user_data, dict):
        raise ValueError("telegram_user_data_missing_for_service")
    await query.answer()

    if action == "repair":
        await render_service_view(query, repair_home_view())
        return TelegramServiceOutcome(True, action, True)
    if action == "property":
        await render_service_view(query, property_view())
        return TelegramServiceOutcome(True, action, True)
    if action == "local":
        await render_service_view(query, local_life_view())
        return TelegramServiceOutcome(True, action, True)
    if action == "general":
        await render_service_view(query, general_prompt_view())
        user_data[SERVICE_GENERAL_WAIT_KEY] = True
        user_data.pop(SERVICE_NEARBY_WAIT_KEY, None)
        return TelegramServiceOutcome(True, action, True)
    if action == "nearby":
        await render_service_view(query, nearby_view())
        return TelegramServiceOutcome(True, action, True)
    if action == "nearby_other":
        await render_service_view(query, general_prompt_view(nearby=True))
        user_data[SERVICE_NEARBY_WAIT_KEY] = True
        user_data.pop(SERVICE_GENERAL_WAIT_KEY, None)
        return TelegramServiceOutcome(True, action, True)
    if action == "rfcity":
        await render_service_view(query, rfcity_home_view())
        return TelegramServiceOutcome(True, action, True)
    if action.startswith("rfcity:"):
        category = action.split(":", 1)[1]
        await render_service_view(query, _rfcity_category_product_view(category))
        return TelegramServiceOutcome(True, action, True)
    if action.startswith("issue:"):
        issue_key = action.split(":", 1)[1]
        draft = service.begin_request(issue_key, request_token=uuid4().hex)
        await render_service_view(query, issue_prompt_view(draft))
        _store_draft(user_data, draft)
        user_data.pop(SERVICE_GENERAL_WAIT_KEY, None)
        user_data.pop(SERVICE_NEARBY_WAIT_KEY, None)
        return TelegramServiceOutcome(True, action, True)
    if action.startswith("slot:"):
        slot = action.split(":", 1)[1]
        draft = _draft_from_session(user_data.get(SERVICE_REQUEST_SESSION_KEY))
        if draft is None or not draft.detail:
            return TelegramServiceOutcome(True, action, False)
        user = _lead_user(update)
        submission = service.submit_repair(user_id=user.user_id, draft=draft, slot=slot)
        effect = None
        if effects is not None:
            effect = await effects.repair(
                bot=getattr(context, "bot", None),
                user=user,
                submission=submission,
            )
        await render_service_view(query, repair_success_view(urgent=submission.urgent))
        user_data.pop(SERVICE_REQUEST_SESSION_KEY, None)
        return TelegramServiceOutcome(
            True,
            action,
            True,
            effect=effect,
            ticket_id=submission.ticket.id,
        )
    return TelegramServiceOutcome(handled=False)


async def handle_v3_service_text(
    update: Any,
    context: Any,
    *,
    service: TenantService,
    effects: ServiceEffectExecutor | None = None,
) -> TelegramServiceOutcome:
    user_data = getattr(context, "user_data", None)
    message = getattr(update, "effective_message", None)
    if not isinstance(user_data, dict) or message is None:
        return TelegramServiceOutcome(handled=False)
    text = str(getattr(message, "text", "") or "").strip()

    draft = _draft_from_session(user_data.get(SERVICE_REQUEST_SESSION_KEY))
    if draft is not None and not draft.detail:
        if len(text) < 4:
            await message.reply_text("请简单描述发生了什么，例如：B栋3楼走廊灯坏了。")
            return TelegramServiceOutcome(True, "repair_detail", True)
        updated = service.with_detail(draft, text)
        await _reply(message, slot_view(updated))
        _store_draft(user_data, updated)
        return TelegramServiceOutcome(True, "repair_detail", True)

    nearby = bool(user_data.get(SERVICE_NEARBY_WAIT_KEY))
    general = bool(user_data.get(SERVICE_GENERAL_WAIT_KEY))
    if not nearby and not general:
        return TelegramServiceOutcome(handled=False)
    if len(text) < 2:
        await message.reply_text("请简单说一下需要什么帮助。")
        return TelegramServiceOutcome(True, "nearby_text" if nearby else "general_text", True)

    await _reply(message, general_success_view(nearby=nearby))
    effect = None
    if effects is not None:
        effect = await effects.general(
            bot=getattr(context, "bot", None),
            user=_lead_user(update),
            details=text,
            nearby=nearby,
        )
    user_data.pop(SERVICE_NEARBY_WAIT_KEY, None)
    user_data.pop(SERVICE_GENERAL_WAIT_KEY, None)
    return TelegramServiceOutcome(
        True,
        "nearby_text" if nearby else "general_text",
        True,
        effect=effect,
    )


__all__ = [
    "SERVICE_GENERAL_WAIT_KEY",
    "SERVICE_NEARBY_WAIT_KEY",
    "SERVICE_REQUEST_SESSION_KEY",
    "TelegramServiceOutcome",
    "build_service_keyboard",
    "handle_v3_service_callback",
    "handle_v3_service_text",
    "render_service_view",
]
