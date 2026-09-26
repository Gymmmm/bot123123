"""Telegram callback/text/media adapters for the V3 tenant-service flow."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode

from .consult import build_consultation_envelope, consultation_handoff_url
from .lead_service import LeadUser
from .service_effects import ServiceEffectExecutor, ServiceEffectResult
from .service_flow import SERVICE_SLOT_LABELS, ServiceRequestDraft, TenantService
from .service_views import (
    ServiceChoice, ServiceView, concierge_home_view, general_prompt_view, general_success_view,
    issue_prompt_view, local_life_view, nearby_view, property_view, property_description_view,
    property_time_view, property_contacted_view, property_confirm_view, property_result_view,
    property_exit_view, repair_home_view, repair_media_view, repair_confirm_view,
    repair_result_view, repair_exit_view, rfcity_category_view, rfcity_home_view,
    slot_view, utility_stub_view,
)
from .telegram_edit import edit_query_panel
from .tenant_v1 import (
    deposit_view, guide_view, handover_view, lease_view, missing_lease_view, renew_view,
    submit_request, tenant_home_view, terminate_view,
)

SERVICE_REQUEST_SESSION_KEY = "v3_service_request"
SERVICE_REQUEST_ANCHOR_KEY = "v3_service_request_anchor"
PROPERTY_SESSION_KEY = "v3_property_request"
PROPERTY_ANCHOR_KEY = "v3_property_request_anchor"
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
    from .service_product_views import service_home_view
    return service_home_view()


def _tenant_binding_view(service: TenantService, user_id: int) -> ServiceView:
    return tenant_home_view(service, user_id)


def _runtime_service_callback(callback_data: str) -> str:
    raw = str(callback_data or "")
    prefix = "v3u:service:property"
    if raw == prefix:
        return "v3u:service:coordination"
    if raw.startswith(prefix + "_"):
        return "v3u:service:coordination_" + raw[len(prefix) + 1:]
    return raw


def build_service_keyboard(view: ServiceView, *, advisor_url: str = "") -> InlineKeyboardMarkup | None:
    if not view.rows:
        return None
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                str(choice.label or ""),
                callback_data=_runtime_service_callback(choice.callback_data),
            )
            for choice in row
        ]
        for row in view.rows
    ])


async def render_service_view(query: Any, view: ServiceView, *, advisor_url: str = "") -> None:
    markup = build_service_keyboard(view, advisor_url=advisor_url)
    await edit_query_panel(
        query,
        text=view.text,
        parse_mode=ParseMode.HTML,
        reply_markup=markup,
    )


def _with_context_contact(
    view: ServiceView,
    *,
    callback_data: str,
    extra_line: str = "",
) -> ServiceView:
    rows = tuple(
        tuple(
            ServiceChoice(choice.label, callback_data)
            if choice.label == "中文顾问"
            else choice
            for choice in row
        )
        for row in view.rows
    )
    text = view.text
    if extra_line:
        text = f"{text}\n\n{extra_line}"
    return ServiceView(view.kind, text, rows)


async def _render_consultation_handoff(
    query: Any,
    *,
    envelope,
    advisor_url: str,
) -> None:
    direct = consultation_handoff_url(advisor_url, envelope)
    rows = []
    if direct:
        rows.append([InlineKeyboardButton("中文顾问", url=direct)])
    rows.append([InlineKeyboardButton("返回侨联服务", callback_data="v3u:home:service")])
    await query.edit_message_text(
        "<b>中文顾问</b>\n\n直接把问题发给我。",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(rows),
    )


def _chat_id(update: Any) -> int:
    chat = getattr(update, "effective_chat", None)
    value = getattr(chat, "id", None)
    if value is None:
        query = getattr(update, "callback_query", None)
        value = getattr(getattr(query, "message", None), "chat_id", None)
    if value is None:
        raise ValueError("telegram_effective_chat_missing_for_service")
    return int(value)


async def _send_view(update: Any, context: Any, view: ServiceView, *, anchor_key: str | None = None, advisor_url: str = "") -> Any:
    query = getattr(update, "callback_query", None)
    source_message = getattr(query, "message", None) if query is not None else None
    sent = None
    if query is not None and source_message is not None:
        await render_service_view(query, view, advisor_url=advisor_url)
        sent = source_message
    else:
        sent = await context.bot.send_message(
            chat_id=_chat_id(update),
            text=view.text,
            parse_mode=ParseMode.HTML,
            reply_markup=build_service_keyboard(view, advisor_url=advisor_url),
        )
    user_data = getattr(context, "user_data", None)
    if anchor_key and isinstance(user_data, dict):
        chat_id = getattr(sent, "chat_id", None)
        message_id = getattr(sent, "message_id", None)
        if chat_id is not None and message_id is not None:
            user_data[anchor_key] = {"chat_id": int(chat_id), "message_id": int(message_id)}
        elif source_message is not None:
            source_chat_id = getattr(source_message, "chat_id", None)
            source_message_id = getattr(source_message, "message_id", None)
            if source_chat_id is not None and source_message_id is not None:
                user_data[anchor_key] = {"chat_id": int(source_chat_id), "message_id": int(source_message_id)}
    return sent


async def _edit_anchor(
    context: Any,
    user_data: dict[str, Any],
    anchor_key: str,
    view: ServiceView,
    *,
    advisor_url: str = "",
    fallback_message: Any | None = None,
) -> None:
    anchor = user_data.get(anchor_key)
    if isinstance(anchor, dict):
        await context.bot.edit_message_text(
            chat_id=int(anchor["chat_id"]),
            message_id=int(anchor["message_id"]),
            text=view.text,
            parse_mode=ParseMode.HTML,
            reply_markup=build_service_keyboard(view, advisor_url=advisor_url),
        )
        return
    reply = getattr(fallback_message, "reply_text", None)
    if callable(reply):
        await reply(
            view.text,
            parse_mode=ParseMode.HTML,
            reply_markup=build_service_keyboard(view, advisor_url=advisor_url),
        )
        return
    raise ValueError("service_transaction_anchor_missing")


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


def _repair_state(user_data: dict[str, Any]) -> dict[str, Any] | None:
    value = user_data.get(SERVICE_REQUEST_SESSION_KEY)
    return value if isinstance(value, dict) else None


def _store_draft(user_data: dict[str, Any], draft: ServiceRequestDraft, **extra: Any) -> None:
    existing = _repair_state(user_data) or {}
    state = {
        "issue_key": draft.issue_key,
        "issue_label": draft.issue_label,
        "detail": draft.detail,
        "request_token": draft.request_token,
        "media_file_ids": list(existing.get("media_file_ids") or ()),
        "slot": str(existing.get("slot") or ""),
        "stage": str(existing.get("stage") or ""),
    }
    state.update(extra)
    user_data[SERVICE_REQUEST_SESSION_KEY] = state


def _rfcity_category_product_view(category: str) -> ServiceView:
    view = rfcity_category_view(category)
    return ServiceView(view.kind, view.text, ((ServiceChoice("⬅️ 返回富力城导航", "v3u:service:rfcity"),),))


def _requires_binding(action: str) -> bool:
    return action in {"tenant_lease", "tenant_renew", "tenant_terminate", "tenant_renew_submit", "tenant_terminate_submit"}


def _updated_entry_view() -> ServiceView:
    return ServiceView(
        "historical_updated",
        "<b>这个入口已经更新</b>\n\n请使用下面的最新服务入口。",
        ((ServiceChoice("⬅️ 返回首页", "v3u:t:home"), ServiceChoice("💬 中文顾问", "v3u:home:contact")),),
    )


def _effect_success(effect: ServiceEffectResult | None) -> bool:
    if effect is None:
        return False
    if getattr(effect.lead, "status", "") == "recorded":
        return True
    return bool(getattr(effect.admin, "sent_admin_ids", ()))


def _property_category(action: str) -> tuple[str, str] | None:
    mapping = {
        "access": "门禁门卡", "parking": "停车问题", "noise": "噪音问题",
        "common": "公共区域", "fees": "物业费用", "facilities": "公共设施", "other": "其他问题",
    }
    key = action.split(":", 1)[1] if ":" in action else ""
    label = mapping.get(key)
    return (key, label) if label else None


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
    # Accept pre-lock callbacks already present in old Telegram messages, while
    # every newly rendered coordination button uses the coordination_* family.
    if action == "property":
        action = "coordination"
    elif action.startswith("property_"):
        action = "coordination_" + action[len("property_"):]
    user_data = getattr(context, "user_data", None)
    if not isinstance(user_data, dict):
        raise ValueError("telegram_user_data_missing_for_service")
    user = _lead_user(update)
    await query.answer()

    if _requires_binding(action) and service.active_binding(user.user_id) is None:
        await _send_view(update, context, missing_lease_view(), advisor_url=advisor_url)
        return TelegramServiceOutcome(True, action, True)

    if action.startswith("consult_ticket:") or action.startswith("consult_lease:"):
        kind = "ticket" if action.startswith("consult_ticket:") else "lease"
        reference_id = action.split(":", 1)[1]
        property_name = ""
        if kind == "lease":
            binding = service.active_binding(user.user_id)
            if binding is not None and str(getattr(binding, "id", "")) == reference_id:
                property_name = str(getattr(binding, "property_name", "") or "")
        envelope = build_consultation_envelope(
            kind,
            reference_id,
            property_name=property_name,
        )
        effect = (
            await effects.general(
                bot=getattr(context, "bot", None),
                user=user,
                details=envelope.admin_details(),
            )
            if effects is not None
            else None
        )
        await _render_consultation_handoff(
            query,
            envelope=envelope,
            advisor_url=advisor_url,
        )
        return TelegramServiceOutcome(True, action, True, effect=effect)

    if action == "tenant":
        from .service_product_views import service_home_view
        await render_service_view(query, service_home_view(), advisor_url=advisor_url)
        return TelegramServiceOutcome(True, action, True)
    if action == "tenant_lease":
        binding = service.require_active_binding(user.user_id)
        view = _with_context_contact(
            lease_view(service, user.user_id),
            callback_data=f"v3u:service:consult_lease:{int(binding.id)}",
        )
        await _send_view(update, context, view, advisor_url=advisor_url)
        return TelegramServiceOutcome(True, action, True)
    if action == "tenant_renew":
        await _send_view(update, context, renew_view(service, user.user_id), advisor_url=advisor_url)
        return TelegramServiceOutcome(True, action, True)
    if action == "tenant_terminate":
        await _send_view(update, context, terminate_view(service, user.user_id), advisor_url=advisor_url)
        return TelegramServiceOutcome(True, action, True)
    if action in {"tenant_renew_submit", "tenant_terminate_submit"}:
        kind = "renew" if action == "tenant_renew_submit" else "terminate"
        view, effect = await submit_request(kind=kind, service=service, effects=effects, update=update, context=context)
        await render_service_view(query, view, advisor_url=advisor_url)
        return TelegramServiceOutcome(True, action, True, effect=effect)

    if action in {"tenant_guide", "tenant_handover", "tenant_deposit", "tenant_handover_pdf", "tenant_deposit_pdf"}:
        await render_service_view(query, _updated_entry_view(), advisor_url=advisor_url)
        return TelegramServiceOutcome(True, action, True)

    if action == "concierge":
        await _send_view(update, context, concierge_home_view(), advisor_url=advisor_url)
        return TelegramServiceOutcome(True, action, True)

    # Repair transaction.
    if action == "repair":
        user_data.pop(SERVICE_REQUEST_SESSION_KEY, None)
        await _send_view(update, context, repair_home_view(), anchor_key=SERVICE_REQUEST_ANCHOR_KEY, advisor_url=advisor_url)
        return TelegramServiceOutcome(True, action, True)
    if action.startswith("issue:"):
        draft = service.begin_request(action.split(":", 1)[1], request_token=uuid4().hex)
        await render_service_view(query, issue_prompt_view(draft), advisor_url=advisor_url)
        _store_draft(user_data, draft, stage="description", media_file_ids=[], slot="")
        return TelegramServiceOutcome(True, action, True)
    if action == "repair_modify_desc":
        draft = _draft_from_session(user_data.get(SERVICE_REQUEST_SESSION_KEY))
        if draft is None:
            return TelegramServiceOutcome(True, action, False)
        cleared = ServiceRequestDraft(draft.issue_key, draft.issue_label, "", draft.request_token)
        _store_draft(user_data, cleared, stage="description")
        await render_service_view(query, issue_prompt_view(cleared), advisor_url=advisor_url)
        return TelegramServiceOutcome(True, action, True)
    if action in {"repair_media_next", "repair_media_skip"}:
        draft = _draft_from_session(user_data.get(SERVICE_REQUEST_SESSION_KEY))
        if draft is None or not draft.detail:
            return TelegramServiceOutcome(True, action, False)
        _store_draft(user_data, draft, stage="time")
        await render_service_view(query, slot_view(draft), advisor_url=advisor_url)
        return TelegramServiceOutcome(True, action, True)
    if action == "repair_back_media":
        draft = _draft_from_session(user_data.get(SERVICE_REQUEST_SESSION_KEY))
        state = _repair_state(user_data)
        if draft is None or state is None:
            return TelegramServiceOutcome(True, action, False)
        _store_draft(user_data, draft, stage="media")
        await render_service_view(query, repair_media_view(draft, len(state.get("media_file_ids") or ())), advisor_url=advisor_url)
        return TelegramServiceOutcome(True, action, True)
    if action == "repair_modify_time":
        draft = _draft_from_session(user_data.get(SERVICE_REQUEST_SESSION_KEY))
        if draft is None:
            return TelegramServiceOutcome(True, action, False)
        _store_draft(user_data, draft, stage="time", slot="")
        await render_service_view(query, slot_view(draft), advisor_url=advisor_url)
        return TelegramServiceOutcome(True, action, True)
    if action.startswith("slot:"):
        draft = _draft_from_session(user_data.get(SERVICE_REQUEST_SESSION_KEY))
        state = _repair_state(user_data)
        if draft is None or state is None or not draft.detail:
            return TelegramServiceOutcome(True, action, False)
        slot = action.split(":", 1)[1]
        slot_label = SERVICE_SLOT_LABELS.get(slot)
        if slot_label is None:
            raise ValueError("unsupported_service_slot")
        _store_draft(user_data, draft, stage="confirm", slot=slot)
        binding = service.active_binding(user.user_id)
        prop = str(getattr(binding, "property_name", "") or "") if binding is not None else ""
        await render_service_view(
            query,
            repair_confirm_view(draft, slot_label=slot_label, media_count=len(state.get("media_file_ids") or ()), property_name=prop),
            advisor_url=advisor_url,
        )
        return TelegramServiceOutcome(True, action, True)
    if action == "repair_confirm":
        draft = _draft_from_session(user_data.get(SERVICE_REQUEST_SESSION_KEY))
        state = _repair_state(user_data)
        if draft is None or state is None:
            return TelegramServiceOutcome(True, action, False)
        slot = str(state.get("slot") or "")
        slot_label = SERVICE_SLOT_LABELS.get(slot)
        if not slot_label:
            return TelegramServiceOutcome(True, action, False)
        binding = service.active_binding(user.user_id)
        prop = str(getattr(binding, "property_name", "") or "") if binding is not None else ""
        detail = (
            f"报修：{draft.issue_label}\n说明：{draft.detail}\n希望时间：{slot_label}"
            f"\n附件数：{len(state.get('media_file_ids') or ())}"
        )
        ticket_id = None
        effect = None
        outcome = "failed"
        if binding is not None:
            try:
                submission = service.submit_repair(user_id=user.user_id, draft=draft, slot=slot)
            except Exception:
                submission = None
            if submission is not None:
                ticket_id = submission.ticket.id
                outcome = "ticket"
                if effects is not None:
                    effect = await effects.repair(bot=getattr(context, "bot", None), user=user, submission=submission)
            elif effects is not None:
                effect = await effects.general(bot=getattr(context, "bot", None), user=user, details=detail)
                if _effect_success(effect):
                    outcome = "handoff"
        elif effects is not None:
            effect = await effects.general(bot=getattr(context, "bot", None), user=user, details=detail)
            if _effect_success(effect):
                outcome = "handoff"
        result_view = repair_result_view(
            outcome=outcome,
            issue_label=draft.issue_label,
            property_name=prop,
        )
        if outcome == "ticket" and ticket_id is not None:
            result_view = _with_context_contact(
                result_view,
                callback_data=f"v3u:service:consult_ticket:{ticket_id}",
                extra_line=f"工单编号｜#{ticket_id}",
            )
        await render_service_view(query, result_view, advisor_url=advisor_url)
        if outcome != "failed":
            user_data.pop(SERVICE_REQUEST_SESSION_KEY, None)
            user_data.pop(SERVICE_REQUEST_ANCHOR_KEY, None)
        return TelegramServiceOutcome(True, action, True, effect, ticket_id)
    if action == "repair_exit":
        user_data.pop(SERVICE_REQUEST_SESSION_KEY, None)
        user_data.pop(SERVICE_REQUEST_ANCHOR_KEY, None)
        await render_service_view(query, repair_exit_view(), advisor_url=advisor_url)
        return TelegramServiceOutcome(True, action, True)

    # Property coordination transaction.
    if action == "coordination":
        user_data.pop(PROPERTY_SESSION_KEY, None)
        await _send_view(update, context, property_view(), anchor_key=PROPERTY_ANCHOR_KEY, advisor_url=advisor_url)
        return TelegramServiceOutcome(True, action, True)
    if action.startswith("coordination_category:"):
        parsed = _property_category(action)
        if parsed is None:
            return TelegramServiceOutcome(True, action, False)
        key, label = parsed
        user_data[PROPERTY_SESSION_KEY] = {"category_key": key, "category": label, "description": "", "event_time": "", "contacted": None, "stage": "description"}
        await render_service_view(query, property_description_view(label), advisor_url=advisor_url)
        return TelegramServiceOutcome(True, action, True)
    if action == "coordination_modify_desc":
        state = user_data.get(PROPERTY_SESSION_KEY)
        if not isinstance(state, dict):
            return TelegramServiceOutcome(True, action, False)
        state["description"] = ""
        state["stage"] = "description"
        await render_service_view(query, property_description_view(str(state.get("category") or "物业协调")), advisor_url=advisor_url)
        return TelegramServiceOutcome(True, action, True)
    if action == "coordination_modify_time":
        state = user_data.get(PROPERTY_SESSION_KEY)
        if not isinstance(state, dict):
            return TelegramServiceOutcome(True, action, False)
        state["event_time"] = ""
        state["stage"] = "time"
        await render_service_view(query, property_time_view(), advisor_url=advisor_url)
        return TelegramServiceOutcome(True, action, True)
    if action == "coordination_modify_contacted":
        state = user_data.get(PROPERTY_SESSION_KEY)
        if not isinstance(state, dict):
            return TelegramServiceOutcome(True, action, False)
        state["contacted"] = None
        state["stage"] = "contacted"
        await render_service_view(query, property_contacted_view(), advisor_url=advisor_url)
        return TelegramServiceOutcome(True, action, True)
    if action.startswith("coordination_time:"):
        state = user_data.get(PROPERTY_SESSION_KEY)
        if not isinstance(state, dict):
            return TelegramServiceOutcome(True, action, False)
        value = action.split(":", 1)[1]
        if value == "other":
            state["stage"] = "time_text"
            await render_service_view(query, ServiceView("property_time_text", "<b>其他时间</b>\n\n请直接发送时间描述，例如：昨晚11点左右。", ((ServiceChoice("返回", "v3u:service:property_modify_time"),),)), advisor_url=advisor_url)
            return TelegramServiceOutcome(True, action, True)
        state["event_time"] = value
        state["stage"] = "contacted"
        await render_service_view(query, property_contacted_view(), advisor_url=advisor_url)
        return TelegramServiceOutcome(True, action, True)
    if action.startswith("coordination_contacted:"):
        state = user_data.get(PROPERTY_SESSION_KEY)
        if not isinstance(state, dict):
            return TelegramServiceOutcome(True, action, False)
        state["contacted"] = action.endswith(":yes")
        state["stage"] = "confirm"
        binding = service.active_binding(user.user_id)
        prop = str(getattr(binding, "property_name", "") or "") if binding is not None else ""
        await render_service_view(query, property_confirm_view(
            category=str(state.get("category") or ""),
            description=str(state.get("description") or ""),
            event_time=str(state.get("event_time") or ""),
            contacted=bool(state.get("contacted")),
            property_name=prop,
        ), advisor_url=advisor_url)
        return TelegramServiceOutcome(True, action, True)
    if action == "coordination_confirm":
        state = user_data.get(PROPERTY_SESSION_KEY)
        if not isinstance(state, dict):
            return TelegramServiceOutcome(True, action, False)
        details = (
            f"物业协调\n类型：{state.get('category','')}\n说明：{state.get('description','')}"
            f"\n发生时间：{state.get('event_time','')}\n已联系物业：{'是' if state.get('contacted') else '否'}"
        )
        effect = await effects.general(bot=getattr(context, "bot", None), user=user, details=details) if effects else None
        success = _effect_success(effect)
        await render_service_view(query, property_result_view(success=success), advisor_url=advisor_url)
        if success:
            user_data.pop(PROPERTY_SESSION_KEY, None)
            user_data.pop(PROPERTY_ANCHOR_KEY, None)
        return TelegramServiceOutcome(True, action, True, effect)
    if action == "coordination_exit":
        user_data.pop(PROPERTY_SESSION_KEY, None)
        user_data.pop(PROPERTY_ANCHOR_KEY, None)
        await render_service_view(query, property_exit_view(), advisor_url=advisor_url)
        return TelegramServiceOutcome(True, action, True)

    if action in {"utilities", "moving", "cleaning", "network_help"}:
        await _send_view(update, context, utility_stub_view(action), advisor_url=advisor_url)
        return TelegramServiceOutcome(True, action, True)

    if action == "local":
        await render_service_view(query, local_life_view(), advisor_url=advisor_url)
        return TelegramServiceOutcome(True, action, True)
    if action == "general":
        await _send_view(update, context, general_prompt_view(), advisor_url=advisor_url)
        user_data[SERVICE_GENERAL_WAIT_KEY] = True
        user_data.pop(SERVICE_NEARBY_WAIT_KEY, None)
        return TelegramServiceOutcome(True, action, True)
    if action == "nearby":
        await render_service_view(query, nearby_view(), advisor_url=advisor_url)
        return TelegramServiceOutcome(True, action, True)
    if action == "nearby_other":
        await _send_view(update, context, general_prompt_view(nearby=True), advisor_url=advisor_url)
        user_data[SERVICE_NEARBY_WAIT_KEY] = True
        user_data.pop(SERVICE_GENERAL_WAIT_KEY, None)
        return TelegramServiceOutcome(True, action, True)
    if action == "rfcity":
        await render_service_view(query, rfcity_home_view(), advisor_url=advisor_url)
        return TelegramServiceOutcome(True, action, True)
    if action.startswith("rfcity:"):
        await render_service_view(query, _rfcity_category_product_view(action.split(":", 1)[1]), advisor_url=advisor_url)
        return TelegramServiceOutcome(True, action, True)

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

    state = _repair_state(user_data)
    draft = _draft_from_session(state)
    if draft is not None and state is not None and str(state.get("stage") or "") == "description":
        if len(text) < 4:
            await _edit_anchor(context, user_data, SERVICE_REQUEST_ANCHOR_KEY, issue_prompt_view(draft), advisor_url=advisor_url, fallback_message=message)
            return TelegramServiceOutcome(True, "repair_detail", True)
        updated = service.with_detail(draft, text)
        _store_draft(user_data, updated, stage="media")
        await _edit_anchor(context, user_data, SERVICE_REQUEST_ANCHOR_KEY, repair_media_view(updated, len(state.get("media_file_ids") or ())), advisor_url=advisor_url, fallback_message=message)
        return TelegramServiceOutcome(True, "repair_detail", True)

    prop = user_data.get(PROPERTY_SESSION_KEY)
    if isinstance(prop, dict):
        stage = str(prop.get("stage") or "")
        if stage == "description":
            if len(text) < 2:
                return TelegramServiceOutcome(True, "property_description", True)
            prop["description"] = text[:800]
            prop["stage"] = "time"
            await _edit_anchor(context, user_data, PROPERTY_ANCHOR_KEY, property_time_view(), advisor_url=advisor_url, fallback_message=message)
            return TelegramServiceOutcome(True, "property_description", True)
        if stage == "time_text":
            if len(text) < 1:
                return TelegramServiceOutcome(True, "property_time_text", True)
            prop["event_time"] = text[:120]
            prop["stage"] = "contacted"
            await _edit_anchor(context, user_data, PROPERTY_ANCHOR_KEY, property_contacted_view(), advisor_url=advisor_url, fallback_message=message)
            return TelegramServiceOutcome(True, "property_time_text", True)

    nearby = bool(user_data.get(SERVICE_NEARBY_WAIT_KEY))
    general = bool(user_data.get(SERVICE_GENERAL_WAIT_KEY))
    if not nearby and not general:
        return TelegramServiceOutcome(False)
    if len(text) < 2:
        return TelegramServiceOutcome(True, "nearby_text" if nearby else "general_text", True)
    effect = await effects.general(bot=getattr(context, "bot", None), user=_lead_user(update), details=text, nearby=nearby) if effects else None
    await _reply(message, general_success_view(nearby=nearby), advisor_url=advisor_url)
    user_data.pop(SERVICE_NEARBY_WAIT_KEY, None)
    user_data.pop(SERVICE_GENERAL_WAIT_KEY, None)
    return TelegramServiceOutcome(True, "nearby_text" if nearby else "general_text", True, effect)


async def handle_v3_service_media(
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
    state = _repair_state(user_data)
    draft = _draft_from_session(state)
    if draft is None or state is None or str(state.get("stage") or "") != "media":
        return TelegramServiceOutcome(False)
    file_id = ""
    photos = getattr(message, "photo", None) or ()
    if photos:
        file_id = str(getattr(photos[-1], "file_id", "") or "")
    if not file_id:
        video = getattr(message, "video", None)
        file_id = str(getattr(video, "file_id", "") or "") if video is not None else ""
    if not file_id:
        return TelegramServiceOutcome(False)
    ids = list(state.get("media_file_ids") or ())
    if file_id not in ids:
        ids.append(file_id)
    _store_draft(user_data, draft, stage="media", media_file_ids=ids)
    await _edit_anchor(context, user_data, SERVICE_REQUEST_ANCHOR_KEY, repair_media_view(draft, len(ids)), advisor_url=advisor_url, fallback_message=message)
    return TelegramServiceOutcome(True, "repair_media", True)


__all__ = [
    "SERVICE_GENERAL_WAIT_KEY", "SERVICE_NEARBY_WAIT_KEY", "SERVICE_REQUEST_SESSION_KEY",
    "SERVICE_REQUEST_ANCHOR_KEY", "PROPERTY_SESSION_KEY", "PROPERTY_ANCHOR_KEY",
    "TelegramServiceOutcome", "build_service_keyboard", "handle_v3_service_callback",
    "handle_v3_service_text", "handle_v3_service_media", "render_service_view",
    "_service_home_with_tenant_entry", "_tenant_binding_view",
]