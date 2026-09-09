"""Independent V3 /start handler for home, broadcast shortcuts, and channel deep links."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto
from telegram.constants import ParseMode

from .contact_effects import ContactEffectExecutor
from .home_views import build_contact_view, build_home_view
from .lead_service import LeadUser
from .public_flow import PublicListingFlowResult, PublicListingFlowService
from .search_no_match_view import build_search_no_match_view
from .search_query import SearchCriteria
from .search_submit_executor import SearchSubmitExecutor
from .telegram_home_ui import build_home_keyboard
from .telegram_search_results import present_search_flow_result
from .telegram_transition_ui import build_transition_keyboard
from .telegram_ui import build_action_keyboard
from .transition_actions import SearchSubmitIntent
from .transition_plan import BookTransition, ChangeSearchTransition, TransitionPlan
from .transition_session import apply_session_mutation, build_transition_session
from .transition_views import TransitionViewService


BROADCAST_START_SHORTCUTS = frozenset({"find_home", "latest", "advisor"})


@dataclass(frozen=True)
class TelegramStartOutcome:
    handled: bool
    kind: str
    payload: str = ""
    result: PublicListingFlowResult | None = None


def _chat_id(update: Any) -> int | str:
    chat = getattr(update, "effective_chat", None)
    value = getattr(chat, "id", None)
    if value is None:
        raise ValueError("telegram_effective_chat_missing_for_start")
    return value


def _lead_user(update: Any) -> LeadUser:
    user = getattr(update, "effective_user", None)
    if user is None or getattr(user, "id", None) is None:
        raise ValueError("telegram_effective_user_missing_for_start")
    display_name = str(getattr(user, "full_name", "") or "").strip()
    if not display_name:
        display_name = " ".join(
            value
            for value in (
                str(getattr(user, "first_name", "") or "").strip(),
                str(getattr(user, "last_name", "") or "").strip(),
            )
            if value
        )
    return LeadUser(
        user_id=int(user.id),
        username=str(getattr(user, "username", "") or ""),
        display_name=display_name,
    )


def _book_plan(result: PublicListingFlowResult) -> TransitionPlan:
    if result.book is None:
        raise ValueError("start_book_result_missing_intent")
    intent = result.book
    from .public_appointment import PublicAppointmentDraft

    return TransitionPlan(
        kind="book",
        next_step="appointment_date",
        effects=("render_appointment_date",),
        book=BookTransition(
            draft=PublicAppointmentDraft(
                public_listing_id=intent.public_listing_id,
                mode="offline",
                source=intent.source,
            )
        ),
    )


def _search_entry_plan() -> TransitionPlan:
    return TransitionPlan(
        kind="change_search",
        next_step="search_entry",
        effects=("render_search_entry",),
        change_search=ChangeSearchTransition(source="daily_broadcast", goal="any"),
    )


async def _render_details(message: Any, result: PublicListingFlowResult) -> None:
    if result.details is None:
        raise ValueError("start_details_result_missing_response")
    await message.reply_text(
        result.details.text,
        parse_mode=ParseMode.HTML,
        reply_markup=build_action_keyboard(result.details.action_rows),
    )


async def _render_photos(update: Any, context: Any, result: PublicListingFlowResult) -> None:
    if result.photos is None:
        raise ValueError("start_photos_result_missing_response")
    chat_id = _chat_id(update)
    for group in result.photos.media_groups:
        if len(group) == 1:
            with Path(group[0]).open("rb") as handle:
                await context.bot.send_photo(chat_id=chat_id, photo=handle)
            continue
        media = [InputMediaPhoto(media=Path(raw).read_bytes()) for raw in group]
        await context.bot.send_media_group(chat_id=chat_id, media=media)
    await context.bot.send_message(
        chat_id=chat_id,
        text=result.photos.text,
        parse_mode=ParseMode.HTML,
        reply_markup=build_action_keyboard(result.photos.action_rows),
    )


async def _render_invalid_link(message: Any) -> None:
    await message.reply_text(
        "这个链接已经失效或房源信息已更新。\n\n您可以重新找房，或直接联系我们。",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("🔍 帮我找房", callback_data="v3u:home:search")],
                [InlineKeyboardButton("💬 联系我们", callback_data="v3u:home:contact")],
                [InlineKeyboardButton("🏠 返回首页", callback_data="v3u:t:home")],
            ]
        ),
    )


async def _handle_broadcast_shortcut(
    update: Any,
    context: Any,
    *,
    payload: str,
    transition_views: TransitionViewService,
    search_executor: SearchSubmitExecutor | None,
    contact_effects: ContactEffectExecutor | None,
    advisor_url: str,
) -> TelegramStartOutcome | None:
    if payload not in BROADCAST_START_SHORTCUTS:
        return None
    message = getattr(update, "effective_message", None)
    user_data = getattr(context, "user_data", None)
    if message is None or not isinstance(user_data, dict):
        raise ValueError("broadcast_shortcut_missing_telegram_context")

    if payload == "find_home":
        plan = _search_entry_plan()
        view = transition_views.build(plan)
        await message.reply_text(
            view.text,
            parse_mode=ParseMode.HTML,
            reply_markup=build_transition_keyboard(view),
        )
        apply_session_mutation(user_data, build_transition_session(plan))
        return TelegramStartOutcome(True, "broadcast_find_home", payload)

    if payload == "latest":
        if search_executor is None:
            await _render_invalid_link(message)
            return TelegramStartOutcome(True, "broadcast_latest_unavailable", payload)
        intent = SearchSubmitIntent(
            criteria=SearchCriteria(raw_text=""),
            source="daily_broadcast_latest",
            goal="any",
            area_display="",
            budget_label="",
            touch_payload={"daily_broadcast": True, "latest": True},
        )
        execution = search_executor.execute(intent, limit=5)
        presentation = await present_search_flow_result(update, context, execution.result)
        if presentation.status == "no_match":
            view = build_search_no_match_view(intent)
            await message.reply_text(
                view.text,
                parse_mode=ParseMode.HTML,
                reply_markup=build_transition_keyboard(view),
            )
        return TelegramStartOutcome(True, "broadcast_latest", payload)

    if contact_effects is not None:
        await contact_effects.execute_general(
            bot=getattr(context, "bot", None),
            user=_lead_user(update),
            source="daily_broadcast",
        )
    view = build_contact_view(advisor_url=advisor_url)
    await message.reply_text(
        view.text,
        parse_mode=ParseMode.HTML,
        reply_markup=build_home_keyboard(view),
    )
    return TelegramStartOutcome(True, "broadcast_advisor", payload)


async def handle_v3_start(
    update: Any,
    context: Any,
    *,
    listings: PublicListingFlowService,
    transition_views: TransitionViewService,
    channel_url: str = "",
    search_executor: SearchSubmitExecutor | None = None,
    contact_effects: ContactEffectExecutor | None = None,
    advisor_url: str = "",
) -> TelegramStartOutcome:
    message = getattr(update, "effective_message", None)
    if message is None:
        return TelegramStartOutcome(handled=False, kind="no_message")
    user_data = getattr(context, "user_data", None)
    if not isinstance(user_data, dict):
        raise ValueError("telegram_user_data_missing_for_start")

    args = tuple(getattr(context, "args", None) or ())
    user_data.clear()
    if not args:
        home = build_home_view(channel_url=channel_url)
        await message.reply_text(
            home.text,
            parse_mode=ParseMode.HTML,
            reply_markup=build_home_keyboard(home),
        )
        return TelegramStartOutcome(handled=True, kind="home")

    payload = str(args[0] or "").strip()
    broadcast = await _handle_broadcast_shortcut(
        update,
        context,
        payload=payload,
        transition_views=transition_views,
        search_executor=search_executor,
        contact_effects=contact_effects,
        advisor_url=advisor_url,
    )
    if broadcast is not None:
        return broadcast

    result = listings.resolve(payload)
    if not result.ok:
        await _render_invalid_link(message)
        return TelegramStartOutcome(
            handled=True,
            kind="invalid_link",
            payload=payload,
            result=result,
        )

    if result.action == "details":
        await _render_details(message, result)
        return TelegramStartOutcome(True, "details", payload, result)
    if result.action == "photos":
        await _render_photos(update, context, result)
        return TelegramStartOutcome(True, "photos", payload, result)
    if result.action == "book":
        plan = _book_plan(result)
        view = transition_views.build(plan)
        mutation = build_transition_session(plan)
        await message.reply_text(
            view.text,
            parse_mode=ParseMode.HTML,
            reply_markup=build_transition_keyboard(view),
        )
        apply_session_mutation(user_data, mutation)
        return TelegramStartOutcome(True, "book", payload, result)

    raise AssertionError(f"unsupported_v3_start_action:{result.action}")


__all__ = [
    "BROADCAST_START_SHORTCUTS",
    "TelegramStartOutcome",
    "handle_v3_start",
]
