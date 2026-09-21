"""Independent V3 /start handler for home, broadcast shortcuts, and channel deep links."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode

from v3_core.publishing.public_ids import normalize_public_id

from .appointment_history import AppointmentHistoryService
from .assurance_views import build_assurance_home_view
from .contact_effects import ContactEffectExecutor
from .deeplink import parse_channel_start_payload, parse_search_start_payload
from .home_views import build_appointment_history_home_view, build_contact_view, build_home_view
from .lead_service import LeadUser
from .listing_contact import ListingContactEffectExecutor, build_listing_contact_view
from .listing_presenter import build_public_listing_details
from .public_flow import PublicListingFlowResult, PublicListingFlowService
from .search_no_match_view import build_search_no_match_view
from .search_query import SearchCriteria
from .search_submit_executor import SearchSubmitExecutor
from .telegram_callback_handler import LISTING_SOURCE_KEY, LISTING_TOUCHPOINT_KEY
from .telegram_assurance_handler import build_assurance_keyboard
from .telegram_home_ui import build_home_keyboard
from .telegram_navigation import advisor_handoff_url, polish_listing_keyboard
from .telegram_search_results import present_search_flow_result
from .service_product_views import service_home_view
from .service_flow import TenantService
from .telegram_service_handler import build_service_keyboard
from .tenant_v1 import tenant_home_view
from .telegram_transition_ui import build_transition_keyboard
from .transition_session import APPOINTMENT_SESSION_KEY, SEARCH_PREF_SESSION_KEY
from .callbacks import encode_listing_callback
from .home_callbacks import encode_home_callback
from .telegram_ui import build_action_keyboard
from .transition_actions import SearchSubmitIntent
from .transition_plan import BookTransition, ChangeSearchTransition, TransitionPlan
from .transition_session import apply_session_mutation, build_transition_session
from .transition_views import TransitionViewService


BROADCAST_START_SHORTCUTS = frozenset(
    {"find_home", "latest", "advisor", "budget", "appointments", "assurance", "service"}
)


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
    return LeadUser(user_id=int(user.id), username=str(getattr(user, "username", "") or ""), display_name=display_name)


def _book_plan(result: PublicListingFlowResult) -> TransitionPlan:
    if result.book is None:
        raise ValueError("start_book_result_missing_intent")
    intent = result.book
    from .public_appointment import PublicAppointmentDraft
    return TransitionPlan(
        kind="book",
        next_step="appointment_date",
        effects=("render_appointment_date",),
        book=BookTransition(draft=PublicAppointmentDraft(public_listing_id=intent.public_listing_id, mode="offline", source=intent.source)),
    )



def _video_book_plan(result: PublicListingFlowResult) -> TransitionPlan:
    if result.book is None:
        raise ValueError("start_video_book_result_missing_intent")
    intent = result.book
    from .public_appointment import PublicAppointmentDraft
    return TransitionPlan(
        kind="book",
        next_step="appointment_date",
        effects=("render_appointment_date",),
        book=BookTransition(
            draft=PublicAppointmentDraft(
                public_listing_id=intent.public_listing_id,
                mode="video",
                source=intent.source,
            )
        ),
    )


async def _handle_video_start(
    update: Any,
    context: Any,
    *,
    payload: str,
    listings: PublicListingFlowService,
    transition_views: TransitionViewService,
    search_executor: SearchSubmitExecutor | None,
    saved_search_pref: dict[str, Any] | None,
    advisor_url: str,
    channel_url: str,
) -> TelegramStartOutcome | None:
    raw = str(payload or "").strip()
    message = getattr(update, "effective_message", None)
    user_data = getattr(context, "user_data", None)
    if message is None or not isinstance(user_data, dict):
        return None

    if raw == "video":
        # Keep the production fallback when inventory search is unavailable.
        if search_executor is None:
            plan = _search_entry_plan()
            view = transition_views.build(plan)
            await message.reply_text(
                view.text,
                parse_mode=ParseMode.HTML,
                reply_markup=build_transition_keyboard(view),
            )
            apply_session_mutation(user_data, build_transition_session(plan))
            user_data["v3_video_booking_preferred"] = True
            return TelegramStartOutcome(True, "video_search", raw)
        return await _handle_video_inventory_start(
            update,
            context,
            transition_views=transition_views,
            search_executor=search_executor,
            saved_search_pref=saved_search_pref,
        )

    prefix = "book_video_"
    if not raw.startswith(prefix):
        return None
    public_id = normalize_public_id(raw[len(prefix):])
    if public_id is None:
        await _render_invalid_link(
            message, advisor_url=advisor_url, channel_url=channel_url
        )
        return TelegramStartOutcome(True, "invalid_link", raw)

    result = listings.resolve(f"property_{public_id}_book")
    _remember_listing_context(user_data, result)
    if not result.ok:
        if _failure_reason(result) == "listing_not_bookable":
            await _render_unbookable(
                message,
                result,
                advisor_url=advisor_url,
                channel_url=channel_url,
            )
            return TelegramStartOutcome(True, "unbookable", raw, result)
        await _render_invalid_link(
            message, advisor_url=advisor_url, channel_url=channel_url
        )
        return TelegramStartOutcome(True, "invalid_link", raw, result)

    plan = _video_book_plan(result)
    if plan.book is None:
        raise ValueError("start_video_book_transition_missing_draft")
    view = transition_views.appointment_date(plan.book.draft)
    await message.reply_text(
        view.text,
        parse_mode=ParseMode.HTML,
        reply_markup=build_transition_keyboard(view),
    )
    apply_session_mutation(user_data, build_transition_session(plan))
    return TelegramStartOutcome(True, "book_video", raw, result)

def _search_entry_plan() -> TransitionPlan:
    return TransitionPlan(kind="change_search", next_step="search_entry", effects=("render_search_entry",), change_search=ChangeSearchTransition(source="daily_broadcast", goal="any"))


def _is_channel_source(value: object) -> bool:
    source = str(value or "").strip().lower()
    return source in {"channel", "channel_deeplink", "channel_listing"} or source.startswith("channel")


def _listing_keyboard(
    result,
    *,
    advisor_url: str = "",
    channel_url: str = "",
    show_channel: bool = False,
):
    if not result.action_rows:
        return None
    return polish_listing_keyboard(
        build_action_keyboard(result.action_rows),
        advisor_url=advisor_url,
        channel_url=channel_url,
        listing_summary=str(getattr(result, "listing_summary", "") or ""),
        add_home=False,
        add_channel=show_channel,
    )


async def _render_details(
    message: Any,
    result: PublicListingFlowResult,
    *,
    advisor_url: str = "",
    channel_url: str = "",
) -> None:
    if result.details is None:
        raise ValueError("start_details_result_missing_response")
    await message.reply_text(
        result.details.text,
        parse_mode=ParseMode.HTML,
        reply_markup=_listing_keyboard(
            result.details,
            advisor_url=advisor_url,
            channel_url=channel_url,
            show_channel=_is_channel_source(result.source),
        ),
    )


async def _render_photos(
    update: Any,
    context: Any,
    result,
    *,
    advisor_url: str = "",
    channel_url: str = "",
) -> None:
    if result.photos is None:
        raise ValueError("start_photos_result_missing_response")
    chat_id = _chat_id(update)
    photos = result.photos
    keyboard = _listing_keyboard(
        photos,
        advisor_url=advisor_url,
        channel_url=channel_url,
        show_channel=_is_channel_source(result.source),
    )
    photo_path = str(getattr(photos, "photo_path", "") or "").strip()
    if not photo_path and photos.media_groups:
        first = photos.media_groups[0]
        if first:
            photo_path = str(first[0] or "").strip()
    path = Path(photo_path) if photo_path else None
    if path is not None and path.is_file():
        with path.open("rb") as handle:
            await context.bot.send_photo(
                chat_id=chat_id,
                photo=handle,
                caption=photos.text,
                parse_mode=ParseMode.HTML,
                reply_markup=keyboard,
            )
    else:
        await context.bot.send_message(
            chat_id=chat_id,
            text=photos.text,
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard,
        )
    detail = str(getattr(photos, "detail_text", "") or "").strip()
    if detail:
        await context.bot.send_message(
            chat_id=chat_id, text=detail, parse_mode=ParseMode.HTML
        )



def _support_keyboard(*, advisor_url: str = "", channel_url: str = "") -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("🔍 开始找房", callback_data="v3u:home:search")],
    ]
    clean_advisor = str(advisor_url or "").strip()
    if clean_advisor:
        rows.append([InlineKeyboardButton("💬 中文顾问", url=advisor_handoff_url(clean_advisor))])
    else:
        rows.append([InlineKeyboardButton("💬 中文顾问", callback_data="v3u:home:contact")])
    clean_channel = str(channel_url or "").strip()
    if clean_channel:
        rows.append([InlineKeyboardButton("📢 最新房源", url=clean_channel)])
    rows.append([InlineKeyboardButton("⬅️ 回首页", callback_data="v3u:t:home")])
    return InlineKeyboardMarkup(rows)


async def _render_invalid_link(
    message: Any, *, advisor_url: str = "", channel_url: str = ""
) -> None:
    await message.reply_text(
        "这套房的入口已经失效，或信息刚刚更新过。\n\n可以重新找房，或让顾问按你的条件接着看。",
        parse_mode=ParseMode.HTML,
        reply_markup=_support_keyboard(
            advisor_url=advisor_url, channel_url=channel_url
        ),
    )


async def _render_unbookable(
    message: Any,
    result: PublicListingFlowResult,
    *,
    advisor_url: str = "",
    channel_url: str = "",
) -> None:
    await message.reply_text(
        "这套房现在暂时不能预约。\n\n可以先看详情，或让顾问帮你看别的选择。",
        parse_mode=ParseMode.HTML,
    )
    if getattr(result, "details", None) is not None:
        await _render_details(
            message, result, advisor_url=advisor_url, channel_url=channel_url
        )
        return
    await _render_invalid_link(
        message, advisor_url=advisor_url, channel_url=channel_url
    )


def _failure_reason(result: object) -> str:
    return str(getattr(result, "reason", "") or "").strip()


def _remember_listing_context(user_data: dict[str, Any], result: PublicListingFlowResult) -> None:
    source = str(getattr(result, "source", "") or "").strip()
    if source:
        user_data[LISTING_SOURCE_KEY] = source
    action = str(getattr(result, "action", "") or "")
    if action == "details" or getattr(result, "details", None) is not None:
        user_data[LISTING_TOUCHPOINT_KEY] = "listing_details"
    elif action == "photos":
        user_data[LISTING_TOUCHPOINT_KEY] = "listing_photos"


async def _handle_broadcast_shortcut(
    update: Any,
    context: Any,
    *,
    payload: str,
    transition_views: TransitionViewService,
    search_executor: SearchSubmitExecutor | None,
    appointment_history: AppointmentHistoryService | None,
    tenant_service: TenantService | None,
    contact_effects: ContactEffectExecutor | None,
    advisor_url: str,
    channel_url: str,
) -> TelegramStartOutcome | None:
    if payload not in BROADCAST_START_SHORTCUTS:
        return None
    message = getattr(update, "effective_message", None)
    user_data = getattr(context, "user_data", None)
    if message is None or not isinstance(user_data, dict):
        raise ValueError("broadcast_shortcut_missing_telegram_context")

    if payload in {"find_home", "budget"}:
        plan = _search_entry_plan()
        view = transition_views.search_budget() if payload == "budget" else transition_views.build(plan)
        await message.reply_text(view.text, parse_mode=ParseMode.HTML, reply_markup=build_transition_keyboard(view))
        apply_session_mutation(user_data, build_transition_session(plan))
        return TelegramStartOutcome(True, f"broadcast_{payload}", payload)

    if payload == "latest":
        if search_executor is None:
            await _render_invalid_link(
                message, advisor_url=advisor_url, channel_url=channel_url
            )
            return TelegramStartOutcome(True, "broadcast_latest_unavailable", payload)
        intent = SearchSubmitIntent(criteria=SearchCriteria(raw_text=""), source="daily_broadcast_latest", goal="any", area_display="", budget_label="", touch_payload={"daily_broadcast": True, "latest": True})
        execution = search_executor.execute(intent, limit=5)
        presentation = await present_search_flow_result(update, context, execution.result)
        if presentation.status == "no_match":
            view = build_search_no_match_view(intent)
            await message.reply_text(view.text, parse_mode=ParseMode.HTML, reply_markup=build_transition_keyboard(view))
        return TelegramStartOutcome(True, "broadcast_latest", payload)

    if payload == "appointments":
        if appointment_history is None:
            await _render_invalid_link(
                message, advisor_url=advisor_url, channel_url=channel_url
            )
            return TelegramStartOutcome(True, "broadcast_appointments_unavailable", payload)
        user = _lead_user(update)
        view = build_appointment_history_home_view(appointment_history.build(user.user_id))
        await message.reply_text(
            view.text,
            parse_mode=ParseMode.HTML,
            reply_markup=build_home_keyboard(view),
        )
        return TelegramStartOutcome(True, "broadcast_appointments", payload)

    if payload == "assurance":
        view = build_assurance_home_view()
        await message.reply_text(
            view.text,
            parse_mode=ParseMode.HTML,
            reply_markup=build_assurance_keyboard(view),
        )
        return TelegramStartOutcome(True, "broadcast_assurance", payload)

    if payload == "service":
        view = service_home_view()
        await message.reply_text(
            view.text,
            parse_mode=ParseMode.HTML,
            reply_markup=build_service_keyboard(view, advisor_url=advisor_url),
        )
        return TelegramStartOutcome(True, "broadcast_service", payload)

    if contact_effects is not None:
        await contact_effects.execute_general(bot=getattr(context, "bot", None), user=_lead_user(update), source="daily_broadcast")
    view = build_contact_view(advisor_url=advisor_url)
    await message.reply_text(view.text, parse_mode=ParseMode.HTML, reply_markup=build_home_keyboard(view))
    return TelegramStartOutcome(True, "broadcast_advisor", payload)


async def _handle_public_search_start(
    update: Any,
    context: Any,
    *,
    payload: str,
    transition_views: TransitionViewService,
    search_executor: SearchSubmitExecutor | None,
) -> TelegramStartOutcome | None:
    route = parse_search_start_payload(payload)
    if route is None:
        return None
    message = getattr(update, "effective_message", None)
    user_data = getattr(context, "user_data", None)
    if message is None or not isinstance(user_data, dict):
        raise ValueError("public_search_start_missing_telegram_context")
    if route.action == "find":
        plan = TransitionPlan(
            kind="change_search",
            next_step="search_entry",
            effects=("render_search_entry",),
            change_search=ChangeSearchTransition(source="deeplink_find", goal="any"),
        )
        view = transition_views.build(plan)
        await message.reply_text(
            view.text,
            parse_mode=ParseMode.HTML,
            reply_markup=build_transition_keyboard(view),
        )
        apply_session_mutation(user_data, build_transition_session(plan))
        return TelegramStartOutcome(True, "find", payload)
    if search_executor is None:
        return TelegramStartOutcome(True, "more_unavailable", payload)
    intent = SearchSubmitIntent(
        criteria=SearchCriteria(location_keys=(route.location_key,)),
        source="channel_more",
        goal="any",
        area_display=route.location_key,
        budget_label="",
        touch_payload={"area_slug": route.area_slug},
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
    return TelegramStartOutcome(True, "more_area", payload)


async def _handle_listing_contact_start(
    update: Any,
    context: Any,
    *,
    payload: str,
    listings: PublicListingFlowService,
    listing_contact_effects: ListingContactEffectExecutor | None,
    advisor_url: str,
) -> TelegramStartOutcome | None:
    route = parse_channel_start_payload(payload)
    if route is None or route.action != "contact":
        return None
    inventory = getattr(getattr(listings, "routes", None), "inventory", None)
    if inventory is None:
        return TelegramStartOutcome(True, "invalid_link", payload)
    from .consult import ConsultService
    resolved = ConsultService(inventory).resolve(
        route.public_listing_id,
        source=route.source,
        touchpoint="channel_listing",
    )
    if not resolved.ok or resolved.intent is None:
        return TelegramStartOutcome(True, "invalid_link", payload)
    if listing_contact_effects is not None:
        await listing_contact_effects.execute(
            bot=getattr(context, "bot", None),
            user=_lead_user(update),
            intent=resolved.intent,
        )
    view = build_listing_contact_view(
        resolved.intent,
        inventory,
        advisor_url=advisor_url,
    )
    direct = advisor_handoff_url(
        advisor_url,
        public_listing_id=view.public_listing_id,
    )
    rows = []
    if direct:
        rows.append([InlineKeyboardButton("💬 中文顾问", url=direct)])
    from .callbacks import encode_listing_callback
    rows.append(
        [InlineKeyboardButton(
            "⬅️ 返回房源详情",
            callback_data=encode_listing_callback("details", view.public_listing_id),
        )]
    )
    message = getattr(update, "effective_message", None)
    await message.reply_text(
        view.text,
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(rows),
    )
    return TelegramStartOutcome(True, "contact", payload)



async def _handle_video_inventory_start(
    update: Any,
    context: Any,
    *,
    transition_views: TransitionViewService,
    search_executor: SearchSubmitExecutor | None,
    saved_search_pref: dict[str, Any] | None,
) -> TelegramStartOutcome | None:
    message = getattr(update, "effective_message", None)
    user_data = getattr(context, "user_data", None)
    if message is None or not isinstance(user_data, dict):
        return None

    pref = dict(saved_search_pref or {})
    location_keys = tuple(str(v) for v in (pref.get("location_keys") or ()) if str(v).strip())
    budget_min = pref.get("budget_min")
    budget_max = pref.get("budget_max")
    room_type = str(pref.get("room_type") or "").strip()
    area = str(pref.get("area_display") or "").strip() or "未填写"
    budget = str(pref.get("budget_label") or "").strip()
    if not budget:
        if budget_min is not None and budget_max is not None:
            budget = f"$" + str(int(budget_min)) + "–" + str(int(budget_max))
        elif budget_max is not None:
            budget = f"$" + str(int(budget_max)) + "以内"
        elif budget_min is not None:
            budget = f"$" + str(int(budget_min)) + "+"
        else:
            budget = "未填写"
    layout = room_type or "未填写"

    criteria = SearchCriteria(
        location_keys=location_keys,
        budget_min=budget_min,
        budget_max=budget_max,
        room_type=room_type,
    )
    cards = ()
    mode = "strict"
    if search_executor is not None:
        execution = search_executor.execute(
            SearchSubmitIntent(
                criteria=criteria,
                source="video_deeplink",
                goal="any",
                area_display=area if area != "未填写" else "",
                budget_label=budget if budget != "未填写" else "",
                touch_payload={"video": True},
            ),
            limit=2,
        )
        cards = execution.result.cards
        mode = execution.result.mode
        if not cards and criteria.has_filter:
            relaxed = search_executor.flow.similar(criteria, limit=2)
            cards = relaxed.cards
            mode = relaxed.mode

    lines = [
        "🎥 可以，侨联可以先帮你视频代看。",
        "适合这些情况：",
        "✔ 人还没到金边",
        "✔ 没时间一套套跑",
        "✔ 想先确认房子真实情况",
        "✔ 想看看周边环境",
        "✔ 想提前了解家具家电状态",
        "",
        "正在为你从侨联房源库中匹配合适房源：",
        f"区域：{area}",
        f"预算：{budget}",
        f"户型：{layout}",
        "",
        "已先为你匹配 1-2 套：",
    ]
    for index, card in enumerate(cards[:2], start=1):
        published = transition_views.inventory.resolve(card.public_listing_id)
        if published is None:
            continue
        details = build_public_listing_details(published)
        price = ("$" + f"{int(details.monthly_rent_usd):,}") if details.monthly_rent_usd else "价格待确认"
        lines.extend([
            f"{index}. {details.location or '位置待确认'}｜{details.layout or '户型待确认'}｜{price}",
        ])
    lines.extend([
        "",
        "如果完全符合条件的房源较少，系统会先为你放宽条件匹配相近房源，顾问再继续人工精筛。",
        "👇 你可以直接咨询房源，或安排视频代看",
    ])

    rows: list[list[InlineKeyboardButton]] = []
    if cards:
        first_id = cards[0].public_listing_id
        rows.append([InlineKeyboardButton("💬 咨询这套房", callback_data=encode_listing_callback("consult", first_id))])
        user_data[APPOINTMENT_SESSION_KEY] = {
            "public_listing_id": first_id,
            "mode": "video",
            "date": "",
            "time": "",
            "source": "video_deeplink",
        }
        rows.append([InlineKeyboardButton("📅 安排视频代看", callback_data="v3u:t:appointment_mode:video")])
    rows.append([InlineKeyboardButton("🏠 查看更多房源", callback_data=encode_home_callback("search"))])
    await message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(rows))
    user_data["v3_video_booking_preferred"] = True
    return TelegramStartOutcome(True, f"video_{mode}", "video")


async def handle_v3_start(
    update: Any,
    context: Any,
    *,
    listings: PublicListingFlowService,
    transition_views: TransitionViewService,
    channel_url: str = "",
    search_executor: SearchSubmitExecutor | None = None,
    appointment_history: AppointmentHistoryService | None = None,
    tenant_service: TenantService | None = None,
    contact_effects: ContactEffectExecutor | None = None,
    listing_contact_effects: ListingContactEffectExecutor | None = None,
    advisor_url: str = "",
) -> TelegramStartOutcome:
    message = getattr(update, "effective_message", None)
    if message is None:
        return TelegramStartOutcome(handled=False, kind="no_message")
    user_data = getattr(context, "user_data", None)
    if not isinstance(user_data, dict):
        raise ValueError("telegram_user_data_missing_for_start")

    stored_search_pref = user_data.get(SEARCH_PREF_SESSION_KEY)
    saved_search_pref = (
        dict(stored_search_pref) if isinstance(stored_search_pref, dict) else {}
    )
    args = tuple(getattr(context, "args", None) or ())
    user_data.clear()
    if not args:
        home = build_home_view(channel_url=channel_url, advisor_url=advisor_url)
        await message.reply_text(home.text, parse_mode=ParseMode.HTML, reply_markup=build_home_keyboard(home))
        return TelegramStartOutcome(handled=True, kind="home")

    payload = str(args[0] or "").strip()

    video_start = await _handle_video_start(
        update,
        context,
        payload=payload,
        listings=listings,
        transition_views=transition_views,
        search_executor=search_executor,
        saved_search_pref=saved_search_pref,
        advisor_url=advisor_url,
        channel_url=channel_url,
    )
    if video_start is not None:
        return video_start

    search_start = await _handle_public_search_start(
        update,
        context,
        payload=payload,
        transition_views=transition_views,
        search_executor=search_executor,
    )
    if search_start is not None:
        return search_start

    contact_start = await _handle_listing_contact_start(
        update,
        context,
        payload=payload,
        listings=listings,
        listing_contact_effects=listing_contact_effects,
        advisor_url=advisor_url,
    )
    if contact_start is not None:
        return contact_start

    broadcast = await _handle_broadcast_shortcut(
        update,
        context,
        payload=payload,
        transition_views=transition_views,
        search_executor=search_executor,
        appointment_history=appointment_history,
        tenant_service=tenant_service,
        contact_effects=contact_effects,
        advisor_url=advisor_url,
        channel_url=channel_url,
    )
    if broadcast is not None:
        return broadcast

    result = listings.resolve(payload)
    _remember_listing_context(user_data, result)
    if not result.ok:
        if _failure_reason(result) == "listing_not_bookable":
            await _render_unbookable(
                message,
                result,
                advisor_url=advisor_url,
                channel_url=channel_url,
            )
            return TelegramStartOutcome(handled=True, kind="unbookable", payload=payload, result=result)
        await _render_invalid_link(
            message, advisor_url=advisor_url, channel_url=channel_url
        )
        return TelegramStartOutcome(handled=True, kind="invalid_link", payload=payload, result=result if isinstance(result, PublicListingFlowResult) else None)

    if result.action in {"details", "photos"}:
        # Prefer merged photo+detail flipper when a frame exists.
        if getattr(result, "photos", None) is not None and result.photos.has_media:
            await _render_photos(
                update,
                context,
                result,
                advisor_url=advisor_url,
                channel_url=channel_url,
            )
            return TelegramStartOutcome(True, result.action, payload, result)
        if result.action == "details":
            await _render_details(
                message, result, advisor_url=advisor_url, channel_url=channel_url
            )
            return TelegramStartOutcome(True, "details", payload, result)
        await _render_photos(
            update,
            context,
            result,
            advisor_url=advisor_url,
            channel_url=channel_url,
        )
        return TelegramStartOutcome(True, "photos", payload, result)
    if result.action == "book":
        plan = _book_plan(result)
        view = transition_views.build(plan)
        mutation = build_transition_session(plan)
        await message.reply_text(view.text, parse_mode=ParseMode.HTML, reply_markup=build_transition_keyboard(view))
        apply_session_mutation(user_data, mutation)
        return TelegramStartOutcome(True, "book", payload, result)

    raise AssertionError(f"unsupported_v3_start_action:{result.action}")


__all__ = ["BROADCAST_START_SHORTCUTS", "TelegramStartOutcome", "handle_v3_start"]
