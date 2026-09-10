"""Telegram-neutral view models for side-effect-free V3 transitions.

Callbacks are represented as semantic choices instead of legacy callback strings.
Consult remains outside this pure view layer because contact effects are handled
at the Telegram boundary.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from html import escape as he
from typing import Literal

from .appointments import display_date
from .listing_presenter import build_public_listing_details
from .public_appointment import PublicAppointmentDraft
from .public_inventory import PublicInventoryReader
from .search_navigation import AREA_OPTIONS, LAYOUT_OPTIONS
from .transition_plan import TransitionPlan


TransitionChoiceKind = Literal[
    "appointment_date",
    "appointment_other_date",
    "appointment_mode",
    "appointment_time",
    "appointment_other_time",
    "appointment_back_date",
    "listing_details",
    "home",
    "budget_choice",
    "budget_custom",
    "change_search",
    "search_area",
    "search_budget",
    "search_layout",
    "search_available",
    "area_choice",
    "area_other",
    "layout_choice",
]


@dataclass(frozen=True)
class TransitionChoice:
    label: str
    kind: TransitionChoiceKind
    value: str = ""
    public_listing_id: str = ""
    budget_min: int | None = None
    budget_max: int | None = None


@dataclass(frozen=True)
class TransitionView:
    kind: str
    text: str
    rows: tuple[tuple[TransitionChoice, ...], ...]


def _format_price(value: int | None) -> str:
    return f"${int(value):,}/月" if value is not None and int(value) > 0 else ""


def _resolve_bookable_details(
    inventory: PublicInventoryReader,
    public_listing_id: str,
):
    view = inventory.resolve(public_listing_id)
    if view is None:
        raise ValueError("listing_not_publicly_published")
    if not view.bookable:
        raise ValueError("listing_not_bookable")
    return build_public_listing_details(view)


def _appointment_date_view(
    draft: PublicAppointmentDraft,
    inventory: PublicInventoryReader,
    *,
    today: date,
) -> TransitionView:
    public_id = draft.public_listing_id
    details = _resolve_bookable_details(inventory, public_id)
    subject = details.subject or details.location or "这套房"
    price = _format_price(details.monthly_rent_usd)
    lines = [
        "🎥 <b>视频看房</b>" if draft.mode == "video" else "📅 <b>预约看房</b>",
        "",
        f"🏠 <b>{he(subject)}</b>",
        f"🆔 {he(public_id)}",
    ]
    if price:
        lines.append(f"💵 <b>{he(price)}</b>")
    lines.extend(["", "请选择看房日期："])
    mode_choice = (
        TransitionChoice("🚶 改为实地看房", "appointment_mode", "offline")
        if draft.mode == "video"
        else TransitionChoice("🎥 改为视频看房", "appointment_mode", "video")
    )

    today_value = today.strftime("%m-%d")
    tomorrow = today + timedelta(days=1)
    after = today + timedelta(days=2)
    tomorrow_value = tomorrow.strftime("%m-%d")
    after_value = after.strftime("%m-%d")
    rows = (
        (
            TransitionChoice(f"今天 · {today.strftime('%m月%d日')}", "appointment_date", today_value),
            TransitionChoice(f"明天 · {tomorrow.strftime('%m月%d日')}", "appointment_date", tomorrow_value),
        ),
        (
            TransitionChoice(f"后天 · {after.strftime('%m月%d日')}", "appointment_date", after_value),
            TransitionChoice("📅 其他日期", "appointment_other_date"),
        ),
        (mode_choice,),
        (
            TransitionChoice("⬅️ 返回租赁详情", "listing_details", public_listing_id=public_id),
            TransitionChoice("🏠 返回首页", "home"),
        ),
    )
    return TransitionView(kind="appointment_date", text="\n".join(lines), rows=rows)


def _appointment_time_view(
    draft: PublicAppointmentDraft,
    inventory: PublicInventoryReader,
) -> TransitionView:
    if not draft.date:
        raise ValueError("appointment_time_view_requires_date")
    details = _resolve_bookable_details(inventory, draft.public_listing_id)
    subject = details.subject or details.location or "这套房"
    text = (
        "🕐 <b>选择看房时间</b>\n\n"
        f"🏠 {he(subject)}\n"
        f"🆔 {he(draft.public_listing_id)}\n"
        f"📅 {he(display_date(draft.date))}"
    )
    rows = (
        (TransitionChoice("上午 09:00–12:00", "appointment_time", "am"),),
        (TransitionChoice("下午 14:00–17:00", "appointment_time", "pm"),),
        (TransitionChoice("晚上 17:00–19:00", "appointment_time", "evening"),),
        (TransitionChoice("✍️ 其他时间", "appointment_other_time"),),
        (
            TransitionChoice("⬅️ 修改日期", "appointment_back_date"),
            TransitionChoice("🏠 返回首页", "home"),
        ),
    )
    return TransitionView(kind="appointment_time", text=text, rows=rows)


def _custom_date_prompt() -> TransitionView:
    return TransitionView(
        kind="appointment_custom_date",
        text=(
            "📅 <b>请输入看房日期</b>\n\n"
            "例如：<code>0905</code>、<code>9月5日</code> 或 <code>下周三</code>。\n"
            "提交后会再让您选择具体时间。"
        ),
        rows=(),
    )


def _custom_time_prompt() -> TransitionView:
    return TransitionView(
        kind="appointment_custom_time",
        text=(
            "🕐 <b>请输入看房时间</b>\n\n"
            "例如：<code>20:00</code> 或 <code>晚上8点</code>"
        ),
        rows=(),
    )


def _custom_area_prompt() -> TransitionView:
    return TransitionView(
        kind="search_custom_area",
        text=(
            "📍 <b>其他位置</b>\n\n"
            "直接输入区域或附近地标。\n"
            "例如：<code>BKK1</code>、<code>永旺1附近</code>。"
        ),
        rows=(),
    )


_BUDGET_OPTIONS = (
    ("b1", "$400以内", None, 400),
    ("b2", "$400–600", 400, 600),
    ("b3", "$600–800", 600, 800),
    ("b4", "$800–1200", 800, 1200),
    ("b5", "$1200–1500", 1200, 1500),
    ("b6", "$1500+", 1500, None),
)


def budget_bounds(code: object) -> tuple[str, int | None, int | None]:
    clean = str(code or "").strip()
    for opt_code, label, budget_min, budget_max in _BUDGET_OPTIONS:
        if opt_code == clean:
            return (label, budget_min, budget_max)
    raise ValueError("unsupported_budget_choice")


def _search_budget_view(
    area_display: str = "",
    *,
    back_label: str = "⬅️ 返回找房",
) -> TransitionView:
    clean_area = str(area_display or "").strip()
    area_line = f"\n已选：{he(clean_area)}" if clean_area else ""
    choices = tuple(
        TransitionChoice(
            label,
            "budget_choice",
            code,
            budget_min=budget_min,
            budget_max=budget_max,
        )
        for code, label, budget_min, budget_max in _BUDGET_OPTIONS
    )
    rows = (
        (choices[0], choices[1]),
        (choices[2], choices[3]),
        (choices[4], choices[5]),
        (TransitionChoice("✍️ 自己输入", "budget_custom"),),
        (TransitionChoice(back_label, "change_search"),),
    )
    return TransitionView(
        kind="search_budget",
        text=f"💰 <b>每月预算大概多少？</b>\n\n单位：USD / 月{area_line}",
        rows=rows,
    )


def _search_area_view() -> TransitionView:
    choices = tuple(TransitionChoice(label, "area_choice", code) for code, label in AREA_OPTIONS)
    rows = tuple(tuple(choices[index : index + 2]) for index in range(0, len(choices), 2)) + (
        (TransitionChoice("📍 其他区域", "area_other"),),
        (TransitionChoice("⬅️ 返回找房", "change_search"),),
    )
    return TransitionView(
        kind="search_area",
        text="📍 <b>想住哪里？</b>\n\n选一个大概位置就行。",
        rows=rows,
    )


def _search_layout_view() -> TransitionView:
    choices = tuple(TransitionChoice(label, "layout_choice", code) for code, label in LAYOUT_OPTIONS)
    rows = (
        (choices[0], choices[1]),
        (choices[2], choices[3]),
        (choices[4], choices[5]),
        (TransitionChoice("⬅️ 返回找房", "change_search"),),
    )
    return TransitionView(
        kind="search_layout",
        text="🛏 <b>想要什么户型？</b>",
        rows=rows,
    )


def _similar_view(plan: TransitionPlan) -> TransitionView:
    if plan.similar is None:
        raise ValueError("similar_transition_missing_intent")
    return _search_budget_view(plan.similar.intent.area_display, back_label="⬅️ 返回")


_SEARCH_ENTRY_TEXT = (
    "🔍 <b>想找什么样的房子？</b>\n\n"
    "直接发一句话就可以：\n\n"
    "「BKK1 一房，预算 $600」\n"
    "「富力城两房，要能做饭」\n"
    "「想找高层、安静一点的」\n\n"
    "我们会根据您的需求，优先筛选 2–3 套更值得看的房源。\n\n"
    "还没想好？也可以按条件找 👇"
)


def _change_search_view(plan: TransitionPlan) -> TransitionView:
    if plan.change_search is None:
        raise ValueError("change_search_transition_missing_state")
    rows = (
        (
            TransitionChoice("📍 按区域", "search_area"),
            TransitionChoice("💰 按预算", "search_budget"),
        ),
        (
            TransitionChoice("🏠 按户型", "search_layout"),
            TransitionChoice("🏘 当前可约", "search_available"),
        ),
        (TransitionChoice("⬅️ 返回首页", "home"),),
    )
    return TransitionView(kind="search_entry", text=_SEARCH_ENTRY_TEXT, rows=rows)


class TransitionViewService:
    def __init__(self, inventory: PublicInventoryReader):
        self.inventory = inventory

    def appointment_date(
        self,
        draft: PublicAppointmentDraft,
        *,
        today: date | None = None,
    ) -> TransitionView:
        return _appointment_date_view(draft, self.inventory, today=today or date.today())

    def appointment_time(self, draft: PublicAppointmentDraft) -> TransitionView:
        return _appointment_time_view(draft, self.inventory)

    @staticmethod
    def custom_date_prompt() -> TransitionView:
        return _custom_date_prompt()

    @staticmethod
    def custom_time_prompt() -> TransitionView:
        return _custom_time_prompt()

    @staticmethod
    def custom_area_prompt() -> TransitionView:
        return _custom_area_prompt()

    @staticmethod
    def search_area() -> TransitionView:
        return _search_area_view()

    @staticmethod
    def search_budget(area_display: str = "") -> TransitionView:
        return _search_budget_view(area_display)

    @staticmethod
    def search_layout() -> TransitionView:
        return _search_layout_view()

    def build(
        self,
        plan: TransitionPlan,
        *,
        today: date | None = None,
    ) -> TransitionView:
        if plan.kind == "book":
            if plan.book is None:
                raise ValueError("book_transition_missing_public_draft")
            return self.appointment_date(plan.book.draft, today=today)
        if plan.kind == "similar":
            return _similar_view(plan)
        if plan.kind == "change_search":
            return _change_search_view(plan)
        if plan.kind == "consult":
            raise ValueError("consult_transition_requires_effect_executor")
        raise ValueError(f"unsupported_transition_view:{plan.kind}")


__all__ = [
    "TransitionChoice",
    "TransitionChoiceKind",
    "TransitionView",
    "TransitionViewService",
    "budget_bounds",
]
