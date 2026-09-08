"""Telegram-neutral view models for side-effect-free V3 transitions.

Copy and option order are extracted from the fixed-SHA User Bot, but callbacks
are represented as semantic choices instead of legacy callback strings. Consult
is intentionally not rendered here: fixed-SHA says "已记录" only after lead/admin
side effects, so V3 must not display that state before a real effect executor
succeeds.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from html import escape as he
from typing import Literal

from .listing_presenter import build_public_listing_details
from .public_inventory import PublicInventoryReader
from .transition_plan import TransitionPlan


TransitionChoiceKind = Literal[
    "appointment_date",
    "appointment_other_date",
    "appointment_mode",
    "listing_details",
    "home",
    "budget_choice",
    "budget_custom",
    "change_search",
    "search_area",
    "search_budget",
    "search_layout",
    "search_available",
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


def _appointment_view(
    plan: TransitionPlan,
    inventory: PublicInventoryReader,
    *,
    today: date,
) -> TransitionView:
    if plan.book is None:
        raise ValueError("book_transition_missing_public_draft")
    public_id = plan.book.draft.public_listing_id
    view = inventory.resolve(public_id)
    if view is None:
        raise ValueError("listing_not_publicly_published")
    if not view.bookable:
        raise ValueError("listing_not_bookable")
    details = build_public_listing_details(view)
    subject = details.subject or details.location or "这套房"
    price = _format_price(details.monthly_rent_usd)
    price_line = f"\n💰 <b>{he(price)}</b>" if price else ""
    heading = f"📅 <b>预约看房｜{he(public_id)}</b>"
    text = (
        f"{heading}\n\n"
        f"🏠 <b>{he(subject)}</b>{price_line}\n\n"
        "哪天方便看房？"
    )

    today_value = today.strftime("%m-%d")
    tomorrow_value = (today + timedelta(days=1)).strftime("%m-%d")
    after_value = (today + timedelta(days=2)).strftime("%m-%d")
    rows = (
        (
            TransitionChoice("今天", "appointment_date", today_value),
            TransitionChoice("明天", "appointment_date", tomorrow_value),
        ),
        (
            TransitionChoice("后天", "appointment_date", after_value),
            TransitionChoice("📅 其他日期", "appointment_other_date"),
        ),
        (TransitionChoice("🎥 改为视频看房", "appointment_mode", "video"),),
        (
            TransitionChoice(
                "⬅️ 返回房源",
                "listing_details",
                public_listing_id=public_id,
            ),
            TransitionChoice("🏠 返回首页", "home"),
        ),
    )
    return TransitionView(kind="appointment_date", text=text, rows=rows)


_BUDGET_OPTIONS = (
    ("b1", "$400以内", None, 400),
    ("b2", "$400–600", 400, 600),
    ("b3", "$600–800", 600, 800),
    ("b4", "$800–1200", 800, 1200),
    ("b5", "$1200–1500", 1200, 1500),
    ("b6", "$1500+", 1500, None),
)


def _similar_view(plan: TransitionPlan) -> TransitionView:
    if plan.similar is None:
        raise ValueError("similar_transition_missing_intent")
    intent = plan.similar.intent
    area_line = f"\n\n已选：{he(intent.area_display)}" if intent.area_display else ""
    text = f"💰 <b>每月预算大概多少？</b>{area_line}"
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
        (TransitionChoice("⬅️ 返回", "change_search"),),
    )
    return TransitionView(kind="search_budget", text=text, rows=rows)


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

    def build(
        self,
        plan: TransitionPlan,
        *,
        today: date | None = None,
    ) -> TransitionView:
        if plan.kind == "book":
            return _appointment_view(
                plan,
                self.inventory,
                today=today or date.today(),
            )
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
]
