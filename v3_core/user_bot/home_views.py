"""Telegram-neutral V3 home/contact/appointment views."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from .appointment_history import AppointmentHistoryView
from .telegram_navigation import advisor_handoff_url

HomeChoiceKind = Literal[
    "root", "search", "book", "appointments", "about", "rental", "service", "local", "contact"
]


@dataclass(frozen=True)
class HomeChoice:
    label: str
    kind: HomeChoiceKind
    url: str = ""


@dataclass(frozen=True)
class HomeView:
    kind: str
    text: str
    rows: tuple[tuple[HomeChoice, ...], ...]


WELCOME_TEXT = (
    "💎 <b>侨联地产｜您在金边的自己人</b>\n\n"
    "找房、预约看房，都可以从这里开始。\n"
    "租到以后，住房的事情也还能找侨联。"
)

ABOUT_TEXT = (
    "🛡 <b>租到房，不代表服务就结束了。</b>\n\n"
    "找房只是开始。住进去以后的事情，侨联也接着管。\n\n"
    "签约前　核对租金、押金和相关费用\n"
    "入住时　房屋、表计、家具家电拍照留档\n"
    "入住后　报修或物业沟通，可以找侨联\n"
    "退租时　按入住记录协助逐项核对\n\n"
    "从找房到住进去以后，有需要都可以找侨联。"
)

CONTACT_TEXT = (
    "💬 <b>中文顾问</b>\n\n"
    "直接把问题发给我。\n"
    "找房可以说：区域、预算、几房、什么时候入住。"
)

BOOK_TEXT = (
    "📅 <b>预约看房</b>\n\n"
    "请从具体房源详情进入预约，依次选择日期和时间后提交。\n\n"
    "提交后表示<b>预约申请已提交</b>，不代表时间已经确认。"
)


def build_home_view(*, channel_url: str = "", advisor_url: str = "") -> HomeView:
    rows: list[tuple[HomeChoice, ...]] = [
        (HomeChoice("🔍 开始找房", "search"), HomeChoice("📅 我的预约", "appointments")),
        (HomeChoice("🛠 入住服务", "service"), HomeChoice("🛡 租后服务", "rental")),
        (HomeChoice("💬 中文顾问", "contact"),),
    ]
    channel = str(channel_url or "").strip()
    if channel:
        rows.append((HomeChoice("📢 最新房源", "root", url=channel),))
    return HomeView("home", WELCOME_TEXT, tuple(rows))


def build_about_view(*, advisor_url: str = "") -> HomeView:
    return HomeView(
        "about",
        ABOUT_TEXT,
        (
            (HomeChoice("📋 入住交接留档", "rental"),),
            (HomeChoice("🔍 开始找房", "search"), HomeChoice("💬 中文顾问", "contact")),
            (HomeChoice("⬅️ 回首页", "root"),),
        ),
    )


def build_booking_view(*, advisor_url: str = "") -> HomeView:
    return HomeView(
        "book",
        BOOK_TEXT,
        (
            (HomeChoice("🔍 开始找房", "search"), HomeChoice("📅 我的预约", "appointments")),
            (HomeChoice("💬 中文顾问", "contact"),),
            (HomeChoice("⬅️ 回首页", "root"),),
        ),
    )


def build_contact_view(*, advisor_url: str = "") -> HomeView:
    advisor = advisor_handoff_url(advisor_url)
    first = HomeChoice("💬 打开中文顾问", "contact", url=advisor) if advisor else HomeChoice("💬 中文顾问", "contact")
    return HomeView(
        "contact",
        CONTACT_TEXT,
        ((first,), (HomeChoice("🔍 开始找房", "search"),), (HomeChoice("⬅️ 回首页", "root"),)),
    )


def build_appointment_history_home_view(history: AppointmentHistoryView) -> HomeView:
    find_label = "🔍 去找房" if history.history_count == 0 and not history.items else "🔍 继续找房"
    return HomeView(
        "appointments",
        history.text,
        ((HomeChoice(find_label, "search"),), (HomeChoice("⬅️ 回首页", "root"),)),
    )


__all__ = [
    "ABOUT_TEXT", "BOOK_TEXT", "CONTACT_TEXT", "HomeChoice", "HomeChoiceKind",
    "HomeView", "WELCOME_TEXT", "build_about_view", "build_appointment_history_home_view",
    "build_booking_view", "build_contact_view", "build_home_view",
]
