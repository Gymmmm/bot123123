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
    "找房、预约、签约和入住后的住房服务，都可以从这里开始。\n\n"
    "如果已经在频道看到具体房源，直接从该房源进入，系统会继续带着同一套房源信息。\n\n"
    "请选择您现在需要的服务："
)

ABOUT_TEXT = (
    "🏠 <b>关于侨联</b>\n\n"
    "侨联地产面向在柬埔寨生活、找房和租房的中文客户。\n\n"
    "找房时把房源和费用说明白，看房、签约、入住按同一条流程留好记录；"
    "入住后遇到房屋、物业、续租或退租问题，也继续由中文服务衔接。\n\n"
    "<b>看对房 · 签约稳 · 入住顺</b>"
)

CONTACT_TEXT = (
    "💬 <b>中文顾问</b>\n\n"
    "直接发送您想咨询的问题即可。\n\n"
    "找房可以发送：<b>区域 + 预算 + 户型 + 入住时间</b>。\n"
    "如果从具体房源进入，系统会自动带上该房源信息，不需要重复说明。"
)

BOOK_TEXT = (
    "📅 <b>预约看房</b>\n\n"
    "请从具体房源详情进入预约，依次选择日期和时间后直接提交。\n\n"
    "提交后表示<b>预约申请已提交</b>，不代表时间已经确认；最终房态和看房时间由中文顾问确认。"
)


def build_home_view(*, channel_url: str = "", advisor_url: str = "") -> HomeView:
    advisor = advisor_handoff_url(advisor_url)
    rows: list[tuple[HomeChoice, ...]] = [
        (HomeChoice("🔍 开始找房", "search"), HomeChoice("📅 我的预约", "appointments")),
        (HomeChoice("🛠 入住服务", "service"), HomeChoice("📄 租赁服务", "rental")),
        (HomeChoice("💬 中文顾问", "contact", url=advisor) if advisor else HomeChoice("💬 中文顾问", "contact"),),
    ]
    channel = str(channel_url or "").strip()
    if channel:
        rows.append((HomeChoice("📢 房源频道", "root", url=channel),))
    rows.append((HomeChoice("🏠 关于侨联", "about"),))
    return HomeView("home", WELCOME_TEXT, tuple(rows))


def build_about_view(*, advisor_url: str = "") -> HomeView:
    advisor = advisor_handoff_url(advisor_url)
    return HomeView(
        "about",
        ABOUT_TEXT,
        (
            (HomeChoice("🔍 开始找房", "search"), HomeChoice("📄 租赁服务", "rental")),
            (HomeChoice("💬 中文顾问", "contact", url=advisor) if advisor else HomeChoice("💬 中文顾问", "contact"),),
            (HomeChoice("🏠 返回首页", "root"),),
        ),
    )


def build_booking_view(*, advisor_url: str = "") -> HomeView:
    advisor = advisor_handoff_url(advisor_url)
    return HomeView(
        "book",
        BOOK_TEXT,
        (
            (HomeChoice("🔍 开始找房", "search"), HomeChoice("📅 我的预约", "appointments")),
            (HomeChoice("💬 中文顾问", "contact", url=advisor) if advisor else HomeChoice("💬 中文顾问", "contact"),),
            (HomeChoice("🏠 返回首页", "root"),),
        ),
    )


def build_contact_view(*, advisor_url: str = "") -> HomeView:
    advisor = advisor_handoff_url(advisor_url)
    first = HomeChoice("💬 打开中文顾问", "contact", url=advisor) if advisor else HomeChoice("💬 中文顾问", "contact")
    return HomeView(
        "contact",
        CONTACT_TEXT,
        ((first,), (HomeChoice("🔍 开始找房", "search"),), (HomeChoice("🏠 返回首页", "root"),)),
    )


def build_appointment_history_home_view(history: AppointmentHistoryView) -> HomeView:
    return HomeView(
        "appointments",
        history.text,
        ((HomeChoice("🔍 继续找房", "search"),), (HomeChoice("🏠 返回首页", "root"),)),
    )


__all__ = [
    "ABOUT_TEXT", "BOOK_TEXT", "CONTACT_TEXT", "HomeChoice", "HomeChoiceKind",
    "HomeView", "WELCOME_TEXT", "build_about_view", "build_appointment_history_home_view",
    "build_booking_view", "build_contact_view", "build_home_view",
]
