"""Telegram-neutral V3 home/contact/appointment views."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from .appointment_history import AppointmentHistoryView


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
    "🏠 <b>侨联地产｜金边华人房产服务</b>\n\n"
    "真实房源 · 实拍更新\n"
    "找房、看房、签约、入住后的事情，都可以从这里处理。\n\n"
    "在频道看到具体房源时，直接点房源按钮进入，房源信息会自动带上。\n\n"
    "请选择您现在要做的事："
)

ABOUT_TEXT = (
    "📖 <b>关于侨联地产</b>\n\n"
    "侨联地产面向在金边生活和找房的华人客户。\n\n"
    "我们不只是发房源，而是把找房、看房、费用确认、签约、入住和后续住房问题接在一起。\n\n"
    "• 房源先筛：按预算、区域、户型收窄选择\n"
    "• 费用先说：押付、水电、物业、网络等关键项提前核对\n"
    "• 入住留档：房屋现状、表计、家具家电按实际情况留档\n"
    "• 后续有人接：报修、物业、续租、换房、退租继续跟进\n\n"
    "侨联地产｜您在金边的自己人"
)

CONTACT_TEXT = (
    "💬 <b>联系顾问</b>\n\n"
    "直接告诉我们您现在需要处理什么。\n\n"
    "找房可发送：区域 + 预算 + 户型\n"
    "例如：BKK1 两房，$900以内。\n\n"
    "如果从具体房源进入，房源信息会自动带上，不用重复说明。"
)

BOOK_TEXT = (
    "📅 <b>预约看房</b>\n\n"
    "已经看中具体房源：从房源详情直接点「预约看房」，系统会自动带上房源。\n\n"
    "还没选好：先找房，或让顾问帮您收窄到 1–3 套再安排看房。"
)


def build_home_view(*, channel_url: str = "") -> HomeView:
    rows: list[tuple[HomeChoice, ...]] = [
        (HomeChoice("🔍 智能找房", "search"), HomeChoice("📖 关于侨联", "about")),
        (HomeChoice("📅 预约看房", "book"), HomeChoice("💬 联系顾问", "contact")),
        (HomeChoice("🛡 入住服务", "service"), HomeChoice("🧭 周边服务", "local")),
    ]
    clean_channel = str(channel_url or "").strip()
    if clean_channel:
        rows.append((HomeChoice("📢 房源频道", "root", url=clean_channel),))
    return HomeView(kind="home", text=WELCOME_TEXT, rows=tuple(rows))


def build_about_view() -> HomeView:
    return HomeView(
        kind="about",
        text=ABOUT_TEXT,
        rows=(
            (HomeChoice("🔍 智能找房", "search"), HomeChoice("💬 联系顾问", "contact")),
            (HomeChoice("🏠 返回首页", "root"),),
        ),
    )


def build_booking_view() -> HomeView:
    return HomeView(
        kind="book",
        text=BOOK_TEXT,
        rows=(
            (HomeChoice("🔍 先找房", "search"), HomeChoice("📅 我的预约", "appointments")),
            (HomeChoice("💬 联系顾问", "contact"),),
            (HomeChoice("🏠 返回首页", "root"),),
        ),
    )


def build_contact_view(*, advisor_url: str = "") -> HomeView:
    clean_advisor = str(advisor_url or "").strip()
    first = HomeChoice("💬 联系顾问", "contact", url=clean_advisor) if clean_advisor else HomeChoice("💬 联系顾问", "contact")
    return HomeView(
        kind="contact",
        text=CONTACT_TEXT,
        rows=(
            (first,),
            (HomeChoice("🔍 智能找房", "search"),),
            (HomeChoice("🏠 返回首页", "root"),),
        ),
    )


def build_appointment_history_home_view(history: AppointmentHistoryView) -> HomeView:
    return HomeView(
        kind="appointments",
        text=history.text,
        rows=(
            (HomeChoice("📅 预约看房", "book"), HomeChoice("🔍 继续找房", "search")),
            (HomeChoice("🏠 返回首页", "root"),),
        ),
    )


__all__ = [
    "ABOUT_TEXT",
    "BOOK_TEXT",
    "CONTACT_TEXT",
    "HomeChoice",
    "HomeChoiceKind",
    "HomeView",
    "WELCOME_TEXT",
    "build_about_view",
    "build_appointment_history_home_view",
    "build_booking_view",
    "build_contact_view",
    "build_home_view",
]
