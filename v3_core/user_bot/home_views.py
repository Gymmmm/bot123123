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
    "从找房、看房到签约入住，后续住房问题也有人跟进。\n\n"
    "找房可以直接告诉我：<b>区域 + 预算 + 户型</b>\n\n"
    "👇 请选择您现在需要的服务"
)

ABOUT_TEXT = (
    "📖 <b>关于侨联地产</b>\n\n"
    "侨联地产面向在金边找房、租房和生活的华人客户。\n\n"
    "我们把找房、看房、签约和入住后的住房服务接在同一条流程里：\n\n"
    "• <b>真实房源</b>｜实拍更新，先按预算、区域、户型筛选\n"
    "• <b>费用确认</b>｜押付、水电、物业等关键费用签约前说清楚\n"
    "• <b>入住留档</b>｜房屋现状、表计、家具家电按实际情况确认\n"
    "• <b>租期服务</b>｜报修、物业、续租、退租继续跟进\n\n"
    "侨联地产｜您在金边的自己人"
)

CONTACT_TEXT = (
    "💬 <b>顾问帮我找</b>\n\n"
    "直接告诉顾问您的找房要求即可。\n\n"
    "建议发送：<b>区域 + 预算 + 户型</b>\n"
    "例如：<code>BKK1 两房，$900以内</code>。\n\n"
    "如果是从具体房源进入，系统会自动带上房源信息，不用重复说明。"
)

BOOK_TEXT = (
    "📅 <b>预约看房</b>\n\n"
    "已经看中具体房源：请从房源详情点「预约看房」，系统会自动带上房源并进入日期、时间选择。\n\n"
    "还没确定房源：建议先找房，筛到合适的 1–3 套后再统一安排看房。"
)


def build_home_view(*, channel_url: str = "") -> HomeView:
    rows: list[tuple[HomeChoice, ...]] = [
        (HomeChoice("🔍 智能找房", "search"), HomeChoice("💬 顾问帮我找", "contact")),
    ]
    clean_channel = str(channel_url or "").strip()
    if clean_channel:
        rows.append(
            (HomeChoice("🏠 最新房源", "root", url=clean_channel), HomeChoice("🛡 入住服务", "service"))
        )
    else:
        rows.append((HomeChoice("🛡 入住服务", "service"),))
    rows.append((HomeChoice("📖 关于侨联", "about"),))
    return HomeView(kind="home", text=WELCOME_TEXT, rows=tuple(rows))


def build_about_view() -> HomeView:
    return HomeView(
        kind="about",
        text=ABOUT_TEXT,
        rows=(
            (HomeChoice("🔍 智能找房", "search"), HomeChoice("💬 顾问帮我找", "contact")),
            (HomeChoice("🏠 返回首页", "root"),),
        ),
    )


def build_booking_view() -> HomeView:
    return HomeView(
        kind="book",
        text=BOOK_TEXT,
        rows=(
            (HomeChoice("🔍 先找房", "search"), HomeChoice("📅 我的预约", "appointments")),
            (HomeChoice("💬 顾问帮我找", "contact"),),
            (HomeChoice("🏠 返回首页", "root"),),
        ),
    )


def build_contact_view(*, advisor_url: str = "") -> HomeView:
    clean_advisor = str(advisor_url or "").strip()
    first = HomeChoice("💬 打开顾问对话", "contact", url=clean_advisor) if clean_advisor else HomeChoice("💬 顾问帮我找", "contact")
    return HomeView(
        kind="contact",
        text=CONTACT_TEXT,
        rows=(
            (first,),
            (HomeChoice("🔍 继续找房", "search"),),
            (HomeChoice("🏠 返回首页", "root"),),
        ),
    )


def build_appointment_history_home_view(history: AppointmentHistoryView) -> HomeView:
    return HomeView(
        kind="appointments",
        text=history.text,
        rows=(
            (HomeChoice("🔍 继续找房", "search"),),
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
