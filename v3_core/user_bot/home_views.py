"""Telegram-neutral V3 home/contact/appointment views."""
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from html import escape as he
from zoneinfo import ZoneInfo
from typing import Literal
from .appointment_history import AppointmentHistoryView
from .telegram_navigation import advisor_handoff_url

HomeChoiceKind = Literal["root","search","book","appointments","about","rental","service","local","contact"]

@dataclass(frozen=True)
class HomeChoice:
    label: str
    kind: HomeChoiceKind
    url: str = ""
    callback_data: str = ""

@dataclass(frozen=True)
class HomeView:
    kind: str
    text: str
    rows: tuple[tuple[HomeChoice, ...], ...]

def phnom_penh_greeting() -> str:
    hour = datetime.now(ZoneInfo("Asia/Phnom_Penh")).hour
    if 5 <= hour < 12:
        return "上午好"
    if 12 <= hour < 18:
        return "下午好"
    return "晚上好"


def welcome_text(*, first_name: str, greeting: str) -> str:
    return (
        "🏠 <b>侨联地产｜金边中文选房</b>\n\n"
        "找房、看实拍、预约看房，都可以在这里完成。\n\n"
        "告诉我们区域、预算和户型，先帮你缩小范围。\n"
        "也可以查看最新房源，或直接联系中文顾问。"
    )


WELCOME_TEXT = welcome_text(first_name="您", greeting="您好")
ABOUT_TEXT = (
    "🏠 <b>侨联地产｜金边中文租房</b>\n\n"
    "中文顾问为你筛房、带看、确认费用，并继续跟进签约、入住和售后。\n\n"
    "✅ 真实房源｜实拍更新\n"
    "✅ 实地看房｜视频带看\n"
    "✅ 押付、水电、物业等费用提前确认\n"
    "✅ 入住后的报修和物业事项继续协助\n\n"
    "看房、费用确认、入住交接留档和入住后的住房事项，都可以继续找侨联。"
)
CONTACT_TEXT = (
    "💬 <b>中文顾问</b>\n\n"
    "请直接发送你的问题或找房需求。\n\n"
    "例如：BKK1、两房、$800以内、11月入住。\n"
    "如正在查看某套房源，相关信息会一并提供给顾问。"
)
BOOK_TEXT = (
    "📅 <b>在线预约</b>\n\n"
    "请从具体房源进入预约，选择看房方式、日期和时间后提交。\n\n"
    "提交后表示预约信息已记录，不代表时间已经确认。"
)

def build_home_view(
    *,
    channel_url: str = "",
    advisor_url: str = "",
    first_name: str = "您",
    greeting: str | None = None,
) -> HomeView:
    rows: list[tuple[HomeChoice, ...]] = [(HomeChoice("🔍 开始找房", "search"),)]
    clean_channel = str(channel_url or "").strip()
    contact = HomeChoice("💬 中文顾问", "contact")
    rows.append((HomeChoice("📢 最新房源", "root", url=clean_channel), contact) if clean_channel else (contact,))
    rows.append((HomeChoice("📋 我的预约", "appointments"), HomeChoice("🛎️ 侨联服务", "service")))
    resolved_greeting = str(greeting or "").strip() or phnom_penh_greeting()
    return HomeView(
        "home",
        welcome_text(first_name=first_name, greeting=resolved_greeting),
        tuple(rows),
    )

def build_about_view(*, advisor_url: str = "") -> HomeView:
    return HomeView(
        "about",
        ABOUT_TEXT,
        (
            (HomeChoice("🔍 开始找房", "search"), HomeChoice("💬 中文顾问", "contact")),
            (HomeChoice("⬅️ 返回首页", "root"),),
        ),
    )

def build_booking_view(*, advisor_url: str = "") -> HomeView:
    return HomeView(
        "book",
        BOOK_TEXT,
        (
            (HomeChoice("🔍 开始找房", "search"), HomeChoice("📋 我的预约", "appointments")),
            (HomeChoice("💬 中文顾问", "contact"),),
            (HomeChoice("⬅️ 返回首页", "root"),),
        ),
    )

def build_contact_view(*, advisor_url: str = "", search_summary: str = "") -> HomeView:
    advisor=advisor_handoff_url(advisor_url, listing_summary=search_summary)
    first=HomeChoice("💬 中文顾问","contact",url=advisor) if advisor else HomeChoice("💬 中文顾问","contact")
    return HomeView(
        "contact",
        CONTACT_TEXT,
        (
            (first,),
            (HomeChoice("🔍 开始找房", "search"),),
            (HomeChoice("⬅️ 返回首页", "root"),),
        ),
    )

def build_appointment_history_home_view(history: AppointmentHistoryView) -> HomeView:
    find_label="🔍 开始找房" if not history.items else "🔍 继续找房"
    rows = []
    if history.items:
        rows.append((HomeChoice("✏️ 管理预约", "appointments", callback_data="appointment_menu:details"),))
    rows.extend(((HomeChoice(find_label,"search"),HomeChoice("💬 中文顾问","contact")),(HomeChoice("⬅️ 返回首页","root"),)))
    return HomeView("appointments",history.text,tuple(rows))

__all__=["ABOUT_TEXT","BOOK_TEXT","CONTACT_TEXT","HomeChoice","HomeChoiceKind","HomeView","WELCOME_TEXT","build_about_view","build_appointment_history_home_view","build_booking_view","build_contact_view","build_home_view","phnom_penh_greeting","welcome_text"]