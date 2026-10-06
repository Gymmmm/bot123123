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
        f"👋 {he(str(first_name))}，{he(str(greeting))}\n\n"
        "🏠 <b>侨联地产｜金边中文租房</b>\n\n"
        "📸 真实房源｜实拍更新\n"
        "🎥 视频带看｜没空到场，也能现场看房\n"
        "🛡️ 入住留档｜入住有记录，退租有依据\n"
        "🤝 租后服务｜签完合同，还有人继续跟\n\n"
        "请选择服务："
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
    "直接把问题发给我。\n"
    "找房可以说：区域、预算、几房、什么时候入住。"
)
BOOK_TEXT = (
    "📅 <b>预约看房</b>\n\n"
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
    rows: list[tuple[HomeChoice, ...]] = [
        (
            HomeChoice("🔍 开始找房", "search"),
            HomeChoice("🛎️ 侨联服务", "service"),
        ),
    ]
    clean_channel = str(channel_url or "").strip()
    if clean_channel:
        rows.append(
            (
                HomeChoice("📢 最新房源", "root", url=clean_channel),
                HomeChoice("💬 中文顾问", "contact"),
            )
        )
    else:
        rows.append((HomeChoice("💬 中文顾问", "contact"),))
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