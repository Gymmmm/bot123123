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
        f"👋 <b>{he(str(first_name))}</b>，{he(str(greeting))}\n\n"
        "🏠 <b>侨联地产｜金边中文租房</b>\n\n"
        "📸 <b>真实房源</b>｜实拍更新，预约前确认房态\n"
        "🎥 <b>视频代看</b>｜人不到金边，也能先把房看清楚\n"
        "💬 <b>1V1中文顾问</b>｜从找房到入住，有事找同一个人\n"
        "🛡️ <b>侨联安心租</b>｜入住有记录，租后有人接\n\n"
        "请选择服务："
    )


WELCOME_TEXT = welcome_text(first_name="您", greeting="您好")
ABOUT_TEXT = (
    "🛡️ <b>侨联安心租</b>\n\n"
    "看房先确认，费用先说清，入住有记录，租后有人接。\n\n"
    "🎥 <b>视频代看</b>｜人不到金边，也能先把房看清楚\n"
    "💵 <b>费用先说清</b>｜租金、水电、物业、网络尽量提前确认\n"
    "🛡️ <b>入住留档</b>｜入住当天记清楚，退租少扯皮\n"
    "🤝 <b>租后管家</b>｜报修、物业、账单继续有人接"
)
CONTACT_TEXT = (
    "💬 <b>1V1中文顾问</b>\n\n"
    "从找房到入住，有事找同一个人。\n"
    "直接发：区域、预算、几房、入住时间；其他住房问题也可以直接说。"
)
BOOK_TEXT = (
    "📅 <b>在线预约看房</b>\n\n"
    "请从具体房源进入，选择顾问陪看或视频代看，再选日期和时间。\n\n"
    "提交后表示预约信息已记录，顾问确认后才算预约成功。"
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
            HomeChoice("🔍 1V1定制找房", "search"),
            HomeChoice("🛎️ 侨联服务", "service"),
        ),
    ]
    clean_channel = str(channel_url or "").strip()
    if clean_channel:
        rows.append(
            (
                HomeChoice("📢 最新房源", "root", url=clean_channel),
                HomeChoice("💬 1V1中文顾问", "contact"),
            )
        )
    else:
        rows.append((HomeChoice("💬 1V1中文顾问", "contact"),))
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
            (HomeChoice("🔍 1V1定制找房", "search"), HomeChoice("💬 1V1中文顾问", "contact")),
            (HomeChoice("🏠 返回首页", "root"),),
        ),
    )

def build_booking_view(*, advisor_url: str = "") -> HomeView:
    return HomeView(
        "book",
        BOOK_TEXT,
        (
            (HomeChoice("🔍 1V1定制找房", "search"), HomeChoice("📅 我的预约", "appointments")),
            (HomeChoice("💬 1V1中文顾问", "contact"),),
            (HomeChoice("🏠 返回首页", "root"),),
        ),
    )

def build_contact_view(*, advisor_url: str = "") -> HomeView:
    advisor=advisor_handoff_url(advisor_url)
    first=HomeChoice("💬 1V1中文顾问","contact",url=advisor) if advisor else HomeChoice("💬 1V1中文顾问","contact")
    return HomeView(
        "contact",
        CONTACT_TEXT,
        (
            (first,),
            (HomeChoice("🔍 1V1定制找房", "search"),),
            (HomeChoice("🏠 返回首页", "root"),),
        ),
    )

def build_appointment_history_home_view(history: AppointmentHistoryView) -> HomeView:
    find_label="🔍 1V1定制找房" if not history.items else "🔍 继续找房"
    return HomeView("appointments",history.text,((HomeChoice(find_label,"search"),HomeChoice("💬 1V1中文顾问","contact")),(HomeChoice("🏠 返回首页","root"),)))

__all__=["ABOUT_TEXT","BOOK_TEXT","CONTACT_TEXT","HomeChoice","HomeChoiceKind","HomeView","WELCOME_TEXT","build_about_view","build_appointment_history_home_view","build_booking_view","build_contact_view","build_home_view","phnom_penh_greeting","welcome_text"]