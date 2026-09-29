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
        "📸 <b>真实房源</b>｜实拍更新\n"
        "📹 <b>视频带看</b>｜没空到场，也能现场看房\n"
        "🛡️ <b>入住留档</b>｜入住有记录，退租有依据\n"
        "🤝 <b>租后服务</b>｜签完合同，还有人继续跟\n\n"
        "请选择服务："
    )


WELCOME_TEXT = welcome_text(first_name="您", greeting="您好")
ABOUT_TEXT = (
    "🛡️ <b>侨联安心租</b>\n\n"
    "不是只帮你找到房，重要环节都有人继续跟。\n\n"
    "📸 <b>真实房源</b>｜实拍更新，房态预约前确认\n"
    "📹 <b>视频带看</b>｜没空到场，顾问到现场实时视频\n"
    "💵 <b>费用透明</b>｜租金之外，水电物业等尽量提前说清\n"
    "🛡️ <b>入住留档</b>｜入住有记录，退租有依据\n"
    "🤝 <b>租后服务</b>｜签完合同，有问题继续找侨联"
)
CONTACT_TEXT = (
    "💬 <b>中文顾问</b>\n\n"
    "直接把问题发给我。\n"
    "找房可以说：区域、预算、几房、什么时候入住。"
)
BOOK_TEXT = (
    "📅 <b>在线预约看房</b>\n\n"
    "请从具体房源进入，选择实地带看或视频带看，再选日期和时间。\n\n"
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
            HomeChoice("🔍 帮我找房", "search"),
            HomeChoice("🛎️ 侨联服务", "service"),
        ),
    ]
    clean_channel = str(channel_url or "").strip()
    if clean_channel:
        rows.append(
            (
                HomeChoice("📢 最新房源", "root", url=clean_channel),
                HomeChoice("💬 联系中文顾问", "contact"),
            )
        )
    else:
        rows.append((HomeChoice("💬 联系中文顾问", "contact"),))
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
            (HomeChoice("🔍 帮我找房", "search"), HomeChoice("💬 联系中文顾问", "contact")),
            (HomeChoice("🏠 返回首页", "root"),),
        ),
    )

def build_booking_view(*, advisor_url: str = "") -> HomeView:
    return HomeView(
        "book",
        BOOK_TEXT,
        (
            (HomeChoice("🔍 帮我找房", "search"), HomeChoice("📅 我的预约", "appointments")),
            (HomeChoice("💬 联系中文顾问", "contact"),),
            (HomeChoice("🏠 返回首页", "root"),),
        ),
    )

def build_contact_view(*, advisor_url: str = "") -> HomeView:
    advisor=advisor_handoff_url(advisor_url)
    first=HomeChoice("💬 联系中文顾问","contact",url=advisor) if advisor else HomeChoice("💬 联系中文顾问","contact")
    return HomeView(
        "contact",
        CONTACT_TEXT,
        (
            (first,),
            (HomeChoice("🔍 帮我找房", "search"),),
            (HomeChoice("🏠 返回首页", "root"),),
        ),
    )

def build_appointment_history_home_view(history: AppointmentHistoryView) -> HomeView:
    find_label="🔍 帮我找房" if not history.items else "🔍 继续找房"
    return HomeView("appointments",history.text,((HomeChoice(find_label,"search"),HomeChoice("💬 联系中文顾问","contact")),(HomeChoice("🏠 返回首页","root"),)))

__all__=["ABOUT_TEXT","BOOK_TEXT","CONTACT_TEXT","HomeChoice","HomeChoiceKind","HomeView","WELCOME_TEXT","build_about_view","build_appointment_history_home_view","build_booking_view","build_contact_view","build_home_view","phnom_penh_greeting","welcome_text"]