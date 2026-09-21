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
        "🏠 <b>侨联地产｜金边中文租房</b>\n"
        "💬 中文顾问｜实地看房 / 视频代看\n\n"
        "📍 富力城｜炳发城｜BKK1｜钻石岛\n"
        "🛎️ 找房 · 看房 · 签约 · 入住 · 售后\n\n"
        "⭐ 金边本地6年经验\n"
        "📸 真实房源｜实拍更新\n"
        "📹 没时间到现场？可约视频实拍代看\n\n"
        "请选择服务："
    )


WELCOME_TEXT = welcome_text(first_name="您", greeting="您好")

ABOUT_TEXT = (
    "<b>安心租房</b>\n\n"
    "找房不只是看图片、签合同。看房、费用确认、入住交接留档和入住后的住房事项，都可以提前了解清楚。"
)
CONTACT_TEXT = (
    "<b>中文顾问</b>\n\n"
    "直接把问题发给我。\n"
    "找房可以说：区域、预算、几房、什么时候入住。"
)
BOOK_TEXT = (
    "<b>预约看房</b>\n\n"
    "请从具体房源进入预约，选择看房方式、日期和时间后提交。\n\n"
    "提交后表示预约信息已记录，不代表时间已经确认。"
)

def build_home_view(*, channel_url: str = "", advisor_url: str = "", first_name: str = "您", greeting: str = "您好") -> HomeView:
    channel=str(channel_url or "").strip()
    second: list[HomeChoice]=[]
    if channel:
        second.append(HomeChoice("📢 最新房源","root",url=channel))
    second.append(HomeChoice("💬 中文顾问","contact"))
    rows: list[tuple[HomeChoice, ...]] = [
        (HomeChoice("🔍 开始找房","search"), HomeChoice("🛎️ 侨联服务","service")),
        tuple(second),
    ]
    return HomeView("home",welcome_text(first_name=first_name, greeting=greeting),tuple(rows))

def build_about_view(*, advisor_url: str = "") -> HomeView:
    return HomeView("about",ABOUT_TEXT,((HomeChoice("开始找房","search"),HomeChoice("中文顾问","contact")),(HomeChoice("返回首页","root"),)))

def build_booking_view(*, advisor_url: str = "") -> HomeView:
    return HomeView("book",BOOK_TEXT,((HomeChoice("开始找房","search"),HomeChoice("我的预约","appointments")),(HomeChoice("中文顾问","contact"),),(HomeChoice("返回首页","root"),)))

def build_contact_view(*, advisor_url: str = "") -> HomeView:
    advisor=advisor_handoff_url(advisor_url)
    first=HomeChoice("中文顾问","contact",url=advisor) if advisor else HomeChoice("中文顾问","contact")
    return HomeView("contact",CONTACT_TEXT,((first,),(HomeChoice("开始找房","search"),),(HomeChoice("返回首页","root"),)))

def build_appointment_history_home_view(history: AppointmentHistoryView) -> HomeView:
    find_label="🔍 开始找房" if not history.items else "🔍 继续找房"
    return HomeView("appointments",history.text,((HomeChoice(find_label,"search"),HomeChoice("💬 中文顾问","contact")),(HomeChoice("⬅️ 返回首页","root"),)))

__all__=["ABOUT_TEXT","BOOK_TEXT","CONTACT_TEXT","HomeChoice","HomeChoiceKind","HomeView","WELCOME_TEXT","build_about_view","build_appointment_history_home_view","build_booking_view","build_contact_view","build_home_view","phnom_penh_greeting","welcome_text"]
