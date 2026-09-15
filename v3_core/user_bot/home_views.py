"""Telegram-neutral V3 home/contact/appointment views."""
from __future__ import annotations
from dataclasses import dataclass
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

WELCOME_TEXT = (
    "🏠 <b>侨联地产｜金边华人租房</b>\n\n"
    "告诉我你的需求，帮你更快找到合适的房子。\n\n"
    "也可以直接发送：<b>区域 + 预算 + 户型</b>"
)
ABOUT_TEXT = (
    "🏠 <b>关于侨联</b>\n\n"
    "侨联地产专注于柬埔寨本地房产与居住服务。\n\n"
    "我们做的不只是把房源发给你。找房时把信息说明白，看房和签约时把重要事项核对清楚，入住后遇到房屋、物业或租约问题，也继续有人衔接。\n\n"
    "从找房、签约到入住后的租住服务，尽量让每一步都有记录、有人跟进。\n\n"
    "侨联地产｜您在金边的自己人"
)
CONTACT_TEXT = (
    "💬 <b>中文顾问</b>\n\n"
    "直接发送你想咨询的问题即可。\n\n"
    "如果是找房，可以告诉我们：<b>区域 + 预算 + 户型 + 入住时间</b>。\n"
    "如果从具体房源进入，系统会自动带上房源信息，不用重复说明。"
)
BOOK_TEXT = (
    "📅 <b>预约看房</b>\n\n"
    "已经看中具体房源：请从房源详情点「预约看房」，系统会自动带上房源并进入日期、时间选择。\n\n"
    "提交后属于看房申请，最终房态和时间以顾问确认结果为准。"
)

def build_home_view(*, channel_url: str = "", advisor_url: str = "") -> HomeView:
    advisor = advisor_handoff_url(advisor_url)
    rows: list[tuple[HomeChoice, ...]] = [
        (HomeChoice("🔍 开始找房", "search"), HomeChoice("📅 我的预约", "appointments")),
        (HomeChoice("🛠 入住服务", "service"), HomeChoice("🏠 关于侨联", "about")),
        (HomeChoice("💬 中文顾问", "contact", url=advisor) if advisor else HomeChoice("💬 中文顾问", "contact"),),
    ]
    channel = str(channel_url or "").strip()
    if channel:
        rows.append((HomeChoice("📢 金边华人租房频道", "root", url=channel),))
    return HomeView("home", WELCOME_TEXT, tuple(rows))

def build_about_view(*, advisor_url: str = "") -> HomeView:
    advisor = advisor_handoff_url(advisor_url)
    return HomeView("about", ABOUT_TEXT, (
        (HomeChoice("💬 中文顾问", "contact", url=advisor) if advisor else HomeChoice("💬 中文顾问", "contact"),),
        (HomeChoice("🏠 返回首页", "root"),),
    ))

def build_booking_view(*, advisor_url: str = "") -> HomeView:
    advisor = advisor_handoff_url(advisor_url)
    return HomeView("book", BOOK_TEXT, (
        (HomeChoice("🔍 开始找房", "search"), HomeChoice("📅 我的预约", "appointments")),
        (HomeChoice("💬 中文顾问", "contact", url=advisor) if advisor else HomeChoice("💬 中文顾问", "contact"),),
        (HomeChoice("🏠 返回首页", "root"),),
    ))

def build_contact_view(*, advisor_url: str = "") -> HomeView:
    advisor = advisor_handoff_url(advisor_url)
    first = HomeChoice("💬 打开中文顾问", "contact", url=advisor) if advisor else HomeChoice("💬 中文顾问", "contact")
    return HomeView("contact", CONTACT_TEXT, ((first,), (HomeChoice("🔍 开始找房", "search"),), (HomeChoice("🏠 返回首页", "root"),)))

def build_appointment_history_home_view(history: AppointmentHistoryView) -> HomeView:
    return HomeView("appointments", history.text, ((HomeChoice("🔍 继续找房", "search"),), (HomeChoice("🏠 返回首页", "root"),)))

__all__ = ["ABOUT_TEXT","BOOK_TEXT","CONTACT_TEXT","HomeChoice","HomeChoiceKind","HomeView","WELCOME_TEXT","build_about_view","build_appointment_history_home_view","build_booking_view","build_contact_view","build_home_view"]
