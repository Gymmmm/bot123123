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
    "<b>侨联地产｜金边中文租房服务</b>\n\n"
    "在金边找房，可以直接说区域、预算和户型。\n"
    "看中房源可以预约看房；租到房以后，住房方面有事也可以继续找侨联。"
)
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

def build_home_view(*, channel_url: str = "", advisor_url: str = "") -> HomeView:
    rows: list[tuple[HomeChoice, ...]] = [(HomeChoice("开始找房","search"),)]
    channel=str(channel_url or "").strip()
    second: list[HomeChoice]=[]
    if channel:
        second.append(HomeChoice("最新房源","root",url=channel))
    second.append(HomeChoice("中文顾问","contact"))
    rows.append(tuple(second))
    rows.append((HomeChoice("侨联服务","service"),))
    return HomeView("home",WELCOME_TEXT,tuple(rows))

def build_about_view(*, advisor_url: str = "") -> HomeView:
    return HomeView("about",ABOUT_TEXT,((HomeChoice("开始找房","search"),HomeChoice("中文顾问","contact")),(HomeChoice("回首页","root"),)))

def build_booking_view(*, advisor_url: str = "") -> HomeView:
    return HomeView("book",BOOK_TEXT,((HomeChoice("开始找房","search"),HomeChoice("我的预约","appointments")),(HomeChoice("中文顾问","contact"),),(HomeChoice("回首页","root"),)))

def build_contact_view(*, advisor_url: str = "") -> HomeView:
    advisor=advisor_handoff_url(advisor_url)
    first=HomeChoice("打开中文顾问","contact",url=advisor) if advisor else HomeChoice("中文顾问","contact")
    return HomeView("contact",CONTACT_TEXT,((first,),(HomeChoice("开始找房","search"),),(HomeChoice("回首页","root"),)))

def build_appointment_history_home_view(history: AppointmentHistoryView) -> HomeView:
    find_label="开始找房" if history.history_count == 0 and not history.items else "继续找房"
    return HomeView("appointments",history.text,((HomeChoice("联系中文顾问","contact"),HomeChoice(find_label,"search")),(HomeChoice("回首页","root"),)))

__all__=["ABOUT_TEXT","BOOK_TEXT","CONTACT_TEXT","HomeChoice","HomeChoiceKind","HomeView","WELCOME_TEXT","build_about_view","build_appointment_history_home_view","build_booking_view","build_contact_view","build_home_view"]
