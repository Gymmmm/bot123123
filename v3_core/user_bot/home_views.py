"""Telegram-neutral V3 home/contact/appointment views."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from .appointment_history import AppointmentHistoryView
from .telegram_navigation import advisor_handoff_url


HomeChoiceKind = Literal["root", "search", "appointments", "rental", "service", "contact"]


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
    "找房、约看房、入住服务，都可以从这里开始。\n\n"
    "频道看到具体房源，直接点房源下方按钮即可。\n\n"
    "也可以直接发送找房需求，例如：\n"
    "<code>BKK1，预算 $800以内</code>\n\n"
    "请选择您现在需要的服务："
)

CONTACT_TEXT = (
    "💬 <b>联系中文顾问</b>\n\n"
    "有什么需要，可以直接咨询。\n"
    "找房、看房、签约或入住后的事情都可以。\n\n"
    "如果是找房，也可以直接发送需求，例如：\n"
    "<code>BKK1，预算 $900以内</code>\n"
    "<code>富力城公寓，预算 $600–800</code>\n\n"
    "点击下方按钮可直接打开中文顾问对话。"
)


def _contact_choice(advisor_url: str = "") -> HomeChoice:
    clean = str(advisor_url or "").strip()
    return HomeChoice(
        "💬 联系中文顾问",
        "contact",
        url=advisor_handoff_url(clean) if clean else "",
    )


def build_home_view(*, channel_url: str = "", advisor_url: str = "") -> HomeView:
    rows: list[tuple[HomeChoice, ...]] = [
        (
            HomeChoice("🔍 帮我找房", "search"),
            HomeChoice("📅 我的预约", "appointments"),
        ),
        (
            HomeChoice("🛡 侨联保障", "rental"),
            HomeChoice("🛠 入住服务", "service"),
        ),
    ]
    clean_channel = str(channel_url or "").strip()
    contact = _contact_choice(advisor_url)
    if clean_channel:
        rows.append(
            (
                HomeChoice("🏠 最新房源", "root", url=clean_channel),
                contact,
            )
        )
    else:
        rows.append((contact,))
    return HomeView(kind="home", text=WELCOME_TEXT, rows=tuple(rows))


def build_contact_view(*, advisor_url: str = "") -> HomeView:
    first = _contact_choice(advisor_url)
    return HomeView(
        kind="contact",
        text=CONTACT_TEXT,
        rows=(
            (first,),
            (HomeChoice("🔍 帮我找房", "search"),),
            (HomeChoice("🏠 返回首页", "root"),),
        ),
    )


def build_appointment_history_home_view(
    history: AppointmentHistoryView,
    *,
    advisor_url: str = "",
) -> HomeView:
    rows: list[tuple[HomeChoice, ...]] = [
        (HomeChoice("🔍 继续找房", "search"),),
    ]
    clean_advisor = str(advisor_url or "").strip()
    if clean_advisor:
        rows.append((_contact_choice(clean_advisor),))
    rows.append((HomeChoice("🏠 返回首页", "root"),))
    return HomeView(
        kind="appointments",
        text=history.text,
        rows=tuple(rows),
    )


__all__ = [
    "CONTACT_TEXT",
    "HomeChoice",
    "HomeChoiceKind",
    "HomeView",
    "WELCOME_TEXT",
    "build_appointment_history_home_view",
    "build_contact_view",
    "build_home_view",
]
