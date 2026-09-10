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
    "如果您已经在频道看到具体房源，直接点房源下方按钮进入，房源信息会自动带上。\n\n"
    "也可以直接告诉我：<b>区域 + 预算 + 户型</b>。\n\n"
    "请选择您现在需要的服务："
)

CONTACT_TEXT = (
    "💬 <b>联系中文顾问</b>\n\n"
    "找房可以直接发送：区域 + 预算 + 户型。\n\n"
    "例如：\n"
    "「BKK1 两房，$900以内」\n"
    "「富力城一房，要能做饭」\n\n"
    "也可以点击下方直接打开顾问对话。"
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
    clean_advisor = str(advisor_url or "").strip()
    first = _contact_choice(clean_advisor)
    return HomeView(
        kind="contact",
        text=CONTACT_TEXT,
        rows=(
            (first,),
            (HomeChoice("🔍 帮我找房", "search"),),
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
    "CONTACT_TEXT",
    "HomeChoice",
    "HomeChoiceKind",
    "HomeView",
    "WELCOME_TEXT",
    "build_appointment_history_home_view",
    "build_contact_view",
    "build_home_view",
]
