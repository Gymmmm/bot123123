"""Compatibility-only routing for retired public User Bot inputs."""
from __future__ import annotations

from typing import Literal

LegacyHomeAction = Literal["home", "search", "book", "appointments", "about", "rental", "service", "local", "contact"]

_LEGACY_HOME_ACTIONS: dict[str, LegacyHomeAction] = {
    "hub:find": "search",
    "hub:precise": "search",
    "hub:appoint": "search",
    "hub:favorites": "search",
    "hub:appointments": "appointments",
    "hub:contract": "service",
    "hub:service": "rental",
    "hub:assurance": "rental",
    "hub:advisor": "contact",
    "hub:contact": "contact",
    "hub:help": "home",
    "home_smart_search": "search",
    "home_brand": "about",
    "home_appoint": "book",
    "home_consult": "contact",
    "home_living": "service",
    "home_nearby": "local",
    "home": "home",
    "service:hub": "service",
    "appointment_menu:list": "appointments",
    "menu_about": "about",
    "menu_human": "contact",
    "menu_service": "service",
    "appointment_menu:contact": "contact",
}

_LEGACY_REPLY_TEXT_ACTIONS: dict[str, LegacyHomeAction] = {
    "开始找房": "search",
    "帮我找房": "search",
    "精准筛选": "search",
    "预约看房": "search",
    "我的收藏": "search",
    "我的预约": "appointments",
    "我的租约": "service",
    "入住服务": "service",
    "售后服务": "rental",
    "服务保障": "rental",
    "租后服务": "rental",
    "联系顾问": "contact",
    "联系我们": "contact",
    "中文顾问": "contact",
    "使用说明": "home",
    "关于侨联": "rental",
    "首页": "home",
    "返回首页": "home",
}

_LEGACY_START_PAYLOADS: dict[LegacyHomeAction, str] = {
    "home": "",
    "search": "find_home",
    "book": "appointments",
    "appointments": "appointments",
    "about": "assurance",
    "rental": "assurance",
    "service": "service",
    "local": "service",
    "contact": "advisor",
}


def legacy_home_action(value: object) -> LegacyHomeAction | None:
    return _LEGACY_HOME_ACTIONS.get(str(value or "").strip().lower())


def legacy_reply_text_action(value: object) -> LegacyHomeAction | None:
    return _LEGACY_REPLY_TEXT_ACTIONS.get(str(value or "").strip())


def legacy_start_payload(action: LegacyHomeAction) -> str:
    return _LEGACY_START_PAYLOADS[action]


__all__ = [
    "LegacyHomeAction",
    "legacy_home_action",
    "legacy_reply_text_action",
    "legacy_start_payload",
]
