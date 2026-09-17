"""Compatibility-only routing for retired public User Bot callbacks."""
from __future__ import annotations

from typing import Literal

LegacyHomeAction = Literal["home", "search", "appointments", "rental", "service", "contact"]

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
    "home_brand": "rental",
    "menu_about": "rental",
    "menu_human": "contact",
    "menu_service": "service",
    "appointment_menu:contact": "contact",
}


def legacy_home_action(value: object) -> LegacyHomeAction | None:
    return _LEGACY_HOME_ACTIONS.get(str(value or "").strip().lower())


__all__ = ["LegacyHomeAction", "legacy_home_action"]
