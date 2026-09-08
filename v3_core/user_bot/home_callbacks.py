"""V3-only callback protocol for User Bot home surfaces."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from .callbacks import PREFIX


HomeAction = Literal["search", "appointments", "rental", "service", "contact"]
HOME_PREFIX = f"{PREFIX}:home"
_ACTIONS = frozenset({"search", "appointments", "rental", "service", "contact"})


@dataclass(frozen=True)
class HomeCallback:
    action: HomeAction


def encode_home_callback(action: str) -> str:
    clean = str(action or "").strip().lower()
    if clean not in _ACTIONS:
        raise ValueError("unsupported_home_action")
    return f"{HOME_PREFIX}:{clean}"


def parse_home_callback(value: object) -> HomeCallback | None:
    raw = str(value or "").strip()
    prefix = f"{HOME_PREFIX}:"
    if not raw.startswith(prefix):
        return None
    action = raw[len(prefix):].strip().lower()
    if action not in _ACTIONS or ":" in action:
        return None
    return HomeCallback(action=action)  # type: ignore[arg-type]


__all__ = ["HOME_PREFIX", "HomeAction", "HomeCallback", "encode_home_callback", "parse_home_callback"]
