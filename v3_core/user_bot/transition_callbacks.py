"""Explicit callback codec for side-effect-free V3 transition views.

Transition views never emit fixed-SHA callback namespaces such as ``apdate:``
or ``findbudget:``. Choices that already have a canonical V3 callback reuse it
(listing details and change-search); the remaining guided-flow choices live
under the compact ``v3u:t:`` namespace.

This module is transport-only: it does not read session state, query inventory,
or execute appointment/search/lead effects.
"""
from __future__ import annotations

from dataclasses import dataclass
import re

from .callbacks import (
    PREFIX,
    encode_change_search_callback,
    encode_listing_callback,
)
from .home_callbacks import encode_home_callback
from .search_navigation import AREA_OPTIONS, LAYOUT_OPTIONS
from .transition_views import TransitionChoice, TransitionChoiceKind


TRANSITION_PREFIX = f"{PREFIX}:t"

_VALUE_KINDS = frozenset(
    {
        "appointment_date",
        "appointment_mode",
        "appointment_time",
        "budget_choice",
        "area_choice",
        "layout_choice",
    }
)
_FLAG_KINDS = frozenset(
    {
        "appointment_other_date",
        "appointment_other_time",
        "appointment_back_date",
        "home",
        "budget_custom",
        "search_area",
        "search_budget",
        "search_layout",
        "search_available",
        "area_other",
    }
)
_BUDGET_CODES = frozenset({"b1", "b2", "b3", "b4", "b5", "b6"})
_AREA_CODES = frozenset(code for code, _ in AREA_OPTIONS)
_LAYOUT_CODES = frozenset(code for code, _ in LAYOUT_OPTIONS)
_MODES = frozenset({"offline", "video"})
_TIMES = frozenset({"am", "pm", "evening"})
_DATE_RE = re.compile(r"^\d{2}-\d{2}$")


@dataclass(frozen=True)
class TransitionCallback:
    kind: TransitionChoiceKind
    value: str = ""


def _validate_value(kind: str, value: object) -> str:
    clean = str(value or "").strip()
    if kind == "appointment_date":
        if not _DATE_RE.fullmatch(clean):
            raise ValueError("invalid_transition_appointment_date")
        month, day = (int(part) for part in clean.split("-", 1))
        if not (1 <= month <= 12 and 1 <= day <= 31):
            raise ValueError("invalid_transition_appointment_date")
        return clean
    if kind == "appointment_mode":
        if clean not in _MODES:
            raise ValueError("invalid_transition_appointment_mode")
        return clean
    if kind == "appointment_time":
        if clean not in _TIMES:
            raise ValueError("invalid_transition_appointment_time")
        return clean
    if kind == "budget_choice":
        if clean not in _BUDGET_CODES:
            raise ValueError("invalid_transition_budget_choice")
        return clean
    if kind == "area_choice":
        if clean not in _AREA_CODES:
            raise ValueError("invalid_transition_area_choice")
        return clean
    if kind == "layout_choice":
        if clean not in _LAYOUT_CODES:
            raise ValueError("invalid_transition_layout_choice")
        return clean
    raise ValueError("transition_callback_kind_has_no_value")


def encode_transition_choice(choice: TransitionChoice) -> str:
    """Encode one transition choice without reviving legacy callback formats."""
    kind = str(choice.kind or "").strip()
    if kind == "listing_details":
        return encode_listing_callback("details", choice.public_listing_id)
    if kind == "change_search":
        return encode_change_search_callback()
    if kind == "contact":
        return encode_home_callback("contact")
    if kind in _VALUE_KINDS:
        value = _validate_value(kind, choice.value)
        return f"{TRANSITION_PREFIX}:{kind}:{value}"
    if kind in _FLAG_KINDS:
        if str(choice.value or "").strip():
            raise ValueError("flag_transition_callback_must_not_have_value")
        return f"{TRANSITION_PREFIX}:{kind}"
    raise ValueError("unsupported_transition_choice")


def parse_transition_callback(value: object) -> TransitionCallback | None:
    raw = str(value or "").strip()
    prefix = f"{TRANSITION_PREFIX}:"
    if not raw.startswith(prefix):
        return None
    parts = raw.split(":")
    if len(parts) == 3:
        kind = parts[2].strip()
        if kind not in _FLAG_KINDS:
            return None
        return TransitionCallback(kind=kind)  # type: ignore[arg-type]
    if len(parts) == 4:
        kind = parts[2].strip()
        if kind not in _VALUE_KINDS:
            return None
        try:
            clean = _validate_value(kind, parts[3])
        except ValueError:
            return None
        return TransitionCallback(kind=kind, value=clean)  # type: ignore[arg-type]
    return None


__all__ = [
    "TRANSITION_PREFIX",
    "TransitionCallback",
    "encode_transition_choice",
    "parse_transition_callback",
]
