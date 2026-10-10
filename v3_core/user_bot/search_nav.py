"""Per-user navigation history for the guided search flow (V4.1).

The guided-search preference carries a tiny stack of the filter pages the
user actually visited (``nav``). 「⬅️ 返回上一步」pops that stack, so the back
button follows real history: budget reached from the area page goes back to
the area page; budget opened straight from the search entry goes back to the
entry. Selected filters always stay in the preference.
"""
from __future__ import annotations

from typing import Any, Mapping

NAV_KEY = "nav"
STEP_AREA = "search_area"
STEP_BUDGET = "search_budget"
STEP_LAYOUT = "search_layout"
STEP_RESULTS = "results"  # the no-match/results page that opened an adjust step
_STEPS = frozenset({STEP_AREA, STEP_BUDGET, STEP_LAYOUT, STEP_RESULTS})
_MAX_DEPTH = 8


def nav_stack(pref: Mapping[str, Any] | None) -> list[str]:
    if not isinstance(pref, Mapping):
        return []
    raw = pref.get(NAV_KEY)
    if not isinstance(raw, (list, tuple)):
        return []
    return [str(step) for step in raw if str(step) in _STEPS]


def push_step(pref: Mapping[str, Any], step: str) -> dict[str, Any]:
    """Return a copy of ``pref`` with ``step`` recorded as the current page."""
    updated = dict(pref)
    stack = nav_stack(pref)
    if step in _STEPS and (not stack or stack[-1] != step):
        stack.append(step)
    updated[NAV_KEY] = stack[-_MAX_DEPTH:]
    return updated


def pop_step(pref: Mapping[str, Any] | None) -> tuple[str, dict[str, Any]]:
    """Leave the current page. Returns ``(target, updated_pref)``.

    ``target`` is the previous page (one of the step names), or ``"entry"``
    when the user started from the search entry.
    """
    updated = dict(pref or {})
    stack = nav_stack(pref)
    if stack:
        stack.pop()  # the page the user is leaving
    updated[NAV_KEY] = stack
    return (stack[-1] if stack else "entry"), updated


__all__ = [
    "NAV_KEY", "STEP_AREA", "STEP_BUDGET", "STEP_LAYOUT", "STEP_RESULTS",
    "nav_stack", "pop_step", "push_step",
]
