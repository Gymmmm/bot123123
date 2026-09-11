"""Pure V3 free-text renter search action.

Explicit search-entry sessions keep the existing behavior. Outside that state,
plain text is claimed only when it clearly contains enough real searchable
rental criteria; unrelated chat remains untouched.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Mapping

from v3_core.inventory.listing_taxonomy import MARKET_LOCATIONS, PHYSICAL_AREAS

from .search_query import SearchCriteria, parse_search_criteria
from .transition_actions import LAST_SEARCH_PREF_KEY, SearchSubmitIntent
from .transition_session import (
    AWAITING_KEYWORD_SESSION_KEY,
    SEARCH_PREF_SESSION_KEY,
    SessionMutationPlan,
)


KeywordSearchStatus = Literal["ok", "invalid", "not_applicable"]


@dataclass(frozen=True)
class KeywordSearchActionResult:
    status: KeywordSearchStatus
    intent: SearchSubmitIntent | None = None
    mutation: SessionMutationPlan | None = None
    prompt: str = ""

    @property
    def ok(self) -> bool:
        return self.status == "ok" and self.intent is not None


def _location_display(criteria: SearchCriteria) -> str:
    if not criteria.location_keys:
        return ""
    displays = {item.key: item.display for item in (*MARKET_LOCATIONS, *PHYSICAL_AREAS)}
    values = [displays.get(key, key) for key in criteria.location_keys]
    return "/".join(dict.fromkeys(value for value in values if value))


def _budget_label(criteria: SearchCriteria) -> str:
    low, high = criteria.budget_min, criteria.budget_max
    if low is not None and high is not None:
        return f"{low}-{high} USD/月"
    if low is not None:
        return f">= {low} USD/月"
    if high is not None:
        return f"<= {high} USD/月"
    return ""


def _last_pref(criteria: SearchCriteria) -> dict[str, object]:
    # Keep the already-locked V3 history schema. Room type stays on the current
    # search criteria/touch payload and is not added to last_search_pref.
    return {
        "property_type": criteria.property_type,
        "location_keys": list(criteria.location_keys),
        "budget_min": criteria.budget_min,
        "budget_max": criteria.budget_max,
    }


def _looks_like_direct_search(criteria: SearchCriteria) -> bool:
    """Claim only text with enough currently-enforced rental filters.

    Room type is intentionally not used as the deciding filter here because the
    current published-inventory query does not strictly filter by room type.
    This prevents messages such as just "两房" from being presented as a precise
    room-type search when they are not.
    """
    has_location = bool(criteria.location_keys)
    has_budget = criteria.budget_min is not None or criteria.budget_max is not None
    has_property_type = bool(str(criteria.property_type or "").strip())

    # A location is a strong real-estate intent when accompanied by another
    # housing cue, including room type. Budget/property combinations are also
    # sufficiently specific. A lone number or casual room-type mention is not.
    if has_location and (has_budget or has_property_type or bool(criteria.room_type)):
        return True
    if has_budget and has_property_type:
        return True
    return False


class KeywordSearchActionService:
    def apply(
        self,
        text: object,
        session: Mapping[str, object],
    ) -> KeywordSearchActionResult:
        waiting = session.get(AWAITING_KEYWORD_SESSION_KEY)
        explicit_waiting = isinstance(waiting, Mapping)

        raw = str(text or "").strip()
        if not raw:
            if not explicit_waiting:
                return KeywordSearchActionResult(status="not_applicable")
            return KeywordSearchActionResult(
                status="invalid",
                prompt="发一句需求就可以，例如：<code>BKK1 预算800内 一房 安静</code>。",
            )

        criteria = parse_search_criteria(raw)
        if not explicit_waiting and not _looks_like_direct_search(criteria):
            return KeywordSearchActionResult(status="not_applicable")

        source = (
            str(waiting.get("source") or "smart_find_play").strip()
            if explicit_waiting
            else "direct_text"
        )
        intent = SearchSubmitIntent(
            criteria=criteria,
            source=source,
            goal="any",
            area_display=_location_display(criteria),
            budget_label=_budget_label(criteria),
            touch_payload={
                "message": raw,
                "room_type": criteria.room_type,
            },
        )
        return KeywordSearchActionResult(
            status="ok",
            intent=intent,
            mutation=SessionMutationPlan(
                set_values={LAST_SEARCH_PREF_KEY: _last_pref(criteria)},
                delete_keys=(AWAITING_KEYWORD_SESSION_KEY, SEARCH_PREF_SESSION_KEY),
            ),
        )


__all__ = ["KeywordSearchActionResult", "KeywordSearchActionService", "KeywordSearchStatus"]
