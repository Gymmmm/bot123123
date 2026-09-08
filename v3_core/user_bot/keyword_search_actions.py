"""Pure V3 free-text search transition from an explicit keyword-awaiting state."""
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


class KeywordSearchActionService:
    def apply(
        self,
        text: object,
        session: Mapping[str, object],
    ) -> KeywordSearchActionResult:
        waiting = session.get(AWAITING_KEYWORD_SESSION_KEY)
        if not isinstance(waiting, Mapping):
            return KeywordSearchActionResult(status="not_applicable")

        raw = str(text or "").strip()
        if not raw:
            return KeywordSearchActionResult(
                status="invalid",
                prompt="发一句需求就可以，例如：<code>BKK1 预算800内 一房 安静</code>。",
            )

        criteria = parse_search_criteria(raw)
        source = str(waiting.get("source") or "smart_find_play").strip()
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
