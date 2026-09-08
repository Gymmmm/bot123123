"""Pure state transitions for ``v3u:t:*`` User Bot callbacks.

This service consumes one validated transition callback plus a snapshot of
Telegram user-session data. It never mutates the supplied mapping, calls
Telegram, queries SQLite, submits an appointment, writes a lead, or notifies an
admin. The result is an explicit public-only session mutation and, when a flow
reaches a boundary, an intent for a later executor.

Fixed-SHA semantics preserved here:
- appointment mode -> rerender date;
- appointment date -> time;
- appointment time -> ready to submit immediately;
- custom date/time -> explicit text-awaiting state;
- budget choice -> ready to search immediately;
- custom budget -> explicit text-awaiting state.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, Mapping

from .public_appointment import PublicAppointmentDraft
from .search_query import SearchCriteria, detect_property_type
from .transition_callbacks import TransitionCallback
from .transition_session import (
    APPOINTMENT_SESSION_KEY,
    AWAITING_KEYWORD_SESSION_KEY,
    SEARCH_CARD_ANCHOR_KEY,
    SEARCH_CARD_SESSION_KEY,
    SEARCH_PREF_SESSION_KEY,
    SessionMutationPlan,
)
from .transition_views import budget_bounds


APPOINTMENT_AWAITING_DATE_KEY = "v3_appointment_awaiting_custom_date"
APPOINTMENT_AWAITING_TIME_KEY = "v3_appointment_awaiting_custom_time"
SEARCH_AWAITING_BUDGET_KEY = "v3_awaiting_custom_budget"
LAST_SEARCH_PREF_KEY = "v3_last_search_pref"

TransitionActionStatus = Literal["ok", "expired", "invalid"]
TransitionNextStep = Literal[
    "appointment_date",
    "appointment_time",
    "appointment_custom_date",
    "appointment_custom_time",
    "appointment_submit",
    "search_custom_budget",
    "search_submit",
    "navigation",
]
TransitionNavigation = Literal[
    "home",
    "search_area",
    "search_budget",
    "search_layout",
    "search_available",
]


@dataclass(frozen=True)
class SearchSubmitIntent:
    criteria: SearchCriteria
    source: str
    goal: str
    area_display: str
    budget_label: str
    touch_payload: dict[str, Any]


@dataclass(frozen=True)
class TransitionActionResult:
    status: TransitionActionStatus
    next_step: TransitionNextStep | None = None
    reason: str = ""
    mutation: SessionMutationPlan | None = None
    appointment: PublicAppointmentDraft | None = None
    search: SearchSubmitIntent | None = None
    navigation: TransitionNavigation | None = None

    @property
    def ok(self) -> bool:
        return self.status == "ok"


def _appointment_values(draft: PublicAppointmentDraft) -> dict[str, Any]:
    return {
        "public_listing_id": draft.public_listing_id,
        "mode": draft.mode,
        "date": draft.date,
        "time": draft.time,
        "source": draft.source,
    }


def _load_appointment(session: Mapping[str, Any]) -> PublicAppointmentDraft | None:
    raw = session.get(APPOINTMENT_SESSION_KEY)
    if not isinstance(raw, Mapping):
        return None
    try:
        return PublicAppointmentDraft(
            public_listing_id=raw.get("public_listing_id", ""),
            mode=raw.get("mode", "offline"),
            date=str(raw.get("date") or ""),
            time=str(raw.get("time") or ""),
            source=str(raw.get("source") or "user_bot"),
        )
    except (TypeError, ValueError):
        return None


def _search_pref(session: Mapping[str, Any]) -> dict[str, Any] | None:
    raw = session.get(SEARCH_PREF_SESSION_KEY)
    if not isinstance(raw, Mapping):
        return None
    return dict(raw)


def _goal_property_type(goal: object) -> str:
    clean = str(goal or "any").strip()
    if clean in {"", "any", "住宅"}:
        return ""
    return detect_property_type(clean) or clean


def _home_mutation() -> SessionMutationPlan:
    return SessionMutationPlan(
        set_values={},
        delete_keys=(
            APPOINTMENT_SESSION_KEY,
            APPOINTMENT_AWAITING_DATE_KEY,
            APPOINTMENT_AWAITING_TIME_KEY,
            SEARCH_PREF_SESSION_KEY,
            SEARCH_AWAITING_BUDGET_KEY,
            AWAITING_KEYWORD_SESSION_KEY,
            SEARCH_CARD_SESSION_KEY,
            SEARCH_CARD_ANCHOR_KEY,
        ),
    )


class TransitionActionService:
    def apply(
        self,
        callback: TransitionCallback,
        session: Mapping[str, Any],
    ) -> TransitionActionResult:
        kind = str(callback.kind or "").strip()

        if kind in {
            "appointment_mode",
            "appointment_date",
            "appointment_other_date",
            "appointment_time",
            "appointment_other_time",
            "appointment_back_date",
        }:
            draft = _load_appointment(session)
            if draft is None:
                return TransitionActionResult(
                    status="expired",
                    reason="appointment_session_expired",
                )

            if kind == "appointment_mode":
                updated = draft.with_mode(callback.value)
                return TransitionActionResult(
                    status="ok",
                    next_step="appointment_date",
                    appointment=updated,
                    mutation=SessionMutationPlan(
                        set_values={APPOINTMENT_SESSION_KEY: _appointment_values(updated)},
                        delete_keys=(
                            APPOINTMENT_AWAITING_DATE_KEY,
                            APPOINTMENT_AWAITING_TIME_KEY,
                        ),
                    ),
                )

            if kind == "appointment_date":
                updated = draft.with_date(callback.value)
                return TransitionActionResult(
                    status="ok",
                    next_step="appointment_time",
                    appointment=updated,
                    mutation=SessionMutationPlan(
                        set_values={APPOINTMENT_SESSION_KEY: _appointment_values(updated)},
                        delete_keys=(
                            APPOINTMENT_AWAITING_DATE_KEY,
                            APPOINTMENT_AWAITING_TIME_KEY,
                        ),
                    ),
                )

            if kind == "appointment_other_date":
                return TransitionActionResult(
                    status="ok",
                    next_step="appointment_custom_date",
                    appointment=draft,
                    mutation=SessionMutationPlan(
                        set_values={APPOINTMENT_AWAITING_DATE_KEY: True},
                        delete_keys=(APPOINTMENT_AWAITING_TIME_KEY,),
                    ),
                )

            if kind == "appointment_time":
                try:
                    updated = draft.with_time(callback.value)
                except ValueError:
                    return TransitionActionResult(
                        status="invalid",
                        reason="appointment_date_required_before_time",
                    )
                return TransitionActionResult(
                    status="ok",
                    next_step="appointment_submit",
                    appointment=updated,
                    mutation=SessionMutationPlan(
                        set_values={APPOINTMENT_SESSION_KEY: _appointment_values(updated)},
                        delete_keys=(
                            APPOINTMENT_AWAITING_DATE_KEY,
                            APPOINTMENT_AWAITING_TIME_KEY,
                        ),
                    ),
                )

            if kind == "appointment_other_time":
                if not draft.date:
                    return TransitionActionResult(
                        status="invalid",
                        reason="appointment_date_required_before_time",
                    )
                return TransitionActionResult(
                    status="ok",
                    next_step="appointment_custom_time",
                    appointment=draft,
                    mutation=SessionMutationPlan(
                        set_values={APPOINTMENT_AWAITING_TIME_KEY: True},
                        delete_keys=(APPOINTMENT_AWAITING_DATE_KEY,),
                    ),
                )

            return TransitionActionResult(
                status="ok",
                next_step="appointment_date",
                appointment=draft,
                mutation=SessionMutationPlan(
                    set_values={},
                    delete_keys=(
                        APPOINTMENT_AWAITING_DATE_KEY,
                        APPOINTMENT_AWAITING_TIME_KEY,
                    ),
                ),
            )

        if kind == "budget_choice":
            pref = _search_pref(session)
            if pref is None:
                return TransitionActionResult(
                    status="expired",
                    reason="search_session_expired",
                )
            try:
                budget_label, budget_min, budget_max = budget_bounds(callback.value)
            except ValueError:
                return TransitionActionResult(
                    status="invalid",
                    reason="invalid_budget_choice",
                )
            goal = str(pref.get("goal") or "any").strip()
            location_keys = tuple(
                str(value).strip()
                for value in (pref.get("location_keys") or ())
                if str(value or "").strip()
            )
            property_type = _goal_property_type(goal)
            criteria = SearchCriteria(
                property_type=property_type,
                location_keys=location_keys,
                budget_min=budget_min,
                budget_max=budget_max,
                raw_text="",
            )
            source = str(pref.get("source") or "user_search").strip()
            area_display = str(pref.get("area_display") or "").strip()
            touch_payload = (
                dict(pref.get("touch_payload") or {})
                if isinstance(pref.get("touch_payload"), Mapping)
                else {}
            )
            return TransitionActionResult(
                status="ok",
                next_step="search_submit",
                search=SearchSubmitIntent(
                    criteria=criteria,
                    source=source,
                    goal=goal,
                    area_display=area_display,
                    budget_label=budget_label,
                    touch_payload=touch_payload,
                ),
                mutation=SessionMutationPlan(
                    set_values={
                        LAST_SEARCH_PREF_KEY: {
                            "property_type": property_type,
                            "location_keys": list(location_keys),
                            "budget_min": budget_min,
                            "budget_max": budget_max,
                        }
                    },
                    delete_keys=(SEARCH_PREF_SESSION_KEY, SEARCH_AWAITING_BUDGET_KEY),
                ),
            )

        if kind == "budget_custom":
            if _search_pref(session) is None:
                return TransitionActionResult(
                    status="expired",
                    reason="search_session_expired",
                )
            return TransitionActionResult(
                status="ok",
                next_step="search_custom_budget",
                mutation=SessionMutationPlan(
                    set_values={SEARCH_AWAITING_BUDGET_KEY: True},
                ),
            )

        if kind == "home":
            return TransitionActionResult(
                status="ok",
                next_step="navigation",
                navigation="home",
                mutation=_home_mutation(),
            )

        if kind in {"search_area", "search_budget", "search_layout", "search_available"}:
            return TransitionActionResult(
                status="ok",
                next_step="navigation",
                navigation=kind,  # type: ignore[arg-type]
                mutation=SessionMutationPlan(set_values={}),
            )

        return TransitionActionResult(
            status="invalid",
            reason="unsupported_transition_action",
        )


__all__ = [
    "APPOINTMENT_AWAITING_DATE_KEY",
    "APPOINTMENT_AWAITING_TIME_KEY",
    "LAST_SEARCH_PREF_KEY",
    "SEARCH_AWAITING_BUDGET_KEY",
    "SearchSubmitIntent",
    "TransitionActionResult",
    "TransitionActionService",
]
