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
- area -> budget;
- layout -> strict search boundary;
- current available -> published-only search boundary;
- budget choice -> ready to search immediately;
- custom budget -> explicit text-awaiting state.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, Mapping

from .public_appointment import PublicAppointmentDraft
from .search_navigation import area_selection, layout_selection
from .search_query import SearchCriteria, detect_property_type
from .transition_callbacks import TransitionCallback
from .search_nav import STEP_RESULTS, pop_step, push_step
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
SEARCH_AWAITING_AREA_KEY = "v3_awaiting_custom_area"
SEARCH_AWAITING_BUDGET_KEY = "v3_awaiting_custom_budget"
LAST_SEARCH_PREF_KEY = "v3_last_search_pref"

TransitionActionStatus = Literal["ok", "expired", "invalid"]
TransitionNextStep = Literal[
    "appointment_date",
    "appointment_time",
    "appointment_custom_date",
    "appointment_custom_time",
    "appointment_submit",
    "search_custom_area",
    "search_custom_budget",
    "search_submit",
    "navigation",
]
TransitionNavigation = Literal[
    "home",
    "search_entry",
    "search_area",
    "search_budget",
    "search_layout",
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


def _fresh_search_pref(source: str) -> dict[str, Any]:
    return {
        "source": str(source or "user_search"),
        "goal": "any",
        "location_keys": [],
        "area_display": "",
        "touch_payload": {},
    }


def _entry_pref(session: Mapping[str, Any], step: str) -> dict[str, Any]:
    """Open a filter page from the search entry.

    An in-progress guided search keeps its already selected filters (the user
    went back to the entry and picked another filter); otherwise start fresh.
    The navigation history restarts at this page.
    """
    pref = _search_pref(session)
    if pref is None or str(pref.get("source") or "") == "similar_listing":
        pref = _fresh_search_pref(f"home_{step.split('_', 1)[1]}")
    pref.pop("resume", None)
    pref["nav"] = []
    return push_step(pref, step)


def _last_pref(
    criteria: SearchCriteria,
    *,
    area_display: str = "",
    budget_label: str = "",
    layout_label: str = "",
) -> dict[str, Any]:
    return {
        "property_type": criteria.property_type,
        "location_keys": list(criteria.location_keys),
        "budget_min": criteria.budget_min,
        "budget_max": criteria.budget_max,
        "room_type": criteria.room_type,
        "area_display": str(area_display or ""),
        "budget_label": str(budget_label or ""),
        "layout_label": str(layout_label or ""),
    }


_ADJUST_NAVIGATION = {
    "adjust_area": "search_area",
    "adjust_budget": "search_budget",
    "adjust_layout": "search_layout",
}


def _adjust_pref(session: Mapping[str, Any], kind: str) -> dict[str, Any]:
    """Seed a guided-search preference from the last submitted search.

    Used by the no-match page: the user changes one condition and the others
    stay as they were (V4 「返回保留筛选状态」). Without a previous search the
    flow degrades to a fresh guided search instead of expiring.
    """
    pref = _fresh_search_pref(f"home_{kind.split('_', 1)[1]}")
    last = session.get(LAST_SEARCH_PREF_KEY)
    if not isinstance(last, Mapping):
        return pref
    pref["resume"] = True
    pref["nav"] = [STEP_RESULTS]
    if kind != "adjust_area":
        pref["location_keys"] = [str(v) for v in (last.get("location_keys") or ()) if str(v or "").strip()]
        pref["area_display"] = str(last.get("area_display") or "")
    if kind != "adjust_budget":
        if last.get("budget_min") is not None:
            pref["budget_min"] = last.get("budget_min")
        if last.get("budget_max") is not None:
            pref["budget_max"] = last.get("budget_max")
        pref["budget_label"] = str(last.get("budget_label") or "")
    if kind != "adjust_layout":
        pref["room_type"] = str(last.get("room_type") or "")
        pref["layout_property_type"] = str(last.get("property_type") or "")
        pref["layout_label"] = str(last.get("layout_label") or "")
    return pref


def _resume_submit(pref: Mapping[str, Any]) -> TransitionActionResult:
    """Submit a resumed (adjusted) search with every kept condition."""
    location_keys = tuple(
        str(value).strip() for value in (pref.get("location_keys") or ()) if str(value or "").strip()
    )
    budget_min = pref.get("budget_min")
    budget_max = pref.get("budget_max")
    room_type = str(pref.get("room_type") or "").strip()
    property_type = str(pref.get("layout_property_type") or "").strip() or _goal_property_type(pref.get("goal"))
    criteria = SearchCriteria(
        property_type=property_type,
        location_keys=location_keys,
        budget_min=int(budget_min) if budget_min is not None else None,
        budget_max=int(budget_max) if budget_max is not None else None,
        room_type=room_type,
        raw_text=room_type,
    )
    area_display = str(pref.get("area_display") or "").strip()
    budget_label = str(pref.get("budget_label") or "").strip()
    layout_label = str(pref.get("layout_label") or "").strip()
    touch_payload = dict(pref.get("touch_payload") or {}) if isinstance(pref.get("touch_payload"), Mapping) else {}
    if room_type:
        touch_payload["room_type"] = room_type
    return TransitionActionResult(
        status="ok",
        next_step="search_submit",
        search=SearchSubmitIntent(
            criteria=criteria,
            source=str(pref.get("source") or "user_search").strip(),
            goal=layout_label or str(pref.get("goal") or "any").strip(),
            area_display=area_display,
            budget_label=budget_label,
            touch_payload=touch_payload,
        ),
        mutation=SessionMutationPlan(
            set_values={
                LAST_SEARCH_PREF_KEY: _last_pref(
                    criteria,
                    area_display=area_display,
                    budget_label=budget_label,
                    layout_label=layout_label,
                )
            },
            delete_keys=(SEARCH_PREF_SESSION_KEY, SEARCH_AWAITING_AREA_KEY, SEARCH_AWAITING_BUDGET_KEY),
        ),
    )


def _home_mutation() -> SessionMutationPlan:
    return SessionMutationPlan(
        set_values={},
        delete_keys=(
            APPOINTMENT_SESSION_KEY,
            APPOINTMENT_AWAITING_DATE_KEY,
            APPOINTMENT_AWAITING_TIME_KEY,
            SEARCH_PREF_SESSION_KEY,
            SEARCH_AWAITING_AREA_KEY,
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
            "appointment_quick_slot",
            "appointment_other_date",
            "appointment_time",
            "appointment_other_time",
            "appointment_back_date",
            "appointment_back_mode",
        }:
            draft = _load_appointment(session)
            if draft is None:
                return TransitionActionResult(
                    status="expired",
                    reason="appointment_session_expired",
                )

            if kind == "appointment_back_mode":
                return TransitionActionResult(
                    status="ok",
                    next_step="appointment_mode",
                    appointment=draft,
                    mutation=SessionMutationPlan(
                        set_values={APPOINTMENT_SESSION_KEY: _appointment_values(draft)},
                        delete_keys=(APPOINTMENT_AWAITING_DATE_KEY, APPOINTMENT_AWAITING_TIME_KEY),
                    ),
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

            if kind == "appointment_quick_slot":
                date_value, time_value = callback.value.rsplit("_", 1)
                try:
                    updated = draft.with_date(date_value).with_time(time_value)
                except ValueError:
                    return TransitionActionResult(
                        status="invalid",
                        reason="invalid_appointment_quick_slot",
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

        if kind == "search_area":
            return TransitionActionResult(
                status="ok",
                next_step="navigation",
                navigation="search_area",
                mutation=SessionMutationPlan(
                    set_values={SEARCH_PREF_SESSION_KEY: _entry_pref(session, "search_area")},
                    delete_keys=(SEARCH_AWAITING_AREA_KEY, SEARCH_AWAITING_BUDGET_KEY),
                ),
            )

        if kind == "area_choice":
            pref = _search_pref(session)
            if pref is None:
                return TransitionActionResult(status="expired", reason="search_session_expired")
            try:
                display, location_keys = area_selection(callback.value)
            except ValueError:
                return TransitionActionResult(status="invalid", reason="invalid_area_choice")
            updated_pref = dict(pref)
            updated_pref["location_keys"] = list(location_keys)
            updated_pref["area_display"] = display
            if updated_pref.get("resume") and updated_pref.get("budget_label"):
                return _resume_submit(updated_pref)
            return TransitionActionResult(
                status="ok",
                next_step="navigation",
                navigation="search_budget",
                mutation=SessionMutationPlan(
                    set_values={SEARCH_PREF_SESSION_KEY: push_step(updated_pref, "search_budget")},
                    delete_keys=(SEARCH_AWAITING_AREA_KEY, SEARCH_AWAITING_BUDGET_KEY),
                ),
            )

        if kind == "area_other":
            if _search_pref(session) is None:
                return TransitionActionResult(status="expired", reason="search_session_expired")
            return TransitionActionResult(
                status="ok",
                next_step="search_custom_area",
                mutation=SessionMutationPlan(
                    set_values={SEARCH_AWAITING_AREA_KEY: True},
                    delete_keys=(SEARCH_AWAITING_BUDGET_KEY,),
                ),
            )

        if kind == "search_budget":
            return TransitionActionResult(
                status="ok",
                next_step="navigation",
                navigation="search_budget",
                mutation=SessionMutationPlan(
                    set_values={SEARCH_PREF_SESSION_KEY: _entry_pref(session, "search_budget")},
                    delete_keys=(SEARCH_AWAITING_AREA_KEY, SEARCH_AWAITING_BUDGET_KEY),
                ),
            )

        if kind == "search_layout":
            return TransitionActionResult(
                status="ok",
                next_step="navigation",
                navigation="search_layout",
                mutation=SessionMutationPlan(
                    set_values={SEARCH_PREF_SESSION_KEY: _entry_pref(session, "search_layout")},
                    delete_keys=(SEARCH_AWAITING_AREA_KEY, SEARCH_AWAITING_BUDGET_KEY),
                ),
            )

        if kind == "layout_choice":
            try:
                display, room_type, property_type = layout_selection(callback.value)
            except ValueError:
                return TransitionActionResult(status="invalid", reason="invalid_layout_choice")
            pref = _search_pref(session) or _fresh_search_pref("home_layout")
            location_keys = tuple(
                str(value).strip()
                for value in (pref.get("location_keys") or ())
                if str(value or "").strip()
            )
            budget_min = pref.get("budget_min")
            budget_max = pref.get("budget_max")
            criteria = SearchCriteria(
                property_type=property_type or _goal_property_type(pref.get("goal")),
                location_keys=location_keys,
                budget_min=int(budget_min) if budget_min is not None else None,
                budget_max=int(budget_max) if budget_max is not None else None,
                room_type=room_type,
                raw_text=room_type,
            )
            touch_payload = (
                dict(pref.get("touch_payload") or {})
                if isinstance(pref.get("touch_payload"), Mapping)
                else {}
            )
            if room_type:
                touch_payload["room_type"] = room_type
            return TransitionActionResult(
                status="ok",
                next_step="search_submit",
                search=SearchSubmitIntent(
                    criteria=criteria,
                    source=str(pref.get("source") or "home_layout").strip(),
                    goal=display if room_type else str(pref.get("goal") or "any").strip(),
                    area_display=str(pref.get("area_display") or "").strip(),
                    budget_label=str(pref.get("budget_label") or "").strip(),
                    touch_payload=touch_payload,
                ),
                mutation=SessionMutationPlan(
                    set_values={LAST_SEARCH_PREF_KEY: _last_pref(criteria, area_display=str(pref.get("area_display") or ""), budget_label=str(pref.get("budget_label") or ""), layout_label=display if room_type else "")},
                    delete_keys=(SEARCH_PREF_SESSION_KEY, SEARCH_AWAITING_AREA_KEY, SEARCH_AWAITING_BUDGET_KEY),
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
            if source != "similar_listing":
                updated_pref = dict(pref)
                updated_pref.update(
                    {
                        "budget_min": budget_min,
                        "budget_max": budget_max,
                        "budget_label": budget_label,
                    }
                )
                if updated_pref.get("resume"):
                    return _resume_submit(updated_pref)
                return TransitionActionResult(
                    status="ok",
                    next_step="navigation",
                    navigation="search_layout",
                    mutation=SessionMutationPlan(
                        set_values={SEARCH_PREF_SESSION_KEY: push_step(updated_pref, "search_layout")},
                        delete_keys=(SEARCH_AWAITING_AREA_KEY, SEARCH_AWAITING_BUDGET_KEY),
                    ),
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
                    set_values={LAST_SEARCH_PREF_KEY: _last_pref(criteria, area_display=area_display, budget_label=budget_label)},
                    delete_keys=(SEARCH_PREF_SESSION_KEY, SEARCH_AWAITING_AREA_KEY, SEARCH_AWAITING_BUDGET_KEY),
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
                    delete_keys=(SEARCH_AWAITING_AREA_KEY,),
                ),
            )

        if kind == "search_back":
            target, pref = pop_step(_search_pref(session))
            if target == STEP_RESULTS:
                last = session.get(LAST_SEARCH_PREF_KEY)
                if isinstance(last, Mapping):
                    previous = dict(last)
                    previous["layout_property_type"] = str(last.get("property_type") or "")
                    previous["source"] = str(pref.get("source") or "user_search")
                    return _resume_submit(previous)
                target = "entry"
            if target == "entry":
                pref.pop("resume", None)
                return TransitionActionResult(
                    status="ok",
                    next_step="navigation",
                    navigation="search_entry",
                    mutation=SessionMutationPlan(
                        set_values={SEARCH_PREF_SESSION_KEY: pref},
                        delete_keys=(SEARCH_AWAITING_AREA_KEY, SEARCH_AWAITING_BUDGET_KEY),
                    ),
                )
            return TransitionActionResult(
                status="ok",
                next_step="navigation",
                navigation=target,  # type: ignore[arg-type]
                mutation=SessionMutationPlan(
                    set_values={SEARCH_PREF_SESSION_KEY: pref},
                    delete_keys=(SEARCH_AWAITING_AREA_KEY, SEARCH_AWAITING_BUDGET_KEY),
                ),
            )

        if kind in {"search_back_area", "search_back_budget"}:
            pref = _search_pref(session)
            target = "search_area" if kind == "search_back_area" else "search_budget"
            if pref is None:
                pref = _fresh_search_pref(f"home_{target.split('_', 1)[1]}")
            else:
                pref = dict(pref)
                if kind == "search_back_area":
                    # Going back to the area step re-opens area; budget/layout
                    # chosen later are kept only for display until re-picked.
                    pref.pop("resume", None)
                else:
                    pref.pop("budget_min", None)
                    pref.pop("budget_max", None)
                    pref.pop("budget_label", None)
            return TransitionActionResult(
                status="ok",
                next_step="navigation",
                navigation=target,  # type: ignore[arg-type]
                mutation=SessionMutationPlan(
                    set_values={SEARCH_PREF_SESSION_KEY: pref},
                    delete_keys=(SEARCH_AWAITING_AREA_KEY, SEARCH_AWAITING_BUDGET_KEY),
                ),
            )

        if kind in _ADJUST_NAVIGATION:
            return TransitionActionResult(
                status="ok",
                next_step="navigation",
                navigation=_ADJUST_NAVIGATION[kind],  # type: ignore[arg-type]
                mutation=SessionMutationPlan(
                    set_values={SEARCH_PREF_SESSION_KEY: push_step(_adjust_pref(session, kind), _ADJUST_NAVIGATION[kind])},
                    delete_keys=(SEARCH_AWAITING_AREA_KEY, SEARCH_AWAITING_BUDGET_KEY),
                ),
            )

        if kind == "home":
            return TransitionActionResult(
                status="ok",
                next_step="navigation",
                navigation="home",
                mutation=_home_mutation(),
            )

        return TransitionActionResult(
            status="invalid",
            reason="unsupported_transition_action",
        )


__all__ = [
    "APPOINTMENT_AWAITING_DATE_KEY",
    "APPOINTMENT_AWAITING_TIME_KEY",
    "LAST_SEARCH_PREF_KEY",
    "SEARCH_AWAITING_AREA_KEY",
    "SEARCH_AWAITING_BUDGET_KEY",
    "SearchSubmitIntent",
    "TransitionActionResult",
    "TransitionActionService",
]
