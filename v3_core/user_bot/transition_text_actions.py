"""Pure text-input transitions for V3 guided appointment/search flows.

This is the text counterpart to ``TransitionActionService``. It handles only
explicit awaiting states already present in public Telegram session data and
never calls Telegram, SQLite, appointment persistence, lead creation, or admin
notification.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, Mapping

from .appointments import normalize_custom_date, valid_custom_time
from .public_appointment import PublicAppointmentDraft
from .search_query import SearchCriteria, detect_location_keys, detect_property_type, parse_budget_range
from .transition_actions import (
    APPOINTMENT_AWAITING_DATE_KEY,
    APPOINTMENT_AWAITING_TIME_KEY,
    LAST_SEARCH_PREF_KEY,
    SEARCH_AWAITING_AREA_KEY,
    SEARCH_AWAITING_BUDGET_KEY,
    SearchSubmitIntent,
)
from .transition_session import APPOINTMENT_SESSION_KEY, SEARCH_PREF_SESSION_KEY, SessionMutationPlan


TextActionStatus = Literal["ok", "invalid", "expired", "not_applicable"]
TextNextStep = Literal[
    "appointment_time",
    "appointment_submit",
    "search_budget",
    "search_submit",
]


@dataclass(frozen=True)
class TransitionTextActionResult:
    status: TextActionStatus
    next_step: TextNextStep | None = None
    reason: str = ""
    prompt: str = ""
    mutation: SessionMutationPlan | None = None
    appointment: PublicAppointmentDraft | None = None
    search: SearchSubmitIntent | None = None

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


def _goal_property_type(goal: object) -> str:
    clean = str(goal or "any").strip()
    if clean in {"", "any", "住宅"}:
        return ""
    return detect_property_type(clean) or clean


def _budget_text(budget_min: int | None, budget_max: int | None) -> str:
    if budget_min is not None and budget_max is not None:
        return f"{budget_min}-{budget_max} USD/月"
    if budget_min is not None:
        return f">= {budget_min} USD/月"
    if budget_max is not None:
        return f"<= {budget_max} USD/月"
    return "-"


def _search_submit_from_text(
    text: str,
    pref: Mapping[str, Any],
    budget_min: int | None,
    budget_max: int | None,
) -> SearchSubmitIntent:
    goal = str(pref.get("goal") or "any").strip()
    property_type = _goal_property_type(goal)
    raw_locations = pref.get("location_keys") or ()
    if isinstance(raw_locations, (list, tuple)):
        location_keys = tuple(
            str(value).strip() for value in raw_locations if str(value or "").strip()
        )
    else:
        location_keys = ()
    area_display = str(pref.get("area_display") or pref.get("area") or "").strip()
    if not location_keys and area_display:
        location_keys = detect_location_keys(area_display)
    touch_payload = (
        dict(pref.get("touch_payload") or {})
        if isinstance(pref.get("touch_payload"), Mapping)
        else {}
    )
    touch_payload = {"message": text, **touch_payload}
    return SearchSubmitIntent(
        criteria=SearchCriteria(
            property_type=property_type,
            location_keys=location_keys,
            budget_min=budget_min,
            budget_max=budget_max,
            raw_text="",
        ),
        source=str(pref.get("source") or "user_search").strip(),
        goal=goal,
        area_display=area_display,
        budget_label=_budget_text(budget_min, budget_max),
        touch_payload=touch_payload,
    )


def _last_search_pref(intent: SearchSubmitIntent) -> dict[str, Any]:
    return {
        "property_type": intent.criteria.property_type,
        "location_keys": list(intent.criteria.location_keys),
        "budget_min": intent.criteria.budget_min,
        "budget_max": intent.criteria.budget_max,
        "room_type": intent.criteria.room_type,
    }


class TransitionTextActionService:
    def apply(self, text: object, session: Mapping[str, Any]) -> TransitionTextActionResult:
        value = str(text or "").strip()[:40]

        if bool(session.get(APPOINTMENT_AWAITING_DATE_KEY)):
            draft = _load_appointment(session)
            if draft is None:
                return TransitionTextActionResult(
                    status="expired",
                    reason="appointment_session_expired",
                )
            normalized = normalize_custom_date(value)
            if not normalized:
                return TransitionTextActionResult(
                    status="invalid",
                    reason="appointment_date_unrecognized",
                    prompt=(
                        "日期格式没有识别出来。请试试：<code>0905</code>、"
                        "<code>9月5日</code> 或 <code>下周三</code>。"
                    ),
                )
            updated = draft.with_date(normalized)
            return TransitionTextActionResult(
                status="ok",
                next_step="appointment_time",
                appointment=updated,
                mutation=SessionMutationPlan(
                    set_values={APPOINTMENT_SESSION_KEY: _appointment_values(updated)},
                    delete_keys=(APPOINTMENT_AWAITING_DATE_KEY,),
                ),
            )

        if bool(session.get(APPOINTMENT_AWAITING_TIME_KEY)):
            draft = _load_appointment(session)
            if draft is None:
                return TransitionTextActionResult(
                    status="expired",
                    reason="appointment_session_expired",
                )
            if not valid_custom_time(value):
                return TransitionTextActionResult(
                    status="invalid",
                    reason="appointment_time_unrecognized",
                    prompt=(
                        "时间格式没有识别出来。请试试：<code>20:00</code> "
                        "或 <code>晚上8点</code>。"
                    ),
                )
            try:
                updated = draft.with_time(value)
            except ValueError:
                return TransitionTextActionResult(
                    status="invalid",
                    reason="appointment_date_required_before_time",
                )
            return TransitionTextActionResult(
                status="ok",
                next_step="appointment_submit",
                appointment=updated,
                mutation=SessionMutationPlan(
                    set_values={APPOINTMENT_SESSION_KEY: _appointment_values(updated)},
                    delete_keys=(APPOINTMENT_AWAITING_TIME_KEY,),
                ),
            )

        if bool(session.get(SEARCH_AWAITING_AREA_KEY)):
            raw_pref = session.get(SEARCH_PREF_SESSION_KEY)
            if not isinstance(raw_pref, Mapping):
                return TransitionTextActionResult(
                    status="expired",
                    reason="search_session_expired",
                )
            location_keys = detect_location_keys(value)
            if not location_keys:
                return TransitionTextActionResult(
                    status="invalid",
                    reason="search_area_unrecognized",
                    prompt=(
                        "位置没有识别出来。请试试：<code>BKK1</code>、"
                        "<code>永旺1附近</code>、<code>富力城</code>。"
                    ),
                )
            pref = dict(raw_pref)
            pref["location_keys"] = list(location_keys)
            pref["area_display"] = value
            return TransitionTextActionResult(
                status="ok",
                next_step="search_budget",
                mutation=SessionMutationPlan(
                    set_values={SEARCH_PREF_SESSION_KEY: pref},
                    delete_keys=(SEARCH_AWAITING_AREA_KEY, SEARCH_AWAITING_BUDGET_KEY),
                ),
            )

        if bool(session.get(SEARCH_AWAITING_BUDGET_KEY)):
            raw_pref = session.get(SEARCH_PREF_SESSION_KEY)
            if not isinstance(raw_pref, Mapping):
                return TransitionTextActionResult(
                    status="expired",
                    reason="search_session_expired",
                )
            budget_min, budget_max = parse_budget_range(value)
            if budget_min is None and budget_max is None:
                return TransitionTextActionResult(
                    status="invalid",
                    reason="budget_unrecognized",
                    prompt=(
                        "预算没有识别出来。请试试：<code>800以内</code>、"
                        "<code>600-900</code> 或 <code>1500以上</code>。"
                    ),
                )
            pref = dict(raw_pref)
            intent = _search_submit_from_text(value, pref, budget_min, budget_max)
            return TransitionTextActionResult(
                status="ok",
                next_step="search_submit",
                search=intent,
                mutation=SessionMutationPlan(
                    set_values={LAST_SEARCH_PREF_KEY: _last_search_pref(intent)},
                    delete_keys=(SEARCH_PREF_SESSION_KEY, SEARCH_AWAITING_AREA_KEY, SEARCH_AWAITING_BUDGET_KEY),
                ),
            )

        return TransitionTextActionResult(status="not_applicable")


__all__ = ["TransitionTextActionResult", "TransitionTextActionService"]
