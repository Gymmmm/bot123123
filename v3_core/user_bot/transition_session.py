"""Public-ID-only session mutation plans for V3 User Bot transitions.

Transition domain objects may legitimately contain an internal listing id for a
later persistence boundary. Telegram ``user_data`` must not. This module
projects transition plans into a small session contract containing only QL
public ids, user-selected values, and frozen public location hints.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from v3_core.publishing.public_ids import normalize_public_id

from .transition_plan import TransitionPlan


APPOINTMENT_SESSION_KEY = "v3_appointment"
SEARCH_PREF_SESSION_KEY = "v3_search_pref"
AWAITING_KEYWORD_SESSION_KEY = "v3_awaiting_keyword_find"
CONTACT_SESSION_KEY = "v3_contact_public_listing_id"
SEARCH_CARD_SESSION_KEY = "v3_find_card_public_ids"
SEARCH_CARD_ANCHOR_KEY = "v3_find_card_anchor"


@dataclass(frozen=True)
class SessionMutationPlan:
    set_values: dict[str, Any]
    delete_keys: tuple[str, ...] = ()


def _public_id(value: object) -> str:
    public_id = normalize_public_id(value)
    if public_id is None:
        raise ValueError("transition_session_requires_public_listing_id")
    return public_id


def build_transition_session(plan: TransitionPlan) -> SessionMutationPlan:
    if plan.kind == "book":
        if plan.book is None:
            raise ValueError("book_transition_missing_public_draft")
        draft = plan.book.draft
        return SessionMutationPlan(
            set_values={
                APPOINTMENT_SESSION_KEY: {
                    "public_listing_id": _public_id(draft.public_listing_id),
                    "mode": draft.mode,
                    "date": draft.date,
                    "time": draft.time,
                    "source": draft.source,
                }
            },
        )

    if plan.kind == "consult":
        if plan.consult is None:
            raise ValueError("consult_transition_missing_intent")
        public_id = _public_id(plan.consult.intent.public_listing_id)
        return SessionMutationPlan(
            set_values={CONTACT_SESSION_KEY: public_id},
        )

    if plan.kind == "similar":
        if plan.similar is None:
            raise ValueError("similar_transition_missing_intent")
        intent = plan.similar.intent
        public_id = _public_id(intent.public_listing_id)
        return SessionMutationPlan(
            set_values={
                SEARCH_PREF_SESSION_KEY: {
                    "source": "similar_listing",
                    "goal": "any",
                    "location_keys": list(intent.location_keys),
                    "area_display": intent.area_display,
                    "touch_payload": {"from_public_listing_id": public_id},
                }
            },
            delete_keys=(AWAITING_KEYWORD_SESSION_KEY,),
        )

    if plan.kind == "change_search":
        if plan.change_search is None:
            raise ValueError("change_search_transition_missing_state")
        return SessionMutationPlan(
            set_values={
                SEARCH_PREF_SESSION_KEY: {
                    "source": plan.change_search.source,
                    "goal": plan.change_search.goal,
                    "location_keys": [],
                    "area_display": "",
                    "touch_payload": {},
                },
                AWAITING_KEYWORD_SESSION_KEY: {
                    "source": plan.change_search.source,
                },
            },
            delete_keys=(SEARCH_CARD_SESSION_KEY, SEARCH_CARD_ANCHOR_KEY),
        )

    raise ValueError(f"unsupported_transition_session_kind:{plan.kind}")


def apply_session_mutation(
    user_data: dict[str, Any],
    mutation: SessionMutationPlan,
) -> None:
    """Apply only the already-built local session mutation plan."""
    for key in mutation.delete_keys:
        user_data.pop(str(key), None)
    for key, value in mutation.set_values.items():
        user_data[str(key)] = value


__all__ = [
    "APPOINTMENT_SESSION_KEY",
    "AWAITING_KEYWORD_SESSION_KEY",
    "CONTACT_SESSION_KEY",
    "SEARCH_CARD_ANCHOR_KEY",
    "SEARCH_CARD_SESSION_KEY",
    "SEARCH_PREF_SESSION_KEY",
    "SessionMutationPlan",
    "apply_session_mutation",
    "build_transition_session",
]
