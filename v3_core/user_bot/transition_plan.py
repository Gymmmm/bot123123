"""Pure transition plans for successful V3 User Bot callback intents.

The Telegram callback layer already separates renderable responses from
transitions. This module turns those transitions into explicit domain plans
without mutating ``context.user_data``, writing leads, notifying admins, or
starting appointment persistence.

The plans preserve fixed-SHA entry semantics while keeping Telegram/session
identity public:
    - book -> public-ID appointment draft, next step mode selection;
    - consult -> contact handoff, with lead/admin effects declared but not run;
    - similar -> for rented/offline: run similar search immediately with known
                 budget/room_type; for bookable: ask for budget preference;
    - change search -> reset to the normal search entry.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from .appointments import AppointmentDraft, normalize_mode
from .consult import ConsultIntent
from .public_appointment import PublicAppointmentDraft
from .search_query import SearchCriteria
from .similar_intent import SimilarSearchIntent
from .telegram_callback_response import TelegramCallbackResponse


TransitionKind = Literal["book", "consult", "similar", "change_search"]
TransitionStep = Literal[
    "appointment_mode",
    "appointment_date",
    "contact_handoff",
    "search_budget",
    "search_entry",
    "search_submit",  # direct similar search (rented/offline)
]
TransitionEffect = Literal[
    "record_lead",
    "notify_admin",
    "render_appointment_mode",
    "render_appointment_date",
    "render_contact_handoff",
    "render_search_budget",
    "render_search_entry",
    "run_similar_search",
]


@dataclass(frozen=True)
class BookTransition:
    draft: PublicAppointmentDraft

    @property
    def public_listing_id(self) -> str:
        return self.draft.public_listing_id


@dataclass(frozen=True)
class ConsultTransition:
    intent: ConsultIntent


@dataclass(frozen=True)
class SimilarTransition:
    intent: SimilarSearchIntent


@dataclass(frozen=True)
class ChangeSearchTransition:
    source: str = "user_search"
    goal: str = "any"


@dataclass(frozen=True)
class TransitionPlan:
    kind: TransitionKind
    next_step: TransitionStep
    effects: tuple[TransitionEffect, ...]
    book: BookTransition | None = None
    consult: ConsultTransition | None = None
    similar: SimilarTransition | None = None
    change_search: ChangeSearchTransition | None = None

    def includes(self, effect: TransitionEffect) -> bool:
        return effect in self.effects


def build_transition_plan(response: TelegramCallbackResponse) -> TransitionPlan:
    if response.kind != "transition" or not response.ok:
        raise ValueError("transition_plan_requires_successful_transition_response")

    if response.transition == "book":
        intent = response.book_intent
        if intent is None:
            raise ValueError("book_transition_missing_intent")
        return TransitionPlan(
            kind="book",
            next_step="appointment_mode",
            effects=("render_appointment_mode",),
            book=BookTransition(
                draft=PublicAppointmentDraft(
                    public_listing_id=intent.public_listing_id,
                    mode="offline",
                    source=intent.source,
                ),
            ),
        )

    if response.transition == "consult":
        intent = response.consult_intent
        if intent is None:
            raise ValueError("consult_transition_missing_intent")
        return TransitionPlan(
            kind="consult",
            next_step="contact_handoff",
            effects=(
                "record_lead",
                "notify_admin",
                "render_contact_handoff",
            ),
            consult=ConsultTransition(intent=intent),
        )

    if response.transition == "similar":
        intent = response.similar_intent
        if intent is None:
            raise ValueError("similar_transition_missing_intent")
        # Rented/offline listings: goal="any", next_step="search_submit" -> direct search
        # Bookable listings: goal="budget", next_step="budget" -> ask for budget
        if intent.next_step == "search_submit":
            # Direct similar search (rented/offline) - must have budget info
            if intent.goal != "any" or not intent.location_keys:
                raise ValueError("similar_transition_contract_mismatch")
            return TransitionPlan(
                kind="similar",
                next_step="search_submit",
                effects=("run_similar_search",),
                similar=SimilarTransition(intent=intent),
            )
        if intent.next_step == "budget" and intent.goal == "budget":
            # Guided similar search (bookable) - ask for budget preference
            return TransitionPlan(
                kind="similar",
                next_step="search_budget",
                effects=("render_search_budget",),
                similar=SimilarTransition(intent=intent),
            )
        # next_step == "budget" but goal != "budget" -> invalid
        raise ValueError("similar_transition_contract_mismatch")

    if response.transition == "change_search":
        return TransitionPlan(
            kind="change_search",
            next_step="search_entry",
            effects=("render_search_entry",),
            change_search=ChangeSearchTransition(),
        )

    raise ValueError("unsupported_transition_response")


__all__ = [
    "BookTransition",
    "ChangeSearchTransition",
    "ConsultTransition",
    "SimilarTransition",
    "TransitionEffect",
    "TransitionKind",
    "TransitionPlan",
    "TransitionStep",
    "build_transition_plan",
]
