"""Pure transition plans for successful V3 User Bot callback intents.

The Telegram callback layer already separates renderable responses from
transitions.  This module turns those transitions into explicit domain plans
without mutating ``context.user_data``, writing leads, notifying admins, or
starting appointment persistence.

The plans preserve fixed-SHA entry semantics:
- book -> offline appointment draft, next step date;
- consult -> contact handoff, with lead/admin effects declared but not run;
- similar -> keep only the frozen public area, goal any, next step budget;
- change search -> reset to the normal search entry.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from .appointments import AppointmentDraft
from .consult import ConsultIntent
from .similar_intent import SimilarSearchIntent
from .telegram_callback_response import TelegramCallbackResponse


TransitionKind = Literal["book", "consult", "similar", "change_search"]
TransitionStep = Literal[
    "appointment_date",
    "contact_handoff",
    "search_budget",
    "search_entry",
]
TransitionEffect = Literal[
    "record_lead",
    "notify_admin",
    "render_appointment_date",
    "render_contact_handoff",
    "render_search_budget",
    "render_search_entry",
]


@dataclass(frozen=True)
class BookTransition:
    public_listing_id: str
    draft: AppointmentDraft


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
            next_step="appointment_date",
            effects=("render_appointment_date",),
            book=BookTransition(
                public_listing_id=intent.public_listing_id,
                draft=AppointmentDraft(
                    listing_id=intent.listing_id,
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
        if intent.goal != "any" or intent.next_step != "budget":
            raise ValueError("similar_transition_contract_mismatch")
        return TransitionPlan(
            kind="similar",
            next_step="search_budget",
            effects=("render_search_budget",),
            similar=SimilarTransition(intent=intent),
        )

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
