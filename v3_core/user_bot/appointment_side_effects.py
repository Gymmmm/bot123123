"""Deterministic side-effect plans for V3 appointment transactions.

The transaction services only write appointment state. This module exposes the
follow-up order that production currently spreads across Database and Telegram
handlers, making retries and future runtime wiring auditable.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from .appointment_management import AppointmentCancellationResult
from .appointment_service import AppointmentSubmissionResult


AppointmentEffect = Literal[
    "recompute_listing_availability",
    "record_lead",
    "sync_channel",
    "notify_admin",
]


@dataclass(frozen=True)
class AppointmentEffectPlan:
    effects: tuple[AppointmentEffect, ...]

    def includes(self, effect: AppointmentEffect) -> bool:
        return effect in self.effects


def submission_effect_plan(result: AppointmentSubmissionResult) -> AppointmentEffectPlan:
    effects: list[AppointmentEffect] = []
    if result.should_recompute_listing_availability:
        effects.append("recompute_listing_availability")
    if result.lead_action:
        effects.append("record_lead")
    if result.should_sync_channel:
        effects.append("sync_channel")
    if result.should_notify_admin:
        effects.append("notify_admin")
    return AppointmentEffectPlan(tuple(effects))


def cancellation_effect_plan(result: AppointmentCancellationResult) -> AppointmentEffectPlan:
    effects: list[AppointmentEffect] = []
    if result.should_recompute_listing_availability:
        effects.append("recompute_listing_availability")
    if result.lead_action:
        effects.append("record_lead")
    if result.should_sync_channel:
        effects.append("sync_channel")
    if result.should_notify_admin:
        effects.append("notify_admin")
    return AppointmentEffectPlan(tuple(effects))


__all__ = [
    "AppointmentEffect",
    "AppointmentEffectPlan",
    "cancellation_effect_plan",
    "submission_effect_plan",
]
