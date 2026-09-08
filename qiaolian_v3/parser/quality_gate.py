from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Mapping


class RoutingDecision(str, Enum):
    READY = 'READY_TO_PUBLISH'
    REVIEW = 'REVIEW'
    STORE_ONLY = 'STORE_ONLY'


@dataclass(frozen=True)
class QualityDecision:
    routing_decision: RoutingDecision
    blocking_flags: tuple[str, ...]
    warning_flags: tuple[str, ...]
    usable_photo_count: int


def evaluate_quality(
    *,
    canonical: Mapping[str, Any],
    usable_photo_count: int,
    minimum_photos: int = 4,
) -> QualityDecision:
    """The single Phase-4 authority that emits routing_decision.

    Media modules only produce evidence. Collector, dedupe and update policy do
    not decide publishability. Sale stays stored and can never be routed to the
    rent publication path. Unknown/conflicting facts stop for review.
    """
    facts = dict(canonical)
    quality = dict(facts.get('quality') or {})
    blocking = [str(v) for v in (quality.get('blocking_flags') or [])]
    warnings = [str(v) for v in (quality.get('warning_flags') or [])]
    count = max(0, int(usable_photo_count))
    deal_type = str(facts.get('deal_type') or 'unknown')

    if deal_type == 'sale':
        return QualityDecision(RoutingDecision.STORE_ONLY, tuple(dict.fromkeys(blocking)), tuple(dict.fromkeys(warnings)), count)

    if deal_type != 'rent':
        if 'unresolved_deal_type' not in blocking:
            blocking.append('unresolved_deal_type')
    elif not facts.get('monthly_rent_usd'):
        if 'missing_price' not in blocking:
            blocking.append('missing_price')

    if count < max(1, int(minimum_photos)):
        if 'insufficient_usable_photos' not in blocking:
            blocking.append('insufficient_usable_photos')

    blocking = list(dict.fromkeys(blocking))
    warnings = list(dict.fromkeys(warnings))
    decision = RoutingDecision.REVIEW if blocking else RoutingDecision.READY
    return QualityDecision(decision, tuple(blocking), tuple(warnings), count)


__all__ = ['RoutingDecision', 'QualityDecision', 'evaluate_quality']
