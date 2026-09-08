from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .eligibility import evaluate_rent_publication_eligibility
from .package import FrozenPublicationPackage


@dataclass(frozen=True)
class GateResult:
    allowed: bool
    reasons: tuple[str, ...]


class AutoPublishGate:
    """Sole gate from frozen package to automatic Telegram-rent planning."""

    def evaluate(self, package: FrozenPublicationPackage, *, source_mode: str | None = None, quality_result: str | None = None) -> GateResult:
        result = evaluate_rent_publication_eligibility(
            canonical={'deal_type': package.deal_type},
            offer={'offer_type': package.offer_type, 'publication_policy': package.publication_policy},
            source_mode=str(source_mode if source_mode is not None else package.source_mode),
            quality_result=str(quality_result if quality_result is not None else package.quality_result),
            frozen=package.frozen,
        )
        return GateResult(result.allowed, result.reason_codes)


__all__ = ['AutoPublishGate','GateResult']
