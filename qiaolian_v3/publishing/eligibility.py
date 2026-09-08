from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class RentPublicationEligibility:
    allowed: bool
    reason_codes: tuple[str, ...]


def evaluate_rent_publication_eligibility(
    *, canonical: dict[str, Any], offer: dict[str, Any], source_mode: str,
    quality_result: str | None, frozen: bool,
) -> RentPublicationEligibility:
    reasons: list[str] = []
    if str(canonical.get('deal_type') or '') != 'rent':
        reasons.append('not_rent')
    if str(offer.get('offer_type') or '') != 'rent':
        reasons.append('offer_not_rent')
    if str(offer.get('publication_policy') or '') != 'telegram_rent':
        reasons.append('offer_not_telegram_rent')
    if str(source_mode or '') != 'collector':
        reasons.append('source_not_collector')
    if str(quality_result or '') != 'AUTO_PUBLISH':
        reasons.append('quality_not_auto_publish')
    if not frozen:
        reasons.append('package_not_frozen')
    return RentPublicationEligibility(not reasons, tuple(reasons))


__all__ = ['RentPublicationEligibility', 'evaluate_rent_publication_eligibility']
