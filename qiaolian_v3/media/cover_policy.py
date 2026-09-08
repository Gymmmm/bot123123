from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CoverPolicyDecision:
    style: str
    source: str
    warnings: tuple[str, ...] = ()


def select_cover_style(
    *,
    property_type: str | None,
    monthly_rent_usd: int | float | None,
    manual_style: str | None = None,
) -> CoverPolicyDecision:
    """Locked V3 cover routing: right_price is administrator manual-only."""
    manual = str(manual_style or '').strip().lower()
    if manual:
        if manual in {'classic_blue', 'black_gold', 'right_price'}:
            return CoverPolicyDecision(manual, 'manual')
        return CoverPolicyDecision('classic_blue', 'manual_fallback', ('invalid_manual_cover_style',))

    property_key = str(property_type or '').strip().lower()
    rent = float(monthly_rent_usd or 0)
    if property_key in {'villa', 'townhouse', '别墅', '联排', '排屋'} or rent >= 1200:
        return CoverPolicyDecision('black_gold', 'automatic')
    return CoverPolicyDecision('classic_blue', 'automatic')


__all__ = ['CoverPolicyDecision', 'select_cover_style']
