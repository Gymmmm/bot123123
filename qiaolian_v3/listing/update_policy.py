from __future__ import annotations

from enum import Enum
from typing import Iterable, Mapping, Any


class UpdateKind(str, Enum):
    DUPLICATE = 'DUPLICATE'
    PRICE_UPDATE = 'PRICE_UPDATE'
    MEDIA_UPDATE = 'MEDIA_UPDATE'
    FACT_UPDATE = 'FACT_UPDATE'
    FACT_AND_MEDIA_UPDATE = 'FACT_AND_MEDIA_UPDATE'


_PRICE_KEYS = {'monthly_rent_usd', 'original_monthly_rent_usd', 'sale_price_usd'}


def _media(values: Iterable[str]) -> tuple[str, ...]:
    return tuple(str(v) for v in values if str(v))


def classify_update(
    *,
    previous_facts: Mapping[str, Any],
    current_facts: Mapping[str, Any],
    previous_media: Iterable[str] = (),
    current_media: Iterable[str] = (),
) -> UpdateKind:
    previous = dict(previous_facts)
    current = dict(current_facts)
    media_changed = _media(previous_media) != _media(current_media)

    all_keys = set(previous) | set(current)
    changed = {key for key in all_keys if previous.get(key) != current.get(key)}
    if not changed and not media_changed:
        return UpdateKind.DUPLICATE
    if changed and changed.issubset(_PRICE_KEYS) and not media_changed:
        return UpdateKind.PRICE_UPDATE
    if not changed and media_changed:
        return UpdateKind.MEDIA_UPDATE
    if changed and media_changed:
        return UpdateKind.FACT_AND_MEDIA_UPDATE
    return UpdateKind.FACT_UPDATE


__all__ = ['UpdateKind', 'classify_update']
