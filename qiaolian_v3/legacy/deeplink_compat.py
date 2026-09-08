from __future__ import annotations

from dataclasses import dataclass

from .public_id_compat import normalize_legacy_listing_reference


@dataclass(frozen=True)
class LegacyDeepLink:
    action: str
    legacy_reference: str
    normalized_legacy_key: str


def resolve_legacy_deeplink(action: str, raw_listing_ref: str) -> LegacyDeepLink | None:
    normalized = normalize_legacy_listing_reference(raw_listing_ref)
    if normalized is None:
        return None
    action_map = {
        'property_details': 'details', 'details': 'details',
        'propertyphotos': 'photos', 'photos': 'photos',
        'property_book': 'book', 'book': 'book', 'appoint': 'book',
    }
    resolved_action = action_map.get(str(action or '').strip())
    if resolved_action is None:
        return None
    return LegacyDeepLink(
        action=resolved_action,
        legacy_reference=str(raw_listing_ref),
        normalized_legacy_key=normalized,
    )


__all__ = ['LegacyDeepLink', 'resolve_legacy_deeplink']
