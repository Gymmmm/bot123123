from __future__ import annotations

from dataclasses import dataclass

from qiaolian_v3.legacy.deeplink_compat import LegacyDeepLink, resolve_legacy_deeplink
from qiaolian_v3.listing.public_id import normalize_public_id


@dataclass(frozen=True)
class DeepLinkTarget:
    action: str
    public_listing_id: str | None
    legacy: LegacyDeepLink | None = None


def require_ql_identity(value: str) -> str:
    normalized = normalize_public_id(value)
    if normalized is None:
        raise ValueError('new_business_requires_ql_public_id')
    return normalized


def _formal_property_target(raw: str) -> DeepLinkTarget | None:
    if not raw.startswith('property_'):
        return None
    body = raw[len('property_'):]
    for suffix, action in (('_details', 'details'), ('_photos', 'photos'), ('_book', 'book')):
        if not body.endswith(suffix):
            continue
        ref = body[:-len(suffix)]
        normalized = normalize_public_id(ref)
        if normalized is not None:
            return DeepLinkTarget(action=action, public_listing_id=normalized)
        legacy = resolve_legacy_deeplink(action, ref)
        if legacy is not None:
            return DeepLinkTarget(action=action, public_listing_id=None, legacy=legacy)
        return None
    return None


def parse_start_payload(payload: str) -> DeepLinkTarget | None:
    raw = str(payload or '').strip()
    if not raw:
        return None

    formal = _formal_property_target(raw)
    if formal is not None:
        return formal

    # Phase-6 frozen channel buttons remain a permanent compatibility surface.
    for prefix, action in (('details_', 'details'), ('photos_', 'photos'), ('book_', 'book')):
        if not raw.startswith(prefix):
            continue
        ref = raw[len(prefix):]
        normalized = normalize_public_id(ref)
        if normalized is not None:
            return DeepLinkTarget(action=action, public_listing_id=normalized)
        legacy = resolve_legacy_deeplink(action, ref)
        if legacy is not None:
            return DeepLinkTarget(action=action, public_listing_id=None, legacy=legacy)
        return None

    # Older property_* forms are read-only legacy compatibility only.
    for prefix, action in (
        ('property_details_', 'details'),
        ('propertyphotos_', 'photos'),
        ('property_book_', 'book'),
    ):
        if raw.startswith(prefix):
            legacy = resolve_legacy_deeplink(action, raw[len(prefix):])
            if legacy is not None:
                return DeepLinkTarget(action=action, public_listing_id=None, legacy=legacy)
            return None
    return None


def build_property_payload(action: str, public_listing_id: str) -> str:
    ql = require_ql_identity(public_listing_id)
    normalized_action = str(action)
    if normalized_action not in {'details', 'photos', 'book'}:
        raise ValueError('unsupported_property_action')
    return f'property_{ql}_{normalized_action}'


__all__ = ['DeepLinkTarget', 'build_property_payload', 'parse_start_payload', 'require_ql_identity']
