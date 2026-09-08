from __future__ import annotations

import re
from dataclasses import dataclass

from qiaolian_v3.legacy.deeplink_compat import LegacyDeepLink, resolve_legacy_deeplink

QL_RE = re.compile(r'^QL\d{6,}$', re.I)


@dataclass(frozen=True)
class DeepLinkTarget:
    action: str
    public_listing_id: str | None
    legacy: LegacyDeepLink | None = None


def require_ql_identity(value: str) -> str:
    raw = str(value or '').strip().upper()
    if not QL_RE.fullmatch(raw):
        raise ValueError('new_business_requires_ql_public_id')
    return raw


def parse_start_payload(payload: str) -> DeepLinkTarget | None:
    raw = str(payload or '').strip()
    if not raw:
        return None
    prefixes = (
        ('property_details_', 'details'),
        ('propertyphotos_', 'photos'),
        ('property_book_', 'book'),
        # Frozen Phase-6 three-button payloads remain supported unchanged.
        ('details_', 'details'),
        ('photos_', 'photos'),
        ('book_', 'book'),
    )
    for prefix, action in prefixes:
        if not raw.startswith(prefix):
            continue
        ref = raw[len(prefix):]
        if QL_RE.fullmatch(ref.upper()):
            return DeepLinkTarget(action=action, public_listing_id=ref.upper())
        legacy = resolve_legacy_deeplink(action, ref)
        if legacy is not None:
            return DeepLinkTarget(action=action, public_listing_id=None, legacy=legacy)
        return None
    return None


def build_property_payload(action: str, public_listing_id: str) -> str:
    ql = require_ql_identity(public_listing_id)
    prefix = {
        'details': 'property_details',
        'photos': 'propertyphotos',
        'book': 'property_book',
    }.get(str(action))
    if prefix is None:
        raise ValueError('unsupported_property_action')
    return f'{prefix}_{ql}'


__all__ = ['DeepLinkTarget', 'QL_RE', 'build_property_payload', 'parse_start_payload', 'require_ql_identity']
