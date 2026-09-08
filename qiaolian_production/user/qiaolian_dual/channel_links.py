"""Canonical public listing identifiers used by channel-facing runtime code."""
from __future__ import annotations


def public_qc_code(listing_id: str) -> str:
    """Compatibility name returning the stable QL public identifier."""
    from .public_listing_id import public_listing_id
    return public_listing_id(listing_id)
