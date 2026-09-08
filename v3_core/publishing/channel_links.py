"""Canonical Telegram Deep Links for channel listing actions.

Extracted from qiaolian_dual/channel_links.py at
8e4605cf5cc21dfec3ce30729654b09e39de9abf.

During side-by-side extraction this module still delegates public ID generation
to the production implementation. That dependency will be removed only after
the public-ID storage contract is extracted and tested separately.
"""
from __future__ import annotations

from qiaolian_dual.public_listing_id import public_listing_id

_ACTION_SUFFIX = {
    "detail": "details",
    "details": "details",
    "photos": "photos",
    "book": "book",
}


def public_qc_code(listing_id: str) -> str:
    """Compatibility name returning the stable QL public identifier."""
    return public_listing_id(listing_id)


def channel_target(listing_id: str) -> str:
    """Channel links always expose QL; internal l_ ids never leave the server."""
    return public_qc_code(listing_id)


def channel_start_payload(listing_id: str, action: str) -> str:
    suffix = _ACTION_SUFFIX.get(str(action or "").strip().lower())
    if not suffix:
        raise ValueError(f"unsupported_channel_action:{action}")
    return f"property_{channel_target(listing_id)}_{suffix}"


def channel_action_url(username: str, listing_id: str, action: str) -> str:
    user = str(username or "").strip().lstrip("@")
    if not user:
        return ""
    return f"https://t.me/{user}?start={channel_start_payload(listing_id, action)}"
