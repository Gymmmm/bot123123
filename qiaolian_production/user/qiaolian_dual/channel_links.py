"""Canonical Telegram deep links for channel listing actions."""
from __future__ import annotations

_ACTION_SUFFIX = {
    "detail": "details",
    "details": "details",
    "photos": "photos",
    "book": "book",
}


def public_qc_code(listing_id: str) -> str:
    """Compatibility name returning the stable QL public identifier."""
    from .public_listing_id import public_listing_id
    return public_listing_id(listing_id)


def channel_target(listing_id: str) -> str:
    """Channel links expose only the stable public identifier."""
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
