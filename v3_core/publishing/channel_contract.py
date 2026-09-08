"""Database-free public channel action contract.

The caller supplies the already-assigned public QL listing id.  This keeps URL
rendering deterministic and prevents channel rendering from reaching into
legacy listing/draft tables merely to discover an identifier.
"""
from __future__ import annotations

from .public_ids import normalize_public_id

_ACTION_SUFFIX = {
    "detail": "details",
    "details": "details",
    "photos": "photos",
    "book": "book",
}


def channel_start_payload(public_listing_id: object, action: str) -> str:
    public_id = normalize_public_id(public_listing_id)
    if not public_id:
        raise ValueError(f"invalid_public_listing_id:{public_listing_id}")
    suffix = _ACTION_SUFFIX.get(str(action or "").strip().lower())
    if not suffix:
        raise ValueError(f"unsupported_channel_action:{action}")
    return f"property_{public_id}_{suffix}"


def channel_action_url(
    username: str, public_listing_id: object, action: str
) -> str:
    user = str(username or "").strip().lstrip("@")
    if not user:
        return ""
    return f"https://t.me/{user}?start={channel_start_payload(public_listing_id, action)}"


def channel_actions(public_listing_id: object) -> tuple[str, str, str]:
    """Return the only three public listing actions in stable order."""
    return tuple(
        channel_start_payload(public_listing_id, action)
        for action in ("details", "photos", "book")
    )


__all__ = [
    "channel_action_url",
    "channel_actions",
    "channel_start_payload",
]
