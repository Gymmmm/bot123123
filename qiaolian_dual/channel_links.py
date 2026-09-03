"""Canonical Telegram Deep Links for channel listing actions.

Every publisher/status edit uses one payload builder. Public QC ids stay QC;
canonical internal ``l_`` ids stay ``l_`` so a status refresh never changes the
link identity that was originally published. The User Bot accepts both and
resolves them to the same listing context.
"""
from __future__ import annotations

import re

_ACTION_SUFFIX = {
    "detail": "details",
    "details": "details",
    "photos": "photos",
    "book": "book",
}


def public_qc_code(listing_id: str) -> str:
    """Return the stable public QC display identifier for a listing id."""
    raw = str(listing_id or "").strip()
    match = re.search(r"(\d{1,8})", raw)
    if not match:
        return raw.upper()
    return f"QC{int(match.group(1)):04d}"


def channel_target(listing_id: str) -> str:
    """Keep canonical l_ ids canonical; normalize other public ids to QC."""
    raw = str(listing_id or "").strip()
    if re.fullmatch(r"(?i)l_\d+", raw):
        return raw.lower()
    return public_qc_code(raw)


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
