"""Pure parser for the only public V3 channel listing deep links.

The published channel contract is intentionally smaller than the historical
User Bot parser.  V3 channel posts may expose only details/photos/book.  Legacy
links remain a compatibility concern for the eventual cut-over adapter and are
not allowed to expand this public contract.
"""
from __future__ import annotations

from dataclasses import dataclass
import re

from v3_core.publishing.public_ids import normalize_public_id


PUBLIC_CHANNEL_ACTIONS = ("details", "photos", "book")
_PROPERTY_RE = re.compile(r"^property_(.+)_(details|photos|book)$", re.I)


@dataclass(frozen=True)
class PublicChannelRoute:
    action: str
    public_listing_id: str


def parse_channel_start_payload(payload: object) -> PublicChannelRoute | None:
    """Parse an official V3 channel payload without DB access or side effects."""
    raw = str(payload or "").strip()
    match = _PROPERTY_RE.fullmatch(raw)
    if match is None:
        return None
    public_id = normalize_public_id(match.group(1))
    if public_id is None:
        return None
    return PublicChannelRoute(
        action=match.group(2).lower(),
        public_listing_id=public_id,
    )


__all__ = [
    "PUBLIC_CHANNEL_ACTIONS",
    "PublicChannelRoute",
    "parse_channel_start_payload",
]
