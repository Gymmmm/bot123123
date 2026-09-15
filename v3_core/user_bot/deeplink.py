"""Pure parser for public V3 channel listing deep links.

Legacy payloads stay valid. New payloads may append a compact source code after
``__`` so acquisition attribution can survive the Telegram /start boundary.
"""
from __future__ import annotations

from dataclasses import dataclass
import re

from v3_core.publishing.public_ids import normalize_public_id


PUBLIC_CHANNEL_ACTIONS = ("details", "photos", "book")
SOURCE_CODE_MAP = {
    "ch": "channel_listing",
    "sr": "search_result",
    "dt": "listing_details",
    "ph": "listing_photos",
    "ap": "appointment_success",
}
_PROPERTY_RE = re.compile(
    r"^property_(.+)_(details|photos|book)(?:__([a-z0-9]{1,8}))?$",
    re.I,
)


@dataclass(frozen=True)
class PublicChannelRoute:
    action: str
    public_listing_id: str
    source: str = "channel_deeplink"
    source_code: str = ""


def parse_channel_start_payload(payload: object) -> PublicChannelRoute | None:
    """Parse an official V3 listing payload without DB access or side effects."""
    raw = str(payload or "").strip()
    match = _PROPERTY_RE.fullmatch(raw)
    if match is None:
        return None
    public_id = normalize_public_id(match.group(1))
    if public_id is None:
        return None
    source_code = str(match.group(3) or "").lower()
    return PublicChannelRoute(
        action=match.group(2).lower(),
        public_listing_id=public_id,
        source=SOURCE_CODE_MAP.get(source_code, "channel_deeplink"),
        source_code=source_code,
    )


__all__ = [
    "PUBLIC_CHANNEL_ACTIONS",
    "SOURCE_CODE_MAP",
    "PublicChannelRoute",
    "parse_channel_start_payload",
]
