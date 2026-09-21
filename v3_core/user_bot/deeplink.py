"""Pure parsers for public V3 Telegram /start deep links."""
from __future__ import annotations

from dataclasses import dataclass
import re

from v3_core.publishing.public_ids import normalize_public_id


PUBLIC_CHANNEL_ACTIONS = ("details", "photos", "book", "contact")
AREA_START_SLUGS = {
    "bkk1": "BKK1",
    "bkk2": "BKK2",
    "bkk3": "BKK3",
    "koh_pich": "钻石岛",
    "tk": "TK/7月区",
    "fuli": "富力城",
    "bingfa": "炳发城",
    "gold_street": "金街",
    "chroy_changvar": "水净华",
    "russian_market": "俄罗斯市场",
    "aeon1": "永旺商圈",
}
SOURCE_CODE_MAP = {
    "ch": "channel_listing",
    "sr": "search_result",
    "dt": "listing_details",
    "ph": "listing_photos",
    "ap": "appointment_success",
}
_PROPERTY_RE = re.compile(
    r"^property_(.+)_(details|photos|book|contact)(?:__([a-z0-9]{1,8}))?$",
    re.I,
)


@dataclass(frozen=True)
class PublicChannelRoute:
    action: str
    public_listing_id: str
    source: str = "channel_deeplink"
    source_code: str = ""


@dataclass(frozen=True)
class PublicSearchStartRoute:
    action: str
    area_slug: str = ""
    location_key: str = ""


def parse_search_start_payload(payload: object) -> PublicSearchStartRoute | None:
    raw = str(payload or "").strip().lower()
    if raw == "find":
        return PublicSearchStartRoute(action="find")
    if not raw.startswith("more_"):
        return None
    slug = raw[len("more_") :]
    location_key = AREA_START_SLUGS.get(slug)
    if not location_key:
        return None
    return PublicSearchStartRoute(
        action="more",
        area_slug=slug,
        location_key=location_key,
    )


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
    "AREA_START_SLUGS",
    "PUBLIC_CHANNEL_ACTIONS",
    "SOURCE_CODE_MAP",
    "PublicChannelRoute",
    "PublicSearchStartRoute",
    "parse_channel_start_payload",
    "parse_search_start_payload",
]
