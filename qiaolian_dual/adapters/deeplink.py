"""Adapter 4: modern start payloads -> 3858 route actions."""

from __future__ import annotations

import re

_PROPERTY = re.compile(
    r"^property_(.+)_(details|photos|book|contact)$",
    re.IGNORECASE,
)
_BOOK_VIDEO = re.compile(r"^book_video_(.+)$", re.IGNORECASE)

_ACTION_MAP = {
    "details": "details",
    "photos": "photos",
    "book": "book",
    "contact": "consult",
}


class DeeplinkAdapter:
    def parse(self, arg: str) -> dict | None:
        raw = str(arg or "").strip()
        if not raw:
            return None
        m = _PROPERTY.fullmatch(raw)
        if m:
            target, action = m.group(1), m.group(2).lower()
            return {
                "action": _ACTION_MAP[action],
                "target": target,
                "source": "channel",
                "channel_return": True,
            }
        m = _BOOK_VIDEO.fullmatch(raw)
        if m:
            return {
                "action": "video",
                "target": m.group(1),
                "source": "channel",
                "channel_return": True,
            }
        return None
