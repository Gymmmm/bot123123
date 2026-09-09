"""Human-readable display labels for persisted consultation source slugs.

The raw source value remains unchanged in lead/attribution storage.  This module
is presentation-only so administrator notifications never expose internal
routing slugs.
"""
from __future__ import annotations


SOURCE_DISPLAY_LABELS = {
    "hub": "首页联系我们",
    "listing_callback": "房源咨询",
    "daily_broadcast": "每日广播咨询",
    "user_search": "找房咨询",
    "channel": "频道房源",
    "channel_deeplink": "频道房源",
    "search_result": "找房结果",
}


def source_display_label(value: object) -> str:
    clean = str(value or "").strip().lower()
    return SOURCE_DISPLAY_LABELS.get(clean, "用户咨询")


__all__ = ["SOURCE_DISPLAY_LABELS", "source_display_label"]
