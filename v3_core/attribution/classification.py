"""Pure attribution classification extracted from locked production behavior.

V3 keeps first-touch/latest-touch semantics but does not import legacy storage or
mutate lead tables at import/runtime.  Official channel listing links are parsed
through the V3 public User Bot contract.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from v3_core.user_bot.deeplink import parse_channel_start_payload


_CHANNEL_ACTION = {
    "details": "channel_listing_detail",
    "photos": "channel_listing_photos",
    "book": "channel_listing_book",
}

_BOT_ACTION = {
    "listing_detail_view": "bot_listing_consult",
    "consult_menu_click": "bot_listing_consult",
    "consult_click": "bot_listing_consult",
    "appointment_click": "bot_listing_book",
    "appointment_submit": "bot_listing_book",
    "photos_click": "bot_listing_consult",
    "smart_search": "bot_search",
    "find_home": "bot_search",
    "assurance_view": "bot_assurance",
    "rental_hub": "bot_assurance",
    "service_hub": "bot_move_in_service",
    "repair_submit": "bot_repair",
    "property_request": "bot_property",
    "local_service": "bot_local_service",
}


def classify_public_start_arg(arg: object) -> dict[str, Any]:
    """Classify direct start or an official property_* V3 channel payload."""
    raw = str(arg or "").strip()
    out: dict[str, Any] = {
        "source_type": "bot_direct_start",
        "source_detail": raw or "bot_direct_start",
        "entry_action": "direct_start",
        "deep_link_payload": raw,
        "listing_hint": "",
        "channel_message_id": None,
        "legacy": False,
    }
    if not raw:
        return out
    route = parse_channel_start_payload(raw)
    if route is None:
        out.update(
            source_type="other",
            source_detail=raw,
            entry_action="direct_start",
        )
        return out
    out.update(
        source_type=_CHANNEL_ACTION[route.action],
        source_detail=raw,
        entry_action=route.action,
        listing_hint=route.public_listing_id,
    )
    return out


def classify_bot_event(
    *,
    action: str = "",
    source: str = "",
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    pack = dict(payload or {}) if isinstance(payload, dict) else {}
    clean_action = str(action or pack.get("first_touch_action") or "")
    clean_source = str(source or "")
    start_arg = str(pack.get("start_arg") or pack.get("deep_link_payload") or "")
    if start_arg:
        classified = classify_public_start_arg(start_arg)
        if clean_action in _BOT_ACTION:
            classified["latest_override"] = _BOT_ACTION[clean_action]
            classified["entry_action"] = clean_action
        return classified

    if clean_source.startswith("channel") or clean_source == "channel_deeplink":
        if clean_action in {"photos_click", "photos"}:
            source_type = "channel_listing_photos"
        elif clean_action in {"appointment_click", "appointment_submit", "book", "appoint"}:
            source_type = "channel_listing_book"
        else:
            source_type = "channel_listing_detail"
        return {
            "source_type": source_type,
            "source_detail": clean_source,
            "entry_action": clean_action or "details",
            "deep_link_payload": "",
            "listing_hint": str(pack.get("listing_id") or ""),
            "channel_message_id": pack.get("channel_message_id"),
            "legacy": False,
        }

    source_type = _BOT_ACTION.get(clean_action)
    if source_type is None:
        if clean_source == "listing_card" and clean_action in {"appointment_submit", "appointment_click"}:
            source_type = "bot_listing_book"
        elif clean_source in {"listing_card", "listing_landing"}:
            source_type = "bot_listing_consult"
        elif "search" in clean_source or clean_source == "user_search":
            source_type = "bot_search"
        else:
            source_type = "other"
    return {
        "source_type": source_type,
        "source_detail": clean_source or clean_action or "unknown",
        "entry_action": clean_action or "other",
        "deep_link_payload": "",
        "listing_hint": str(pack.get("listing_id") or ""),
        "channel_message_id": pack.get("channel_message_id"),
        "legacy": clean_action == "discussion_entry",
    }


def merge_touch(
    existing: dict[str, Any] | None,
    incoming: dict[str, Any],
    *,
    user_id: int,
    username: str = "",
    display_name: str = "",
    listing_id: str = "",
    now: str = "",
) -> dict[str, Any]:
    """Freeze first source while updating latest source, matching production."""
    stamp = str(now or datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    next_touch = dict(incoming or {})
    latest_override = str(next_touch.pop("latest_override", "") or "")
    latest = str(latest_override or next_touch.get("source_type") or "other")
    latest_detail = latest if latest_override else str(next_touch.get("source_detail") or latest)
    first = dict(existing or {})
    listing = str(listing_id or next_touch.get("listing_hint") or first.get("listing_id") or "")

    if first.get("first_source_type"):
        first_type = str(first["first_source_type"])
        first_at = str(first.get("first_entry_at") or stamp)
        first_detail = str(first.get("first_source_detail") or "")
        first_legacy = bool(first.get("first_legacy"))
        first_link = str(first.get("first_deep_link") or next_touch.get("deep_link_payload") or "")
    else:
        first_type = str(next_touch.get("source_type") or "other")
        first_detail = str(next_touch.get("source_detail") or first_type)
        first_at = stamp
        first_legacy = bool(next_touch.get("legacy"))
        first_link = str(next_touch.get("deep_link_payload") or "")

    return {
        "user_id": int(user_id or 0),
        "username": str(username or ""),
        "display_name": str(display_name or ""),
        "source_type": latest,
        "source_detail": latest_detail,
        "listing_id": listing,
        "channel_message_id": next_touch.get("channel_message_id") or first.get("channel_message_id"),
        "deep_link_payload": str(next_touch.get("deep_link_payload") or first.get("latest_deep_link") or ""),
        "entry_action": str(next_touch.get("entry_action") or ""),
        "first_source_type": first_type,
        "first_source_detail": first_detail,
        "first_entry_at": first_at,
        "first_listing_id": str(first.get("first_listing_id") or listing),
        "first_deep_link": first_link,
        "first_entry_action": str(first.get("first_entry_action") or next_touch.get("entry_action") or ""),
        "first_legacy": first_legacy,
        "latest_source_type": latest,
        "latest_source_detail": latest_detail,
        "latest_touch_at": stamp,
        "latest_listing_id": listing,
        "latest_deep_link": str(next_touch.get("deep_link_payload") or ""),
        "latest_entry_action": str(next_touch.get("entry_action") or ""),
        "legacy": bool(next_touch.get("legacy") or first_legacy),
    }


def attach_to_lead_payload(
    payload: dict[str, Any] | None,
    touch: dict[str, Any],
) -> dict[str, Any]:
    pack = dict(payload or {}) if isinstance(payload, dict) else {}
    pack.update(
        {
            "source_type": touch.get("latest_source_type") or "other",
            "source_detail": touch.get("latest_source_detail") or "",
            "first_source_type": touch.get("first_source_type") or "other",
            "first_source_detail": touch.get("first_source_detail") or "",
            "first_entry_at": touch.get("first_entry_at") or "",
            "latest_touch_at": touch.get("latest_touch_at") or "",
            "entry_action": touch.get("entry_action") or "",
            "deep_link_payload": touch.get("deep_link_payload") or "",
            "channel_message_id": touch.get("channel_message_id"),
            "legacy": bool(touch.get("legacy")),
        }
    )
    return pack


__all__ = [
    "attach_to_lead_payload",
    "classify_bot_event",
    "classify_public_start_arg",
    "merge_touch",
]
