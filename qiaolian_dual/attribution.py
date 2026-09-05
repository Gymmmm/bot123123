"""客户入口归因：首次来源冻结，latest_touch 更新。"""
from __future__ import annotations
from typing import Any

SOURCE_TYPE_ZH = {
    "channel_listing_detail": "频道房源详情",
    "channel_listing_photos": "频道更多实拍",
    "channel_listing_book": "频道预约看房",
    "channel_index_area": "频道按区域找房",
    "channel_index_budget": "频道按预算找房",
    "channel_index_layout": "频道按户型找房",
    "channel_latest": "频道最新房源",
    "channel_advisor": "频道联系顾问",
    "bot_direct_start": "Bot 直接打开",
    "bot_search": "Bot 找房",
    "bot_listing_consult": "Bot 房源咨询",
    "bot_listing_book": "Bot 预约看房",
    "bot_assurance": "侨联保障",
    "bot_move_in_service": "入住服务",
    "bot_repair": "报修",
    "bot_property": "物业协调",
    "bot_local_service": "本地生活",
    "other": "其他",
}
ADMIN_SOURCE_GROUP_ZH = {
    "channel_listing_detail": "频道房源详情",
    "channel_listing_photos": "频道更多实拍",
    "channel_listing_book": "频道预约看房",
    "channel_index_area": "Bot 找房",
    "channel_index_budget": "Bot 找房",
    "channel_index_layout": "Bot 找房",
    "channel_latest": "频道房源详情",
    "channel_advisor": "Bot 房源咨询",
    "bot_direct_start": "其他",
    "bot_search": "Bot 找房",
    "bot_listing_consult": "Bot 房源咨询",
    "bot_listing_book": "Bot 房源咨询",
    "bot_assurance": "侨联保障",
    "bot_move_in_service": "入住服务",
    "bot_repair": "入住服务",
    "bot_property": "入住服务",
    "bot_local_service": "入住服务",
    "other": "其他",
}
LEAD_STATUS_ZH = {
    "new": "新咨询", "followup": "待跟进", "claimed": "已接手",
    "contacted": "已联系", "booked": "已预约", "done": "已完成",
    "converted": "已完成", "invalid": "无效",
}
_INDEX = {
    "find_area": "channel_index_area", "index_area": "channel_index_area", "area_index": "channel_index_area",
    "find_budget": "channel_index_budget", "index_budget": "channel_index_budget",
    "find_layout": "channel_index_layout", "index_layout": "channel_index_layout",
    "latest": "channel_latest", "index_latest": "channel_latest",
    "advisor": "channel_advisor", "index_advisor": "channel_advisor",
    "consult_general": "channel_advisor", "cooperate": "channel_advisor",
}
_CH_ACTION = {
    "details": "channel_listing_detail", "detail": "channel_listing_detail", "photos": "channel_listing_photos",
    "book": "channel_listing_book", "appoint": "channel_listing_book", "consult": "channel_listing_detail",
}
_BOT_ACTION = {
    "listing_detail_view": "bot_listing_consult", "consult_menu_click": "bot_listing_consult",
    "consult_click": "bot_listing_consult", "appointment_click": "bot_listing_book",
    "appointment_submit": "bot_listing_book", "photos_click": "bot_listing_consult",
    "smart_search": "bot_search", "find_home": "bot_search", "assurance_view": "bot_assurance",
    "rental_hub": "bot_assurance", "service_hub": "bot_move_in_service", "repair_submit": "bot_repair",
    "property_request": "bot_property", "local_service": "bot_local_service",
}

def source_type_zh(v): return SOURCE_TYPE_ZH.get(str(v or "").strip() or "other", "其他")
def admin_source_group_zh(v): return ADMIN_SOURCE_GROUP_ZH.get(str(v or "").strip() or "other", "其他")
def lead_status_zh(v): return LEAD_STATUS_ZH.get(str(v or "new").strip() or "new", "新咨询")
def entry_action_zh(v):
    return {"details": "房源详情", "photos": "更多实拍", "book": "预约看房", "appoint": "预约看房",
            "consult": "联系我们", "consult_click": "联系我们", "consult_menu_click": "联系我们",
            "listing_detail_view": "房源详情", "appointment_submit": "提交预约",
            "direct_start": "打开 Bot", "discussion_entry": "历史讨论区入口"}.get(str(v or ""), "其他")

def public_deep_link_ok(arg: str) -> bool:
    raw = str(arg or "").strip()
    if raw in {"find_area", "find_budget", "find_layout", "latest", "advisor"}:
        return True
    return raw.startswith("property_") and raw.endswith(("_details", "_photos", "_book"))

def classify_start_arg(arg: str | None) -> dict[str, Any]:
    raw = str(arg or "").strip()
    out = {"source_type": "bot_direct_start", "source_detail": raw or "bot_direct_start",
           "entry_action": "direct_start", "deep_link_payload": raw, "listing_hint": "",
           "channel_message_id": None, "legacy": False}
    if not raw:
        return out
    from .session_deeplink import parse_start_arg_payload
    parsed = parse_start_arg_payload(raw) or {}
    action = str(parsed.get("action") or "")
    out["listing_hint"] = str(parsed.get("target") or "")
    out["channel_message_id"] = parsed.get("channel_message_id")
    out["entry_action"] = action or "direct_start"
    if action == "discussion_entry" or raw.startswith("discussion_entry"):
        out.update(source_type="channel_listing_detail", source_detail="legacy_discussion_entry",
                   legacy=True, entry_action="discussion_entry")
        return out
    if raw in _INDEX or action in _INDEX:
        out["source_type"] = _INDEX.get(raw) or _INDEX[action]
        return out
    if action in _CH_ACTION or raw.startswith("property_"):
        out["source_type"] = _CH_ACTION.get(action, "channel_listing_detail")
        return out
    out["source_type"] = "bot_search" if action == "find_home" else "other"
    return out

def classify_bot_event(*, action: str = "", source: str = "", payload: dict | None = None) -> dict[str, Any]:
    pack = dict(payload or {}) if isinstance(payload, dict) else {}
    action = str(action or pack.get("first_touch_action") or "")
    source = str(source or "")
    start_arg = str(pack.get("start_arg") or pack.get("deep_link_payload") or "")
    if start_arg:
        classified = classify_start_arg(start_arg)
        if action in _BOT_ACTION:
            classified["latest_override"] = _BOT_ACTION[action]
            classified["entry_action"] = action
        return classified
    if source.startswith("channel") or source == "channel_deeplink":
        st = "channel_listing_photos" if action in {"photos_click", "photos"} else (
            "channel_listing_book" if action in {"appointment_click", "appointment_submit", "book", "appoint"} else "channel_listing_detail")
        return {"source_type": st, "source_detail": source, "entry_action": action or "details",
                "deep_link_payload": start_arg, "listing_hint": str(pack.get("listing_id") or ""),
                "channel_message_id": pack.get("channel_message_id"), "legacy": False}
    st = _BOT_ACTION.get(action)
    if st is None:
        if source == "listing_card" and action in {"appointment_submit", "appointment_click"}:
            st = "bot_listing_book"
        elif source in {"listing_card", "listing_landing"}:
            st = "bot_listing_consult"
        elif "search" in source or source == "user_search":
            st = "bot_search"
        else:
            st = "other"
    return {"source_type": st, "source_detail": source or action or "unknown", "entry_action": action or "other",
            "deep_link_payload": start_arg, "listing_hint": str(pack.get("listing_id") or ""),
            "channel_message_id": pack.get("channel_message_id"), "legacy": action == "discussion_entry"}

def merge_touch(existing, incoming, *, user_id, username="", display_name="", listing_id="", now=""):
    from .session_deeplink import now_ts
    stamp = now or now_ts()
    incoming = dict(incoming)
    latest_override = str(incoming.pop("latest_override", "") or "")
    latest = str(latest_override or incoming.get("source_type") or "other")
    latest_detail = latest if latest_override else str(incoming.get("source_detail") or latest)
    first = dict(existing or {})
    listing = str(listing_id or incoming.get("listing_hint") or first.get("listing_id") or "")
    if first.get("first_source_type"):
        first_type, first_at = first["first_source_type"], first.get("first_entry_at") or stamp
        first_detail = first.get("first_source_detail") or ""
        first_legacy = bool(first.get("first_legacy"))
        first_link = first.get("first_deep_link") or incoming.get("deep_link_payload") or ""
    else:
        first_type = str(incoming.get("source_type") or "other")
        first_detail = str(incoming.get("source_detail") or first_type)
        first_at, first_legacy = stamp, bool(incoming.get("legacy"))
        first_link = str(incoming.get("deep_link_payload") or "")
    return {
        "user_id": int(user_id or 0), "username": username, "display_name": display_name,
        "source_type": latest, "source_detail": latest_detail,
        "listing_id": listing, "channel_message_id": incoming.get("channel_message_id") or first.get("channel_message_id"),
        "deep_link_payload": str(incoming.get("deep_link_payload") or first.get("latest_deep_link") or ""),
        "entry_action": str(incoming.get("entry_action") or ""),
        "first_source_type": first_type, "first_source_detail": first_detail, "first_entry_at": first_at,
        "first_listing_id": first.get("first_listing_id") or listing, "first_deep_link": first_link,
        "first_entry_action": first.get("first_entry_action") or incoming.get("entry_action") or "",
        "first_legacy": first_legacy, "latest_source_type": latest,
        "latest_source_detail": latest_detail, "latest_touch_at": stamp,
        "latest_listing_id": listing, "latest_deep_link": str(incoming.get("deep_link_payload") or ""),
        "latest_entry_action": str(incoming.get("entry_action") or ""),
        "legacy": bool(incoming.get("legacy") or first_legacy),
    }

def remember_touch(user, *, action="", source="", listing_id="", payload=None, start_arg=""):
    from .attribution_store import get_user_attribution, upsert_user_attribution
    from .session_deeplink import now_ts, user_display_name
    if user is None or not getattr(user, "id", None):
        return merge_touch(None, {"source_type": "other", "source_detail": "unknown"}, user_id=0, listing_id=listing_id, now=now_ts())
    pack = dict(payload or {}) if isinstance(payload, dict) else {}
    if start_arg:
        pack.setdefault("start_arg", start_arg)
        incoming = classify_start_arg(start_arg)
        if action:
            incoming["entry_action"] = action
    else:
        incoming = classify_bot_event(action=action, source=source, payload=pack)
    merged = merge_touch(get_user_attribution(int(user.id)), incoming, user_id=int(user.id),
                         username=getattr(user, "username", "") or "", display_name=user_display_name(user),
                         listing_id=listing_id or str(pack.get("listing_id") or incoming.get("listing_hint") or ""), now=now_ts())
    upsert_user_attribution(merged)
    return merged

def attach_to_lead_payload(payload, touch):
    pack = dict(payload or {}) if isinstance(payload, dict) else {}
    pack.update({
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
    })
    return pack
