"""Final V3 public channel caption renderer.

The renderer consumes already materialized listing/offer facts and emits only
public information. It performs no DB lookup and never exposes internal ids.
"""
from __future__ import annotations

import re
from typing import Any

from v3_core.presentation.location_display import display_location

from .formatting import display_floor, display_layout, display_property_type
from .public_ids import normalize_public_id

_EMPTY_FACTS = {
    "", "—", "-", "--", "暂无", "[暂无]", "未知", "待确认", "待定", "none", "null", "unknown",
}
_GENERIC_HEADINGS = {"侨联地产", "侨联精选", "精选房源", "优质房源", "房源", "金边房源"}

_CHANNEL_STATUS_PRESENTATION = {
    "active": ("🟢", "当前可预约"),
    "reserved": ("🟡", "已有预约，仍可预约"),
    "high_demand": ("🟠", "预约较多"),
    "pending": ("🔵", "房态确认中"),
    "rented": ("🔴", "已租出"),
    "inactive": ("⚫", "已下架"),
    "offline": ("⚫", "已下架"),
    "withdrawn": ("⚫", "已下架"),
}


def channel_status_presentation(status: object) -> tuple[str, str]:
    return _CHANNEL_STATUS_PRESENTATION.get(
        str(status or "").strip().lower(),
        ("🔵", "房态确认中"),
    )


def _clean(value: Any, limit: int = 40) -> str:
    text = re.sub(r"\s+", " ", str(value or "").strip()).replace("|", "｜")
    if text.lower() in _EMPTY_FACTS or text in _EMPTY_FACTS:
        return ""
    return text[:limit]


def _display_size(value: Any) -> str:
    raw = _clean(value, 18)
    if not raw:
        return ""
    normalized = raw.replace("平方米", "㎡").replace("平米", "㎡")
    normalized = re.sub(r"(?<=\d)平$", "㎡", normalized)
    if re.fullmatch(r"\d+(?:\.\d+)?", normalized):
        normalized += "㎡"
    return normalized


def _normalize_contract(value: Any) -> str:
    text = _clean(value, 20)
    if not text:
        return ""
    return re.sub(r"^(?:租期|合同)\s*[:：]?\s*", "", text)


def _status_line(status: str, public_id: str) -> str:
    icon, label = channel_status_presentation(status)
    return f"{icon} {label}　{public_id}"


def _hashtag(value: str) -> str:
    text = re.sub(r"[^0-9A-Za-z\u4e00-\u9fff]+", "", str(value or "").strip())
    return f"#{text}" if text else ""


def _layout_tag(value: Any, property_type: Any = "") -> str:
    raw = _clean(display_layout(value, property_type), 20)
    if raw == "单间":
        return "单间"
    match = re.search(r"\d+(?:[+＋]\d+)?房", raw)
    return match.group(0).replace("+", "加").replace("＋", "加") if match else raw


def _price_tag(amount: int) -> str:
    if amount <= 0:
        return ""
    if amount <= 400:
        return "400以下"
    if amount <= 600:
        return "400至600"
    if amount <= 800:
        return "600至800"
    if amount <= 1200:
        return "800至1200"
    if amount <= 1500:
        return "1200至1500"
    return "1500以上"


def render_channel_caption(*, listing: dict[str, Any], offer: dict[str, Any], public_listing_id: object, status: str | None = None, adviser_note: str = "") -> str:
    public_id = normalize_public_id(public_listing_id)
    if not public_id:
        raise ValueError("valid public_listing_id is required")
    project = _clean(listing.get("project_name") or listing.get("project"), 28)
    area = _clean(display_location(listing.get("public_location_display") or listing.get("area"), project=project), 28)
    property_type_raw = _clean(listing.get("property_type"), 24)
    property_type = _clean(display_property_type(property_type_raw), 24)
    layout = _clean(display_layout(listing.get("layout") or "", property_type), 20)
    offer_type = str(offer.get("offer_type") or "rent").strip().lower()
    price = offer.get("monthly_rent_usd") if offer_type == "rent" else offer.get("sale_price_usd")
    try:
        amount = int(float(price or 0))
    except (TypeError, ValueError):
        amount = 0
    price_text = f"${amount:,}" + ("/月" if offer_type == "rent" else "") if amount > 0 else ""
    effective_status = str(status if status is not None else listing.get("inventory_status") or "pending")
    icon, status_label = channel_status_presentation(effective_status)
    if status_label == "当前可预约":
        status_label = "可预约"

    lines: list[str] = []
    home_bits = [part for part in (property_type, layout) if part]
    if home_bits:
        lines.append(f"🏡 {'｜'.join(home_bits)}")
    location = project or area
    if project and area and not display_location(area, project=project) == "":
        location = "｜".join(dict.fromkeys(part for part in (project, area) if part))
    if location:
        lines.append(f"📍 {location}")
    if price_text:
        lines.append(f"💵 {price_text}")
    deposit = _clean(offer.get("payment_terms") or offer.get("deposit_terms"), 20)
    contract = _normalize_contract(offer.get("contract_term"))
    terms = "｜".join(value for value in (deposit, contract) if value)
    if offer_type == "rent" and terms:
        lines.append(f"🔑 {terms}")
    lines.append(f"{icon} {status_label}")
    lines.extend(["", public_id])
    return "\n".join(lines).strip()[:1024]

