"""Final V3 public channel caption renderer.

The renderer consumes already materialized listing/offer facts and emits only
public information.  It performs no DB lookup and never exposes internal ids.
"""
from __future__ import annotations

import html
import re
from typing import Any

from v3_core.status_labels import inventory_status_presentation

from .formatting import display_floor, display_layout
from .public_ids import normalize_public_id

_EMPTY_FACTS = {
    "",
    "—",
    "-",
    "--",
    "暂无",
    "[暂无]",
    "未知",
    "待确认",
    "待定",
    "none",
    "null",
    "unknown",
}
_GENERIC_HEADINGS = {"侨联地产", "侨联精选", "精选房源", "优质房源", "房源", "金边房源"}


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


def _property_line(value: Any) -> str:
    raw = _clean(value, 24)
    identity = raw.lower()
    if any(token in identity for token in ("villa", "别墅")):
        return "🏛️ 别墅"
    if any(token in identity for token in ("排屋", "townhouse", "town house", "row house", "shophouse")):
        return "🏘️ 排屋"
    if any(token in identity for token in ("公寓", "apartment", "condo", "condominium")):
        return "🏢 公寓"
    return f"🏠 {raw}" if raw else ""


def _status_line(status: str, public_id: str) -> str:
    icon, label = inventory_status_presentation(status)
    return f"{icon} {label}　{public_id}"


def render_channel_caption(
    *,
    listing: dict[str, Any],
    offer: dict[str, Any],
    public_listing_id: object,
    status: str | None = None,
) -> str:
    public_id = normalize_public_id(public_listing_id)
    if not public_id:
        raise ValueError("valid public_listing_id is required")

    project = _clean(listing.get("project_name") or listing.get("project"), 28)
    area = _clean(listing.get("public_location_display") or listing.get("area"), 28)
    heading = project if project and project not in _GENERIC_HEADINGS else area
    if not heading:
        heading = "金边房源"
    property_type = _clean(listing.get("property_type"), 24)
    layout = _clean(
        display_layout(listing.get("layout") or "", property_type),
        20,
    )
    heading_line = "｜".join(value for value in (heading, layout) if value)

    offer_type = str(offer.get("offer_type") or "rent").strip().lower()
    if offer_type == "rent":
        price = offer.get("monthly_rent_usd")
    elif offer_type == "sale":
        price = offer.get("sale_price_usd")
    else:
        raise ValueError(f"unsupported offer_type:{offer_type}")
    try:
        amount = int(float(price or 0))
    except (TypeError, ValueError):
        amount = 0
    price_text = f"${amount:,}" + ("/月" if offer_type == "rent" else "") if amount > 0 else ""

    size = _display_size(listing.get("size_sqm") or listing.get("size"))
    floor = _clean(display_floor(_clean(listing.get("floor"), 16)), 18)
    size_floor = "｜".join(value for value in (size, floor) if value)

    deposit = _clean(offer.get("payment_terms") or offer.get("deposit_terms"), 20)
    contract = _normalize_contract(offer.get("contract_term"))
    deposit_contract = "｜".join(value for value in (deposit, contract) if value)

    effective_status = str(status if status is not None else listing.get("inventory_status") or "active")
    blocks: list[str] = [f"🏡 <b>{html.escape(heading_line)}</b>"]
    if price_text:
        blocks.append(f"💵 <b>{html.escape(price_text)}</b>")
    property_display = _property_line(property_type)
    if property_display:
        blocks.append(html.escape(property_display))
    if size_floor:
        blocks.append(f"📐 {html.escape(size_floor)}")
    if offer_type == "rent" and deposit_contract:
        blocks.append(f"🗝️ {html.escape(deposit_contract)}")
    blocks.append(html.escape(_status_line(effective_status, public_id)))
    return "\n\n".join(blocks).strip()[:1024]


__all__ = ["render_channel_caption"]
