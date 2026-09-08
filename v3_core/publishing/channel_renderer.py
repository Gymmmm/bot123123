"""Final V3 public channel caption renderer.

Extracted from ``qiaolian_dual.channel_post``.  The V3 renderer receives one
listing, one offer and an already-assigned public QL id; it performs no DB
lookup and exposes no internal listing/draft identifiers.
"""
from __future__ import annotations

import html
import re
from typing import Any, Iterable

from .formatting import display_floor, display_layout
from .public_ids import normalize_public_id

_STATUS_LABELS = {
    "active": "🟢 当前可预约",
    "reserved": "🟡 已有预约 · 仍可预约",
    "pending": "🔵 房态待确认",
    "rented": "🔴 已租出",
    "inactive": "⚫ 已下架",
    "offline": "⚫ 已下架",
}
_GENERIC_HEADINGS = {"侨联地产", "侨联精选", "精选房源", "优质房源", "房源", "金边房源"}
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
    "面议",
    "租金面议",
    "售价面议",
    "价格待确认",
    "随时入住",
    "即起",
    "现在",
    "立即",
}


def _clean(value: Any, limit: int = 32) -> str:
    text = re.sub(r"\s+", " ", str(value or "").strip()).replace("|", "｜")
    if text in _EMPTY_FACTS or text in _GENERIC_HEADINGS:
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


def _price_bucket(price: Any) -> str:
    try:
        amount = int(float(price))
    except (TypeError, ValueError):
        return ""
    if amount <= 0:
        return ""
    if amount < 500:
        return "#租金500以下"
    if amount < 1000:
        return "#租金500至1000"
    if amount < 1500:
        return "#租金1000至1500"
    if amount < 2000:
        return "#租金1500至2000"
    if amount < 3000:
        return "#租金2000至3000"
    return "#租金3000以上"


def _safe_hashtag(value: str) -> str:
    token = re.sub(r"[^0-9A-Za-z\u4e00-\u9fff]+", "", str(value or ""))
    if not token or token in {"公寓", "金边"}:
        return ""
    return f"#{token}"


def _dedupe(values: Iterable[str]) -> list[str]:
    out: list[str] = []
    for value in values:
        value = str(value or "").strip()
        if value and value not in out:
            out.append(value)
    return out


def _layout_tag(layout: str) -> str:
    clean = str(layout or "").strip()
    if not clean:
        return ""
    if re.search(r"(单间|开间|studio)", clean, flags=re.I):
        return "#单间"
    match = re.search(r"(\d+)\s*房", clean)
    if match:
        number = int(match.group(1))
        cn = {1: "一", 2: "两", 3: "三", 4: "四", 5: "五"}.get(number, str(number))
        return f"#{cn}房"
    for cn in ("一", "两", "二", "三", "四", "五"):
        if f"{cn}房" in clean:
            return f"#{'两' if cn == '二' else cn}房"
    return ""


def _normalize_contract(value: Any) -> str:
    text = _clean(value, 14)
    if not text:
        return ""
    return re.sub(r"^租期\s*", "", text)


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

    project = _clean(listing.get("project_name") or listing.get("project"), 24)
    area = _clean(listing.get("public_location_display") or listing.get("area"), 24)
    heading = project if project and project not in _GENERIC_HEADINGS else (area or "金边房源")
    property_type = _clean(listing.get("property_type"), 16)
    layout = _clean(
        display_layout(
            listing.get("layout") or property_type or "整租",
            property_type,
        ),
        18,
    )

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
    price_text = (
        f"${amount:,}" + ("/月" if offer_type == "rent" else "")
        if amount > 0
        else ""
    )

    size = _display_size(listing.get("size_sqm") or listing.get("size"))
    floor = display_floor(_clean(listing.get("floor"), 16))
    property_line = "｜".join(
        value for value in (property_type, size, floor) if value
    )
    deposit = _clean(
        offer.get("payment_terms") or offer.get("deposit_terms"), 18
    )
    contract = _normalize_contract(offer.get("contract_term"))
    rental = "｜".join(
        value
        for value in (deposit, f"租期{contract}" if contract else "")
        if value
    )

    effective_status = str(
        status if status is not None else listing.get("inventory_status") or "active"
    ).strip().lower()
    status_text = _STATUS_LABELS.get(effective_status, "🔵 房态待确认")

    lines = [
        f"🏠 <b>{html.escape('｜'.join(part for part in (heading, layout) if part) or '金边租房')}</b>"
    ]
    if price_text:
        lines.append(f"💰 <b>{html.escape(price_text)}</b>")
    if property_line:
        lines.extend(["", f"🏢 {html.escape(property_line)}"])
    if offer_type == "rent" and rental:
        lines.append(f"🔑 {html.escape(rental)}")
    lines.extend(["", f"{status_text}　{html.escape(public_id)}"])

    tags = [
        _safe_hashtag(heading),
        _layout_tag(layout),
        _price_bucket(price) if offer_type == "rent" else "",
    ]
    tags = [tag for tag in _dedupe(tags) if tag]
    if tags:
        lines.extend(["", " ".join(tags)])
    return "\n".join(lines).strip()[:1024]


__all__ = ["render_channel_caption"]
