"""Final public channel listing caption contract.

The channel post is discovery-only: four compact fact/status lines plus factual
hashtags. Only the title and rent are bold. Detailed costs, availability,
highlights and internal identifiers belong in the detail flow, not the feed.
"""
from __future__ import annotations

import html
import re
from typing import Any, Iterable

from .channel_links import public_qc_code
from .utils_formatting import _display_floor, _display_layout

_STATUS_LABELS = {
    "active": "🟢 当前可预约",
    "reserved": "🟡 已有预约 · 仍可预约",
    "pending": "🔵 房态待确认",
    "rented": "🔴 已租出",
    "inactive": "⚫ 已下架",
    "offline": "⚫ 已下架",
}
_ROOM_TAGS = {
    1: "#一房",
    2: "#两房",
    3: "#三房",
    4: "#四房",
    5: "#五房",
    6: "#六房",
    7: "#七房",
    8: "#八房",
    9: "#九房",
    10: "#十房",
}
_GENERIC_HEADINGS = {
    "侨联地产", "侨联精选", "精选房源", "优质房源", "房源", "金边房源"
}


def _clean(value: Any, limit: int = 32) -> str:
    text = re.sub(r"\s+", " ", str(value or "").strip())
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


def _price_bucket(price: Any, deal_type: str) -> str:
    if deal_type != "rent":
        return ""
    try:
        amount = int(float(price))
    except (TypeError, ValueError):
        return ""
    if amount <= 0:
        return ""
    if amount < 500:
        return "#租金500以下"
    if amount < 800:
        return "#租金500至800"
    if amount < 1200:
        return "#租金800至1200"
    if amount < 2000:
        return "#租金1200至2000"
    if amount < 3000:
        return "#租金2000至3000"
    return "#租金3000以上"


def _safe_hashtag(value: str) -> str:
    token = re.sub(r"[^0-9A-Za-z\u4e00-\u9fff]+", "", str(value or ""))
    return f"#{token}" if token else ""


def _dedupe(values: Iterable[str]) -> list[str]:
    out: list[str] = []
    for value in values:
        value = str(value or "").strip()
        if value and value not in out:
            out.append(value)
    return out


def _factual_tags(d: dict, *, heading: str, layout: str, deal_type: str) -> list[str]:
    existing = d.get("tags") if isinstance(d.get("tags"), list) else []
    existing = [str(tag).strip() for tag in existing if str(tag).strip().startswith("#")]

    location_tag = _safe_hashtag(heading) if heading and heading not in _GENERIC_HEADINGS else ""
    room_tag = ""
    room_match = re.search(r"(\d+)\s*房", layout)
    if room_match:
        room_tag = _ROOM_TAGS.get(int(room_match.group(1)), "")

    rent_market_tag = "#金边租房" if deal_type == "rent" else ""
    return _dedupe([
        location_tag,
        room_tag,
        rent_market_tag,
        _price_bucket(d.get("price"), deal_type),
        *existing,
    ])


def format_channel_listing_post(
    d: dict,
    listing_id: str = "",
    *,
    status: str | None = None,
    appointment_count: int = 0,
    extra_tags: Iterable[str] | None = None,
) -> str:
    """Render the locked channel discovery caption.

    Format:
      title
      rent + optional size/floor
      optional deposit/contract
      public status + QC
      factual hashtags
    """
    project = _clean(d.get("project") or d.get("project_name"), 24)
    area = _clean(d.get("public_location_display") or d.get("area"), 24)
    heading = project if project and project not in _GENERIC_HEADINGS else (area or "金边房源")

    property_type = _clean(d.get("property_type"), 16)
    layout = _clean(
        _display_layout(d.get("layout") or d.get("room_type") or property_type or "整租", property_type),
        18,
    )

    deal_type = str(d.get("deal_type") or "rent").strip().lower()
    try:
        amount = int(float(d.get("price")))
    except (TypeError, ValueError):
        amount = 0
    if amount > 0:
        price_text = f"${amount:,}" + ("/月" if deal_type != "sale" else "")
    else:
        price_text = "售价面议" if deal_type == "sale" else "租金面议"

    size = _display_size(d.get("size") or d.get("size_sqm"))
    floor = _display_floor(_clean(d.get("floor"), 16))
    secondary = "｜".join(value for value in (size, floor) if value)

    deposit = _clean(d.get("payment_terms") or d.get("deposit") or d.get("deposit_rule"), 18)
    contract = _clean(d.get("contract_term"), 14)
    rental = "｜".join(value for value in (deposit, contract) if value)

    effective_status = str(status if status is not None else d.get("status") or "active").strip().lower()
    if effective_status == "pending" and int(appointment_count or 0) >= 5:
        status_text = "🔵 已有5份预约看房，房态待确认"
    else:
        status_text = _STATUS_LABELS.get(effective_status, "🔵 房态待确认")

    qc_code = public_qc_code(listing_id or d.get("listing_id") or d.get("property_id") or "")
    title_line = f"🏠 <b>{html.escape(heading)}｜{html.escape(layout)}</b>"
    price_line = f"💰 <b>{html.escape(price_text)}</b>"
    if secondary:
        price_line += f"　{html.escape(secondary)}"

    lines = [title_line, price_line]
    if rental:
        lines.append(f"🔑 {html.escape(rental)}")
    lines.append(f"{status_text}　{html.escape(qc_code)}" if qc_code else status_text)

    tags = _factual_tags(d, heading=heading, layout=layout, deal_type=deal_type)
    if extra_tags:
        tags = _dedupe([*tags, *(str(tag).strip() for tag in extra_tags if str(tag).strip().startswith("#"))])
    if tags:
        lines.extend(["", " ".join(tags)])
    return "\n".join(lines).strip()[:1024]


__all__ = ["format_channel_listing_post"]
