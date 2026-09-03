"""Final public channel listing caption contract.

The channel post is discovery-only: compact fact/status lines plus factual
hashtags. Only the title and rent are bold. Detailed costs, availability
notes that are not a concrete date, highlights and internal identifiers
belong in the detail flow, not the feed.
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
    text = re.sub(r"\s+", " ", str(value or "").strip())
    text = text.replace("|", "｜")
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


def _available_line(value: Any) -> str:
    raw = _clean(value, 16)
    if not raw:
        return ""
    if raw.endswith("可住") or raw.endswith("入住"):
        date = raw
    else:
        date = f"{raw}可住"
    return f"📅 {date}"


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
      optional concrete move-in date
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
    price_text = ""
    if amount > 0:
        price_text = f"${amount:,}" + ("" if deal_type == "sale" else "/月")

    size = _display_size(d.get("size") or d.get("size_sqm"))
    floor = _display_floor(_clean(d.get("floor"), 16))
    secondary = "｜".join(value for value in (size, floor) if value)

    deposit = _clean(d.get("payment_terms") or d.get("deposit") or d.get("deposit_rule"), 18)
    contract = _clean(d.get("contract_term"), 14)
    rental = "｜".join(value for value in (deposit, contract) if value)
    available = _available_line(d.get("available_date"))

    effective_status = str(status if status is not None else d.get("status") or "active").strip().lower()
    if effective_status == "pending" and int(appointment_count or 0) >= 5:
        status_text = "🔵 已有5份预约看房，房态待确认"
    else:
        status_text = _STATUS_LABELS.get(effective_status, "🔵 房态待确认")

    qc_code = public_qc_code(listing_id or d.get("listing_id") or d.get("property_id") or "")
    title_bits = [part for part in (heading, layout) if part]
    title_line = f"🏠 <b>{html.escape('｜'.join(title_bits) or '金边租房')}</b>"

    lines = [title_line]
    if price_text:
        price_line = f"💰 <b>{html.escape(price_text)}</b>"
        if secondary:
            price_line += f"　{html.escape(secondary)}"
        lines.append(price_line)
    elif secondary:
        lines.append(html.escape(secondary))
    if rental:
        lines.append(f"🔑 {html.escape(rental)}")
    if available:
        lines.append(available)
    lines.append(f"{status_text}　{html.escape(qc_code)}" if qc_code else status_text)

    tags = _factual_tags(d, heading=heading, layout=layout, deal_type=deal_type)
    if extra_tags:
        tags = _dedupe([*tags, *(str(tag).strip() for tag in extra_tags if str(tag).strip().startswith("#"))])
    tags = [tag for tag in tags if tag not in {"#公寓", "#金边"}]
    if tags:
        lines.extend(["", " ".join(tags)])
    return "\n".join(lines).strip()[:1024]


def format_button_post_text(
    d: dict,
    listing_id: str = "",
    tag_lines: Iterable[str] | None = None,
    caption_variant: str = "a",
) -> str:
    """Frozen-package entry: ignore A/B/C variants, emit the locked caption."""
    _ = caption_variant
    extra = [str(line).strip() for line in (tag_lines or []) if str(line).strip().startswith("#")]
    return format_channel_listing_post(
        d,
        listing_id=listing_id or (d.get("listing_id") if isinstance(d, dict) else "") or "",
        status=d.get("status") if isinstance(d, dict) else None,
        extra_tags=extra,
    )


__all__ = ["format_channel_listing_post", "format_button_post_text"]
