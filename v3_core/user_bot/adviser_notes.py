"""Evidence-only adviser notes for V3 public listing details.

Tone: Phnom Penh Chinese-agent colloquial — short, useful, never brochure-speak.
Prefer one line. Never invent fees or amenities. Skip facts already obvious on
the details card (type / size / floor) unless there is a real extra signal.
"""
from __future__ import annotations

import json
import re
from typing import Any

from .public_inventory import PublishedListingView


_EMPTY = {"", "待确认", "暂无", "未知", "--", "-", "null", "none"}


def _maps(listing: dict[str, Any]):
    yield listing
    for key in ("canonical_facts", "normalized_data", "extracted_data"):
        raw = listing.get(key)
        if isinstance(raw, dict):
            yield raw
        elif raw:
            try:
                parsed = json.loads(str(raw))
                if isinstance(parsed, dict):
                    yield parsed
            except (TypeError, ValueError, json.JSONDecodeError):
                pass


def _fact(listing: dict[str, Any], *keys: str) -> str:
    for mapping in _maps(listing):
        for key in keys:
            value = str(mapping.get(key) or "").strip()
            if value and value.lower() not in _EMPTY:
                return value
    return ""


def _price_display(raw: str) -> str:
    text = str(raw or "").strip()
    if not text:
        return ""
    if text.startswith("$") or "美元" in text:
        return text.replace("美元", "").strip()
    if re.fullmatch(r"\d+(?:\.\d+)?", text):
        return f"${text}"
    return text


def _floor_display(raw: str) -> str:
    text = str(raw or "").strip()
    if not text:
        return ""
    if re.fullmatch(r"\d{1,3}", text):
        return f"{text}楼"
    return text


def _included_bits(listing: dict[str, Any]) -> list[str]:
    bits: list[str] = []
    for key, label in (("management_fee", "物业费"), ("internet_fee", "网费")):
        token = _fact(listing, key).lower().replace(" ", "")
        if token in {"包含", "已包含", "包", "included", "免费"}:
            bits.append(f"{label}包了")
    return bits


def _highlight_bits(listing: dict[str, Any]) -> list[str]:
    highlights = listing.get("highlights") or []
    if isinstance(highlights, str):
        highlights = [highlights]
    return [str(value).strip() for value in highlights if str(value).strip()][:2]


def _value_line(listing: dict[str, Any]) -> str:
    included = _included_bits(listing)
    if not included:
        return ""
    price = _price_display(_fact(listing, "price", "rent"))
    head = f"月租 {price}，" if price else ""
    return head + "、".join(included) + "。"


def _highlight_line(listing: dict[str, Any]) -> str:
    bits = _highlight_bits(listing)
    if not bits:
        return ""
    return f"资料写了{'、'.join(bits)}，实拍再确认就行。"


def _location_line(listing: dict[str, Any]) -> str:
    area = _fact(listing, "area", "district")
    project = _fact(listing, "project", "building")
    if area and project and area != project:
        return f"{project}在{area}这边，通勤自己看着办。"
    if project:
        return f"在{project}，建议先翻实拍再约看。"
    if area:
        return f"位置在{area}，具体门牌看房时再对。"
    return ""


def _building_line(listing: dict[str, Any]) -> str:
    """Only used when there are no stronger signals; keep very short."""
    if _highlight_bits(listing) or _included_bits(listing):
        return ""
    size = _fact(listing, "size_sqm", "area_sqm")
    floor = _floor_display(_fact(listing, "floor"))
    if size and "㎡" not in size and re.fullmatch(r"\d+(?:\.\d+)?", size):
        size = f"{size}㎡"
    bits = [value for value in (size, floor) if value]
    if len(bits) < 2:
        return ""
    return f"大概 {' / '.join(bits)}，现场再看空间。"


def generate_adviser_notes(
    listing: dict[str, Any],
    max_points: int = 1,
    allow_empty: bool = True,
) -> str:
    """Generate short colloquial adviser lines; default one sentence."""
    candidates = [
        line
        for line in (
            _value_line(listing),
            _highlight_line(listing),
            _location_line(listing),
            _building_line(listing),
        )
        if line
    ]
    if not candidates:
        return "" if allow_empty else "条件以资料和现场核对为准。"
    return "\n".join(candidates[: max(0, int(max_points))])


def frozen_adviser_evidence(view: PublishedListingView) -> dict[str, Any]:
    """Project frozen V3 facts into the adviser-note vocabulary."""
    snapshot = view.snapshot
    if str(snapshot.get("schema") or "") != "v3_publication_snapshot.v1":
        raise ValueError("frozen_public_snapshot_missing")
    canonical = snapshot.get("canonical_facts")
    if not isinstance(canonical, dict):
        raise ValueError("frozen_canonical_facts_missing")

    listing = view.frozen_listing
    offer = view.frozen_offer
    evidence: dict[str, Any] = {
        "area": str(listing.get("public_location_display") or "").strip(),
        "project": str(listing.get("project_name") or "").strip(),
        "property_type": str(listing.get("property_type") or "").strip(),
        "size_sqm": listing.get("size_sqm"),
        "floor": listing.get("floor"),
        "price": offer.get("monthly_rent_usd"),
        "canonical_facts": dict(canonical),
    }

    highlights = canonical.get("highlights")
    if highlights not in (None, "", []):
        evidence["highlights"] = highlights

    raw_included = canonical.get("included")
    included = (
        {str(item).strip().lower() for item in raw_included if str(item).strip()}
        if isinstance(raw_included, list)
        else set()
    )
    if "物业费" in included:
        evidence["management_fee"] = "包含"
    if included.intersection({"wi-fi", "wifi", "网络费", "网络"}):
        evidence["internet_fee"] = "包含"

    return evidence


def adviser_notes_for_view(
    view: PublishedListingView,
    *,
    max_points: int = 1,
    allow_empty: bool = True,
) -> str:
    return generate_adviser_notes(
        frozen_adviser_evidence(view),
        max_points=max_points,
        allow_empty=allow_empty,
    )


__all__ = [
    "adviser_notes_for_view",
    "frozen_adviser_evidence",
    "generate_adviser_notes",
]
