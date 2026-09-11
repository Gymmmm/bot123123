"""Evidence-only adviser notes for V3 public listing details.

New publications use the controlled ``侨联说`` tag engine.  Already-published
packages remain compatible: their frozen canonical facts are inspected for
explicit supported tags first, then the legacy factual wording is used when no
new-style evidence exists.  No live canonical/draft lookup is allowed here.
"""
from __future__ import annotations

import json
from typing import Any

from v3_core.inventory.qiaolian_say import (
    extract_qiaolian_tags,
    qiaolian_notes_text,
)

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


def _location(listing: dict[str, Any]) -> str:
    area = _fact(listing, "area", "district")
    project = _fact(listing, "project", "building")
    if area and project and area != project:
        return f"这套标注在{area}，项目是{project}，可以按实际通勤路线再判断。"
    if project or area:
        return f"这套位置标注为{project or area}，看房前可以先核对具体定位。"
    return ""


def _building(listing: dict[str, Any]) -> str:
    highlights = listing.get("highlights") or []
    if isinstance(highlights, str):
        highlights = [highlights]
    clean_highlights = [str(value).strip() for value in highlights if str(value).strip()][:2]
    if clean_highlights:
        return f"资料明确标注：{'、'.join(clean_highlights)}；具体状态可以结合实拍确认。"

    kind = _fact(listing, "property_type", "type")
    size = _fact(listing, "size_sqm", "area_sqm")
    floor = _fact(listing, "floor")
    facts = [
        value
        for value in (
            kind,
            f"{size}㎡" if size and "㎡" not in size else size,
            floor,
        )
        if value
    ]
    return (
        f"资料里写的是{'、'.join(facts)}，实际空间和楼层以现场为准。"
        if len(facts) >= 2
        else ""
    )


def _value(listing: dict[str, Any]) -> str:
    price = _fact(listing, "price", "rent")
    included: list[str] = []
    for key, label in (("management_fee", "物业费"), ("internet_fee", "网络费")):
        if _fact(listing, key).lower().replace(" ", "") in {
            "包含",
            "已包含",
            "包",
            "included",
            "免费",
        }:
            included.append(f"{label}已包含")
    if not included:
        return ""
    bits = (
        [f"月租为{price}" if "$" in price or "美元" in price else f"月租为${price}"]
        if price
        else []
    ) + included
    return "；".join(bits) + "。"


def generate_adviser_notes(
    listing: dict[str, Any],
    max_points: int = 2,
    allow_empty: bool = True,
) -> str:
    """Legacy evidence-only generator kept for frozen-package compatibility."""
    has_location = bool(
        _fact(listing, "area", "district") and _fact(listing, "project", "building")
    )
    lines = [
        line
        for line in (
            _location(listing) if has_location else "",
            _building(listing),
            _value(listing),
        )
        if line
    ]
    if not lines:
        return "" if allow_empty else "具体条件以房源资料和现场核对结果为准。"
    return "\n".join(lines[: max(0, max_points)])


def frozen_adviser_evidence(view: PublishedListingView) -> dict[str, Any]:
    """Project frozen V3 facts into the legacy note vocabulary."""
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
    max_points: int = 2,
    allow_empty: bool = True,
) -> str:
    snapshot = view.snapshot
    if str(snapshot.get("schema") or "") != "v3_publication_snapshot.v1":
        raise ValueError("frozen_public_snapshot_missing")
    canonical = snapshot.get("canonical_facts")
    if not isinstance(canonical, dict):
        raise ValueError("frozen_canonical_facts_missing")

    seed = str(snapshot.get("public_listing_id") or view.listing.get("public_listing_id") or "")
    if "qiaolian_tags" in canonical or "qiaolian_say" in canonical or canonical.get("qiaolian_say_disabled"):
        return qiaolian_notes_text(
            canonical,
            seed=seed,
            limit=max_points,
            allow_fallback=not allow_empty or "qiaolian_tags" in canonical,
        )

    # Older frozen packages may still contain explicit highlights/included facts
    # that map safely to the new voice.  Use only those frozen facts.
    legacy_tags = extract_qiaolian_tags("", canonical)
    if legacy_tags:
        projected = dict(canonical)
        projected["qiaolian_tags"] = legacy_tags
        return qiaolian_notes_text(
            projected,
            seed=seed,
            limit=max_points,
            allow_fallback=False,
        )

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
