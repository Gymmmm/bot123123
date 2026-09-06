"""Evidence-only adviser notes for public listing details."""
from __future__ import annotations
import json
from typing import Any

_EMPTY = {"", "待确认", "暂无", "未知", "--", "-", "null", "none"}

def _maps(listing: dict[str, Any]):
    yield listing
    for key in ("canonical_facts", "normalized_data", "extracted_data"):
        raw = listing.get(key)
        if isinstance(raw, dict): yield raw
        elif raw:
            try:
                parsed = json.loads(str(raw))
                if isinstance(parsed, dict): yield parsed
            except (TypeError, ValueError, json.JSONDecodeError): pass

def _fact(listing: dict[str, Any], *keys: str) -> str:
    for mapping in _maps(listing):
        for key in keys:
            value = str(mapping.get(key) or "").strip()
            if value and value.lower() not in _EMPTY: return value
    return ""

def _location(listing):
    area, project = _fact(listing, "area", "district"), _fact(listing, "project", "building")
    if area and project and area != project: return f"这套标注在{area}，项目是{project}，可以按实际通勤路线再判断。"
    if project or area: return f"这套位置标注为{project or area}，看房前可以先核对具体定位。"
    return ""

def _building(listing):
    highlights = listing.get("highlights") or []
    if isinstance(highlights, str): highlights = [highlights]
    clean_highlights = [str(v).strip() for v in highlights if str(v).strip()][:2]
    if clean_highlights: return f"资料明确标注：{'、'.join(clean_highlights)}；具体状态可以结合实拍确认。"
    kind, size, floor = _fact(listing, "property_type", "type"), _fact(listing, "size_sqm", "area_sqm"), _fact(listing, "floor")
    facts = [v for v in (kind, f"{size}㎡" if size and "㎡" not in size else size, floor) if v]
    return f"资料里写的是{'、'.join(facts)}，实际空间和楼层以现场为准。" if len(facts) >= 2 else ""

def _value(listing):
    price = _fact(listing, "price", "rent")
    included = []
    for key, label in (("management_fee", "物业费"), ("internet_fee", "网络费")):
        if _fact(listing, key).lower().replace(" ", "") in {"包含", "已包含", "包", "included", "免费"}: included.append(f"{label}已包含")
    if not included: return ""
    bits = ([f"月租为{price}" if "$" in price or "美元" in price else f"月租为${price}"] if price else []) + included
    return "；".join(bits) + "。"

def generate_talk(listing: dict[str, Any], max_points: int = 2, allow_empty: bool = True) -> str:
    """Generate only 区位型、楼宇型、性价比型 factual lines."""
    has_location = bool(_fact(listing, "area", "district") and _fact(listing, "project", "building"))
    lines = [line for line in ((_location(listing) if has_location else ""), _building(listing), _value(listing)) if line]
    if not lines: return "" if allow_empty else "具体条件以房源资料和现场核对结果为准。"
    return "\n".join(lines[:max(0, max_points)])

__all__ = ["generate_talk"]
