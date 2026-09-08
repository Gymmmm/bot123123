"""Pure public display formatting extracted from qiaolian_dual.utils_formatting.

Only the layout/floor behavior used by channel rendering lives here.  The old
module imported ``common.*`` for unrelated User Bot helpers; V3 deliberately
does not.
"""
from __future__ import annotations

import re


def display_layout(layout: object, property_type: object = "") -> str:
    raw = re.sub(r"\s+", "", str(layout or "").strip())
    if not raw:
        return ""
    kind = str(property_type or "").strip()
    is_commercial = any(
        word in kind for word in ("办公室", "办公楼", "写字楼", "商铺", "商业")
    )
    if not is_commercial:
        match = re.fullmatch(r"(\d+)房(\d+)办公(\d+)卫", raw)
        if match:
            rooms, studies, baths = match.groups()
            study_text = "书房" if studies == "1" else f"{studies}书房"
            return f"{rooms}房＋{study_text}｜{baths}卫"
        match = re.fullmatch(r"(\d+)房(\d+)厅(\d+)卫", raw)
        if match:
            rooms, halls, baths = match.groups()
            return f"{rooms}房{halls}厅｜{baths}卫"
    return raw.replace("|", "｜")


def display_floor(floor: object) -> str:
    raw = str(floor or "").strip()
    if not raw or raw in {"未知", "待确认", "暂无", "[暂无]", "-", "--"}:
        return ""
    compact = re.sub(r"\s+", "", raw)
    if re.fullmatch(r"\d+", compact):
        return f"{int(compact)}楼"
    if re.fullmatch(r"\d+楼", compact):
        return compact
    return raw.replace("|", "｜")


# Compatibility aliases while extracted channel_post is being rewired.
_display_layout = display_layout
_display_floor = display_floor


__all__ = [
    "display_layout",
    "display_floor",
    "_display_layout",
    "_display_floor",
]
