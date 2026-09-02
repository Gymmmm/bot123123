"""Tracked admin-only previews for the three production cover styles."""
from __future__ import annotations

from pathlib import Path
from typing import Iterable

from html_cover_renderer import render_html_cover

_REPO_ROOT = Path(__file__).resolve().parents[2]

STYLE_LABELS = {
    "blue_banner": "蓝色横幅",
    "left_info": "左下信息卡",
    "black_gold": "黑金价格牌",
}

_STYLE_TEMPLATE_PATHS = {
    "blue_banner": _REPO_ROOT / "templates" / "property" / "02_蓝色横幅模板.html",
    "left_info": _REPO_ROOT / "templates" / "property" / "01_经典蓝卡模板.html",
    "black_gold": _REPO_ROOT / "templates" / "property" / "04_黑金高级感_右侧价格牌模板.html",
}

_STYLE_ALIASES = {
    "classic": "left_info",
    "classic_blue": "left_info",
    "minimal": "blue_banner",
    "minimal_white": "blue_banner",
    "price_tag": "black_gold",
    "right_price": "black_gold",
}


def normalize_cover_style(style: str | None) -> str:
    value = str(style or "blue_banner").strip().lower()
    value = _STYLE_ALIASES.get(value, value)
    return value if value in STYLE_LABELS else "blue_banner"


def generate_house_cover(
    bg_image_path: str,
    output_path: str,
    *,
    project: str,
    property_type: str = "",
    area: str = "",
    size: str = "",
    floor: str = "",
    price: str = "",
    highlights: Iterable[str] | None = None,
    style: str = "blue_banner",
    **_: object,
) -> str:
    """Render a private preview with the same HTML templates used by packages."""
    normalized_style = normalize_cover_style(style)
    values = [str(value).strip() for value in (highlights or []) if str(value).strip()]
    render_html_cover(
        template_path=str(_STYLE_TEMPLATE_PATHS[normalized_style]),
        source_image=str(bg_image_path),
        output_path=str(output_path),
        data={
            "project": str(project or "").strip(),
            "layout": str(property_type or "").strip(),
            "area": str(area or "").strip(),
            "size": str(size or "").strip(),
            "floor": str(floor or "").strip(),
            "price": str(price or "").strip(),
            "highlights": values,
            "h1": values[0] if len(values) > 0 else "",
            "h2": values[1] if len(values) > 1 else "",
            "h3": values[2] if len(values) > 2 else "",
            "deal_type": "rent",
            "is_real_photo": True,
        },
    )
    return str(output_path)


__all__ = ["STYLE_LABELS", "generate_house_cover", "normalize_cover_style"]
