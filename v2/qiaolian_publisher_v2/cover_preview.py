"""Thin admin-preview adapter for the single production cover registry."""
from __future__ import annotations

from typing import Iterable

from html_cover_renderer import render_html_cover
from qiaolian_dual.cover_styles import (
    STYLE_LABELS,
    cover_template_path,
    normalize_cover_style,
)


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
    style: str = "classic_blue",
    **_: object,
) -> str:
    """Render a private preview with the same HTML templates used by packages."""
    normalized_style = normalize_cover_style(style)
    values = [str(value).strip() for value in (highlights or []) if str(value).strip()]
    render_html_cover(
        template_path=str(cover_template_path(normalized_style, allow_video=False)),
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
