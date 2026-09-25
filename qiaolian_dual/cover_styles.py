"""侨联房源封面的唯一风格注册表。

正式图片封面四套风格，各有横版 / 竖版标准模板。
单视频封面另有横版 / 竖版专用模板。
旧名称只做兼容映射，不再形成新的渲染分支。

Canonical implementation lives in v3_core.media.cover_styles;
this module re-exports for legacy publication_package callers.
"""
from __future__ import annotations

from v3_core.media.cover_styles import (  # noqa: F401
    ACCEPTED_COVER_STYLE_KEYS,
    COVER_CANVAS,
    FINAL_COVER_STYLES,
    PORTRAIT_COVER_STYLES,
    STYLE_LABELS,
    VIDEO_COVER_STYLES,
    cover_canvas_size,
    cover_orientation,
    cover_style_family,
    cover_template_path,
    cover_viewport,
    is_portrait_cover_style,
    is_video_cover_style,
    landscape_style_for,
    normalize_cover_style,
    portrait_style_for,
    recommended_cover_style,
    resolve_cover_style_for_source,
    source_image_orientation,
)

__all__ = [
    "ACCEPTED_COVER_STYLE_KEYS",
    "COVER_CANVAS",
    "FINAL_COVER_STYLES",
    "PORTRAIT_COVER_STYLES",
    "STYLE_LABELS",
    "VIDEO_COVER_STYLES",
    "cover_canvas_size",
    "cover_orientation",
    "cover_style_family",
    "cover_template_path",
    "cover_viewport",
    "is_portrait_cover_style",
    "is_video_cover_style",
    "landscape_style_for",
    "normalize_cover_style",
    "portrait_style_for",
    "recommended_cover_style",
    "resolve_cover_style_for_source",
    "source_image_orientation",
]
