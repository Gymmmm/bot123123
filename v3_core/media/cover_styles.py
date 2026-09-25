"""侨联房源封面的唯一风格注册表。

正式图片封面四套风格；画幅由选中主图自动决定：
  横图 → 1080×864；竖图 → 1200×1500（共用竖版标准模板按风格换配色）。
单视频封面另有横版 / 竖版专用模板，同样跟主图方向走。
旧名称只做兼容映射，不再形成新的渲染分支。
"""
from __future__ import annotations

from pathlib import Path


# This file moved two package levels deeper than the production source.
REPO_ROOT = Path(__file__).resolve().parents[2]

# Four visual families (image covers).
FINAL_COVER_STYLES = ("classic_blue", "right_price", "black_gold", "premium_photo")

# Portrait image variants — same four families, 竖版标准画布.
PORTRAIT_COVER_STYLES = tuple(f"{style}_portrait" for style in FINAL_COVER_STYLES)

# Dedicated single-video cover templates.
VIDEO_COVER_STYLES = ("video_landscape", "video_portrait")

STYLE_LABELS = {
    "classic_blue": "经典蓝卡",
    "right_price": "右侧价格牌",
    "black_gold": "黑金高级感",
    "premium_photo": "极简实拍渐变",
    "classic_blue_portrait": "经典蓝卡",
    "right_price_portrait": "右侧价格牌",
    "black_gold_portrait": "黑金高级感",
    "premium_photo_portrait": "极简实拍渐变",
    "video_landscape": "视频封面 · 横版",
    "video_portrait": "视频封面 · 竖版",
}

# Standard canvas sizes used by HTML posters (Playwright screenshots .poster).
# Image landscape 1080×864 (5:4); image portrait 1200×1500 (4:5 / 用户称 5:4 竖版).
COVER_CANVAS = {
    "landscape": (1080, 864),
    "portrait": (1200, 1500),
    "video_landscape": (1600, 900),
    "video_portrait": (1080, 1920),
}

_LANDSCAPE_TEMPLATE = {
    "classic_blue": REPO_ROOT / "templates" / "property" / "01_经典蓝卡模板.html",
    "right_price": REPO_ROOT / "templates" / "property" / "03_右侧价格牌模板.html",
    "black_gold": REPO_ROOT / "templates" / "property" / "04_黑金高级感_右侧价格牌模板.html",
    "premium_photo": REPO_ROOT / "templates" / "property" / "13_侨联极简实拍渐变版.html",
}
_PORTRAIT_IMAGE_TEMPLATE = REPO_ROOT / "templates" / "property" / "14_图片竖版标准封面.html"
_VIDEO_LANDSCAPE_TEMPLATE = REPO_ROOT / "templates" / "property" / "15_视频横版封面.html"
_VIDEO_PORTRAIT_TEMPLATE = REPO_ROOT / "templates" / "property" / "16_视频竖版封面.html"

_TEMPLATE_PATHS = {
    **_LANDSCAPE_TEMPLATE,
    **{f"{style}_portrait": _PORTRAIT_IMAGE_TEMPLATE for style in FINAL_COVER_STYLES},
    "video_landscape": _VIDEO_LANDSCAPE_TEMPLATE,
    "video_portrait": _VIDEO_PORTRAIT_TEMPLATE,
    # Legacy key kept for old packages / readiness; points at production portrait video.
    "video_vertical": _VIDEO_PORTRAIT_TEMPLATE,
}

_ALIASES = {
    "classic": "classic_blue",
    "left_info": "classic_blue",
    "minimal": "classic_blue",
    "minimal_white": "classic_blue",
    "blue_banner": "classic_blue",
    "premium_4image": "classic_blue",
    "price_tag": "right_price",
    "right_price_fixed": "right_price",
    "villa_premium": "black_gold",
    "dark_glass": "black_gold",
    "vertical": "video_portrait",
    "video_vertical": "video_portrait",
    "video_horizontal": "video_landscape",
    "portrait": "premium_photo_portrait",
    "image_portrait": "premium_photo_portrait",
    "image_landscape": "premium_photo",
}

_ALL_PRODUCTION_STYLES = frozenset(
    {*FINAL_COVER_STYLES, *PORTRAIT_COVER_STYLES, *VIDEO_COVER_STYLES, "video_vertical"}
)

ACCEPTED_COVER_STYLE_KEYS = frozenset({*_ALL_PRODUCTION_STYLES, *_ALIASES.keys()})


def _listing_identity(*values: object) -> str:
    return " ".join(str(value or "").strip().lower() for value in values)


def is_villa_listing(*values: object) -> bool:
    """True for villa / townhouse style homes (not ordinary apartments)."""
    identity = _listing_identity(*values)
    return any(
        token in identity
        for token in ("别墅", "villa", "排屋", "townhouse", "town house", "独栋", "双拼", "联排")
    )


def recommended_cover_style(*values: object) -> str:
    """Return the production default: premium photo for ordinary, black gold for villas."""
    return "black_gold" if is_villa_listing(*values) else "premium_photo"


def resolve_cover_room_preference(*values: object) -> str:
    """Channel cover room priority: villa → exterior; apartment → living."""
    return "exterior" if is_villa_listing(*values) else "living"


def is_video_cover_style(style: str | None) -> bool:
    value = str(style or "").strip().lower()
    value = _ALIASES.get(value, value)
    return value in VIDEO_COVER_STYLES or value == "video_vertical"


def is_portrait_cover_style(style: str | None) -> bool:
    value = str(style or "").strip().lower()
    value = _ALIASES.get(value, value)
    if value in PORTRAIT_COVER_STYLES or value in {"video_portrait", "video_vertical"}:
        return True
    return False


def cover_style_family(style: str | None) -> str:
    """Map any style key back to its visual family (for logo / gallery mapping)."""
    value = normalize_cover_style(style, allow_video=True)
    if value in VIDEO_COVER_STYLES or value == "video_vertical":
        return "classic_blue"
    if value.endswith("_portrait"):
        base = value[: -len("_portrait")]
        return base if base in FINAL_COVER_STYLES else "premium_photo"
    return value if value in FINAL_COVER_STYLES else "classic_blue"


def cover_orientation(style: str | None) -> str:
    """Return canvas orientation bucket for viewport / sizing."""
    value = normalize_cover_style(style, allow_video=True)
    if value == "video_landscape":
        return "video_landscape"
    if value in {"video_portrait", "video_vertical"}:
        return "video_portrait"
    if value in PORTRAIT_COVER_STYLES:
        return "portrait"
    return "landscape"


def cover_canvas_size(style: str | None) -> tuple[int, int]:
    return COVER_CANVAS[cover_orientation(style)]


def cover_viewport(style: str | None) -> dict[str, int]:
    """Playwright viewport must fully contain the poster."""
    width, height = cover_canvas_size(style)
    return {"width": width + 80, "height": height + 80}


def source_image_orientation(source_image: str | Path) -> str:
    """Detect landscape / portrait / square from the selected cover source."""
    from PIL import Image

    from .photo_formatter import detect_orientation

    path = Path(source_image).expanduser().resolve()
    with Image.open(path) as img:
        return detect_orientation(img.width, img.height)


def resolve_cover_style_for_source(
    style: str | None,
    source_image: str | Path | None = None,
    *,
    allow_video: bool = True,
) -> str:
    """Normalize style, then lock image covers to the main-photo orientation.

    Admin/autopilot only choose the four visual families (or a video template).
    Horizontal main photo → landscape canvas; vertical → portrait canvas.
    Square sources stay on the landscape family.
    """
    normalized = normalize_cover_style(style, allow_video=allow_video)
    if is_video_cover_style(normalized):
        if source_image is None:
            return normalized
        try:
            orientation = source_image_orientation(source_image)
        except Exception:
            return normalized
        if orientation == "portrait":
            return "video_portrait"
        return "video_landscape"
    family = cover_style_family(normalized)
    if source_image is None:
        return family
    try:
        orientation = source_image_orientation(source_image)
    except Exception:
        return family
    if orientation == "portrait":
        return portrait_style_for(family)
    return family


def normalize_cover_style(style: str | None, *, allow_video: bool = True) -> str:
    """把旧名称收敛到正式封面套装；空值固定使用经典蓝卡。"""
    value = str(style or "classic_blue").strip().lower()
    value = _ALIASES.get(value, value)
    if value == "video_vertical":
        value = "video_portrait"
    if value in VIDEO_COVER_STYLES:
        return value if allow_video else "classic_blue"
    if value in FINAL_COVER_STYLES or value in PORTRAIT_COVER_STYLES:
        return value
    return "classic_blue"


def cover_template_path(style: str | None, *, allow_video: bool = True) -> Path:
    normalized = normalize_cover_style(style, allow_video=allow_video)
    return _TEMPLATE_PATHS[normalized]


def portrait_style_for(style: str | None) -> str:
    """Convert a landscape family key to its portrait image variant."""
    family = cover_style_family(style)
    if family not in FINAL_COVER_STYLES:
        family = "premium_photo"
    return f"{family}_portrait"


def landscape_style_for(style: str | None) -> str:
    """Convert any image style key to its landscape family key."""
    return cover_style_family(style)


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
    "is_villa_listing",
    "landscape_style_for",
    "normalize_cover_style",
    "portrait_style_for",
    "recommended_cover_style",
    "resolve_cover_room_preference",
    "resolve_cover_style_for_source",
    "source_image_orientation",
]
