"""Offline, failure-isolated image renderer for canonical Qiaolian listings.

This module has no Telegram dependency. Source images are read-only and every
output is written below ``rendered/<property id>`` using atomic replacement.
"""
from __future__ import annotations

import os
import re
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

from PIL import Image, ImageDraw, ImageEnhance, ImageFont, ImageOps, UnidentifiedImageError

from qiaolian_dual.canonical_fact_projection import listing_projection
from qiaolian_dual.cover_templates import CANVAS_SIZE, COVER_TEXT_BOX, PHOTO_BOX, SAFE_INSET, WHITE_BORDER, get_template

MIN_SOURCE_SIZE = (320, 240)
_PLACEHOLDERS = {"", "待确认", "未知", "--", "暂无", "none", "null"}


@dataclass(frozen=True)
class RenderIssue:
    index: int
    source: str
    code: str
    message: str


@dataclass
class RenderResult:
    property_id: str
    output_dir: Path
    outputs: list[Path] = field(default_factory=list)
    issues: list[RenderIssue] = field(default_factory=list)
    logo_variants: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return bool(self.outputs) and not any(issue.index == 0 for issue in self.issues)


def renderer_listing_data(canonical_facts: dict[str, Any], property_id: str) -> dict[str, Any]:
    """Thin, one-way adapter from the existing canonical listing source."""
    projected = listing_projection(canonical_facts, listing_id=str(property_id))
    return {
        "property_id": str(property_id),
        "location": projected.get("project") or projected.get("area"),
        "layout": projected.get("layout"),
        "price": projected.get("price"),
        "currency": projected.get("currency") or "USD",
        "size": projected.get("size_sqm"),
        "floor": projected.get("floor"),
        "property_type": projected.get("property_type_display") or projected.get("property_type"),
    }


def _clean(value: Any) -> str:
    text = str(value if value is not None else "").strip()
    return "" if text.casefold() in _PLACEHOLDERS else text


def _safe_property_id(value: str) -> str:
    raw = str(value or "").strip()
    if not raw or raw in {".", ".."} or not re.fullmatch(r"[A-Za-z0-9_-]{1,80}", raw):
        raise ValueError("unsafe_property_id")
    return raw


def _font(size: int, bold: bool = False) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    candidates = [
        "/System/Library/Fonts/PingFang.ttc",
        "/System/Library/Fonts/STHeiti Medium.ttc" if bold else "/System/Library/Fonts/STHeiti Light.ttc",
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc" if bold else "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    ]
    for candidate in candidates:
        if Path(candidate).is_file():
            try:
                return ImageFont.truetype(candidate, size=size, index=0)
            except OSError:
                continue
    return ImageFont.load_default()


def _open_source(path: Path) -> Image.Image:
    try:
        with Image.open(path) as opened:
            opened.verify()
        with Image.open(path) as opened:
            if opened.mode in {"RGBA", "LA", "P"} and ("transparency" in opened.info or opened.mode in {"RGBA", "LA"}):
                raise ValueError("transparent_image")
            image = ImageOps.exif_transpose(opened).convert("RGB")
    except UnidentifiedImageError as exc:
        raise ValueError("unrecognized_image") from exc
    if image.width < MIN_SOURCE_SIZE[0] or image.height < MIN_SOURCE_SIZE[1]:
        raise ValueError("image_too_small")
    return image


def _contain(image: Image.Image) -> Image.Image:
    box_w, box_h = PHOTO_BOX[2] - PHOTO_BOX[0], PHOTO_BOX[3] - PHOTO_BOX[1]
    fitted = ImageOps.contain(image, (box_w, box_h), Image.Resampling.LANCZOS)
    canvas = Image.new("RGB", CANVAS_SIZE, "white")
    x = PHOTO_BOX[0] + (box_w - fitted.width) // 2
    y = PHOTO_BOX[1] + (box_h - fitted.height) // 2
    canvas.paste(fitted, (x, y))
    return canvas


def _logo_variant(image: Image.Image) -> str:
    sample = image.crop((WHITE_BORDER, WHITE_BORDER, min(400, image.width), min(170, image.height))).convert("L")
    return "dark" if int(sample.resize((1, 1)).getpixel((0, 0))) > 145 else "light"


def _draw_logo(canvas: Image.Image, variant: str) -> None:
    draw = ImageDraw.Draw(canvas)
    color = (31, 37, 41) if variant == "dark" else (255, 255, 255)
    shadow = (255, 255, 255) if variant == "dark" else (20, 20, 20)
    xy = (SAFE_INSET, SAFE_INSET)
    draw.rounded_rectangle((xy[0] - 10, xy[1] - 7, xy[0] + 142, xy[1] + 34), radius=8, fill=(*shadow, 150))
    draw.text(xy, "侨联地产", font=_font(22, True), fill=color)


def _price(data: dict[str, Any]) -> str:
    raw = data.get("price")
    if raw in (None, "", 0, "0"):
        return ""
    try:
        number = f"{int(float(raw)):,}"
    except (TypeError, ValueError):
        number = _clean(raw)
    return f"${number} / 月" if number else ""


def _draw_cover(canvas: Image.Image, data: dict[str, Any], template_key: str) -> None:
    theme = get_template(template_key)
    overlay = Image.new("RGBA", CANVAS_SIZE, (0, 0, 0, 0))
    draw = ImageDraw.Draw(overlay)
    x1, y1, x2, y2 = COVER_TEXT_BOX
    panel = (*theme.panel[:3], theme.opacity)
    draw.rounded_rectangle((x1, y1, x2, y2), radius=18, fill=panel)
    draw.rectangle((x1, y1, x1 + 7, y2), fill=(*theme.accent, 255))
    location = _clean(data.get("location"))
    layout = _clean(data.get("layout"))
    price = _price(data)
    title = " · ".join(value for value in (location, layout) if value)
    if title:
        draw.text((x1 + 28, y1 + 24), title, font=_font(31, True), fill=theme.primary)
    details = []
    for value, suffix in ((data.get("size"), "㎡"), (data.get("floor"), "层"), (data.get("property_type"), "")):
        cleaned = _clean(value)
        if cleaned:
            details.append(cleaned if suffix and cleaned.endswith(suffix) else f"{cleaned}{suffix}")
    if details:
        draw.text((x1 + 28, y1 + 78), "  ·  ".join(details[:3]), font=_font(21), fill=theme.secondary)
    if price:
        bbox = draw.textbbox((0, 0), price, font=_font(35, True))
        draw.text((x2 - (bbox[2] - bbox[0]) - 28, y2 - 55), price, font=_font(35, True), fill=theme.accent)
    canvas.paste(overlay, (0, 0), overlay)


def _atomic_jpeg(image: Image.Image, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{target.stem}-", suffix=".jpg", dir=target.parent)
    os.close(fd)
    try:
        image.save(temporary, "JPEG", quality=92, optimize=True, progressive=True)
        os.replace(temporary, target)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def render_listing_images(*, property_id: str, canonical_facts: dict[str, Any], source_images: Iterable[str | Path], output_root: str | Path = "rendered", template: str = "A") -> RenderResult:
    """Render cover and detail photos while isolating per-image failures."""
    safe_id = _safe_property_id(property_id)
    theme = get_template(template)  # validate before creating output
    del theme
    root = Path(output_root).expanduser().resolve()
    output_dir = (root / safe_id).resolve()
    if output_dir.parent != root:
        raise ValueError("unsafe_output_path")
    sources = [Path(item).expanduser().resolve() for item in source_images]
    if not sources:
        raise ValueError("no_source_images")
    for source in sources:
        if source == output_dir or output_dir in source.parents or source == root:
            raise ValueError("output_overlaps_input")
    data = renderer_listing_data(canonical_facts, safe_id)
    result = RenderResult(safe_id, output_dir)
    successful = 0
    for index, source in enumerate(sources):
        try:
            if not source.is_file():
                raise ValueError("source_not_found")
            image = _open_source(source)
            image = ImageEnhance.Contrast(ImageEnhance.Brightness(image).enhance(1.025)).enhance(1.035)
            canvas = _contain(image)
            variant = _logo_variant(canvas)
            _draw_logo(canvas, variant)
            target = output_dir / ("cover.jpg" if successful == 0 else f"photo_{successful:02d}.jpg")
            if successful == 0:
                _draw_cover(canvas, data, template)
            _atomic_jpeg(canvas, target)
            result.outputs.append(target)
            result.logo_variants.append(variant)
            successful += 1
        except (OSError, ValueError) as exc:
            code = str(exc) if isinstance(exc, ValueError) else "image_io_error"
            result.issues.append(RenderIssue(index=index, source=str(source), code=code, message=f"图片 {index + 1} 渲染失败：{code}"))
    return result


__all__ = ["MIN_SOURCE_SIZE", "RenderIssue", "RenderResult", "renderer_listing_data", "render_listing_images"]
