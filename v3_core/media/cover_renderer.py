"""DB-free Pillow renderer for Telegram property covers."""
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable
from PIL import Image, ImageDraw, ImageFont, ImageOps

CANVAS = (1080, 1360)
GUTTER = 8
HERO_HEIGHT = 850
GALLERY_TOP = HERO_HEIGHT + GUTTER
FOOTER_HEIGHT = 72
FOOTER_TOP = CANVAS[1] - FOOTER_HEIGHT
GALLERY_HEIGHT = FOOTER_TOP - GALLERY_TOP - GUTTER
GOLD = (226, 190, 105)
GOLD_LIGHT = (249, 226, 164)
INK = (33, 28, 22)
FONT_CANDIDATES = (
    str(Path(__file__).resolve().parents[2] / "assets" / "fonts" / "NotoSansSC-Regular.otf"),
    str(Path(__file__).resolve().parents[2] / "assets" / "fonts" / "NotoSansSC-Medium.otf"),
    "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
    "/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc",
    "/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc",
    "/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
)

@dataclass(frozen=True)
class CoverRenderData:
    public_listing_id: str
    project: str = ""
    project_alias: str = ""
    property_type: str = ""
    deal_type: str = "rent"
    layout: str = ""
    area: str = ""
    size: str = ""
    floor: str = ""
    price: str = ""
    highlight_1: str = ""
    highlight_2: str = ""
    highlight_3: str = ""

    def price_line(self) -> str:
        raw = str(self.price or "").strip()
        if not raw:
            return ""
        negotiable = raw in {"售价面议", "租金面议", "价格面议", "面议"}
        price = raw if raw.startswith("$") or negotiable else "$" + raw
        return price + "/月" if str(self.deal_type or "rent").lower() == "rent" and not negotiable else price

def _display_size(value: Any) -> str:
    text = str(value or "").strip().replace("平方米", "㎡").replace("平米", "㎡")
    if text and text.replace(".", "", 1).isdigit():
        text += "㎡"
    return text

def _font(size: int, *, bold: bool = False) -> ImageFont.ImageFont:
    candidates = list(FONT_CANDIDATES)
    if bold:
        candidates = [
            str(Path(__file__).resolve().parents[2] / "assets" / "fonts" / "NotoSansSC-Bold.otf"),
            "/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc",
            "/usr/share/fonts/truetype/noto/NotoSansCJK-Bold.ttc",
        ] + candidates
    for path in candidates:
        try:
            if Path(path).is_file():
                return ImageFont.truetype(path, size=size)
        except OSError:
            continue
    try:
        return ImageFont.truetype("DejaVuSans.ttf", size=size)
    except OSError:
        return ImageFont.load_default()

def _open_image(path: str | Path) -> Image.Image | None:
    try:
        with Image.open(path) as raw:
            return ImageOps.exif_transpose(raw).convert("RGB")
    except (OSError, ValueError, SyntaxError):
        return None

def _usable_images(paths: Iterable[str | Path]) -> list[Image.Image]:
    result: list[Image.Image] = []
    seen: set[str] = set()
    for value in paths:
        key = str(Path(value).expanduser())
        if not key or key in seen:
            continue
        seen.add(key)
        image = _open_image(key)
        if image is not None:
            result.append(image)
    return result

def _fit(image: Image.Image, size: tuple[int, int]) -> Image.Image:
    return ImageOps.fit(image, size, method=Image.Resampling.LANCZOS, centering=(0.5, 0.5))

def _paste_gallery(canvas: Image.Image, images: list[Image.Image], *, layout: str = "hero_three") -> None:
    # All style aliases intentionally converge on the same channel-first layout.
    # This keeps apartment/townhouse/villa posts recognisably in one system.
    del layout
    canvas.paste(_fit(images[0], (1080, HERO_HEIGHT)), (0, 0))
    secondary = images[1:4]
    if not secondary:
        return
    count = len(secondary)
    total_gutter = GUTTER * (count - 1)
    widths = [(1080 - total_gutter) // count] * count
    widths[-1] += 1080 - total_gutter - sum(widths)
    x = 0
    for image, width in zip(secondary, widths):
        canvas.paste(_fit(image, (width, GALLERY_HEIGHT)), (x, GALLERY_TOP))
        x += width + GUTTER

def _text(draw: ImageDraw.ImageDraw, xy: tuple[int, int], value: str, *, size: int, bold: bool = False) -> None:
    if value:
        draw.text(xy, value, font=_font(size, bold=bold), fill=(255, 255, 255), stroke_width=1, stroke_fill=(20, 20, 20))

def _house_mark(draw: ImageDraw.ImageDraw, x: int, y: int) -> None:
    """Small original house outline; avoids depending on external logo files."""
    draw.line(((x, y + 28), (x + 30, y), (x + 60, y + 28)), fill=GOLD_LIGHT, width=7, joint="curve")
    draw.line(((x + 9, y + 23), (x + 9, y + 57), (x + 51, y + 57), (x + 51, y + 23)), fill=GOLD_LIGHT, width=6, joint="curve")
    draw.line(((x + 25, y + 57), (x + 25, y + 36), (x + 42, y + 36), (x + 42, y + 57)), fill=GOLD_LIGHT, width=5)


def _brand_lockup() -> Image.Image | None:
    path = Path(__file__).resolve().parents[2] / "media_pipeline" / "assets" / "qiaolian_logo_black_gold_lockup.png"
    try:
        with Image.open(path) as raw:
            image = raw.convert("RGBA")
        bbox = image.getchannel("A").getbbox()
        return image.crop(bbox) if bbox else None
    except (OSError, ValueError):
        return None


def _place_brand(layer: Image.Image, *, x: int, y: int, width: int) -> None:
    lockup = _brand_lockup()
    if lockup is None:
        draw = ImageDraw.Draw(layer)
        _house_mark(draw, x, y)
        draw.text((x + 80, y - 7), "侨联地产", font=_font(47, bold=True), fill=GOLD_LIGHT)
        return
    height = max(1, round(lockup.height * width / lockup.width))
    lockup = lockup.resize((width, height), Image.Resampling.LANCZOS)
    shadow = Image.new("RGBA", layer.size, (0, 0, 0, 0))
    shadow.alpha_composite(lockup, (x + 2, y + 4))
    shadow.putalpha(shadow.getchannel("A").point(lambda value: int(value * 0.48)))
    layer.alpha_composite(shadow, (0, 0))
    layer.alpha_composite(lockup, (x, y))


def _fit_text(draw: ImageDraw.ImageDraw, value: str, *, max_width: int, start_size: int, min_size: int = 28) -> ImageFont.ImageFont:
    size = start_size
    while size > min_size:
        font = _font(size, bold=True)
        if draw.textbbox((0, 0), value, font=font)[2] <= max_width:
            return font
        size -= 2
    return _font(min_size, bold=True)


def _gallery_labels(canvas: Image.Image, labels: list[str], count: int) -> None:
    if count <= 0:
        return
    layer = Image.new("RGBA", CANVAS, (0, 0, 0, 0))
    draw = ImageDraw.Draw(layer)
    total_gutter = GUTTER * (count - 1)
    widths = [(1080 - total_gutter) // count] * count
    widths[-1] += 1080 - total_gutter - sum(widths)
    x = 0
    for index, width in enumerate(widths):
        label = labels[index] if index < len(labels) and labels[index] else "实拍"
        font = _font(23, bold=True)
        text_w = draw.textbbox((0, 0), label, font=font)[2]
        draw.rounded_rectangle(
            (x + 16, GALLERY_TOP + 15, x + 40 + text_w, GALLERY_TOP + 57),
            radius=18,
            fill=(28, 24, 20, 165),
            outline=(255, 255, 255, 190),
            width=2,
        )
        draw.text((x + 28, GALLERY_TOP + 22), label, font=font, fill=(255, 255, 255, 245))
        x += width + GUTTER
    canvas.paste(layer, (0, 0), layer)


def _overlay(canvas: Image.Image, data: CoverRenderData) -> None:
    layer = Image.new("RGBA", CANVAS, (0, 0, 0, 0))
    draw = ImageDraw.Draw(layer)
    # Gentle top shade for the gold brand and a stronger bottom fade for facts.
    for y in range(0, 210):
        alpha = int(92 * (1 - y / 210))
        draw.rectangle((0, y, 1080, y + 1), fill=(20, 17, 13, alpha))
    fade_top = 500
    for y in range(fade_top, HERO_HEIGHT):
        progress = (y - fade_top) / max(1, HERO_HEIGHT - fade_top)
        alpha = int(200 * progress)
        draw.rectangle((0, y, 1080, y + 1), fill=(12, 10, 8, alpha))

    _place_brand(layer, x=42, y=38, width=360)

    title = str(data.project or data.project_alias or data.area or data.property_type or "精选房源").strip()
    title_font = _fit_text(draw, title, max_width=650, start_size=56, min_size=34)
    draw.text((42, 652), title, font=title_font, fill=(255, 255, 255), stroke_width=2, stroke_fill=(20, 18, 16))
    subtitle = " · ".join(part for part in (str(data.layout or "").strip(), str(data.property_type or "").strip()) if part)
    if subtitle:
        draw.text((44, 720), subtitle, font=_font(31, bold=True), fill=(255, 255, 255), stroke_width=1, stroke_fill=(20, 18, 16))

    _draw_dark_price_pill(draw, data, right=1038, bottom=790, max_width=320)

    # White footer matching the reference's clean real-estate card finish.
    draw.rectangle((0, FOOTER_TOP, 1080, CANVAS[1]), fill=(255, 255, 255, 255))
    draw.text((38, FOOTER_TOP + 14), "侨联地产", font=_font(28, bold=True), fill=INK)
    draw.text((181, FOOTER_TOP + 31), "QIAO LIAN PROPERTY", font=_font(12), fill=(155, 126, 72))
    footer_label = "出租房源" if str(data.deal_type or "rent").lower() == "rent" else "出售房源"
    footer_font = _font(25, bold=True)
    footer_w = draw.textbbox((0, 0), footer_label, font=footer_font)[2]
    draw.text((1040 - footer_w, FOOTER_TOP + 19), footer_label, font=footer_font, fill=(112, 105, 94))
    canvas.paste(layer, (0, 0), layer)


def _is_apartment(data: CoverRenderData) -> bool:
    value = str(data.property_type or "").strip().lower()
    return any(token in value for token in ("公寓", "apartment", "condo", "studio"))


def _draw_dark_price_pill(
    draw: ImageDraw.ImageDraw,
    data: CoverRenderData,
    *,
    right: int,
    bottom: int,
    max_width: int = 320,
) -> None:
    price = data.price_line()
    if not price:
        return
    suffix = "/月" if price.endswith("/月") else ""
    amount = price[:-2] if suffix else price
    amount_font = _fit_text(draw, amount, max_width=max_width - 100, start_size=50, min_size=34)
    suffix_font = _font(24, bold=True)
    amount_w = draw.textbbox((0, 0), amount, font=amount_font)[2]
    suffix_w = draw.textbbox((0, 0), suffix, font=suffix_font)[2] if suffix else 0
    width = min(max_width, amount_w + suffix_w + 65)
    height = 78
    left, top = right - width, bottom - height
    draw.rounded_rectangle(
        (left, top, right, bottom),
        radius=16,
        fill=(17, 15, 13, 232),
        outline=GOLD,
        width=3,
    )
    x = left + 26
    draw.text((x, top + 7), amount, font=amount_font, fill=GOLD_LIGHT)
    if suffix:
        draw.text((x + amount_w + 7, top + 31), suffix, font=suffix_font, fill=(255, 255, 255))


def render_cover(*, style: str, source_image: str, output_path: str, data: CoverRenderData,
                 source_images: Iterable[str] | None = None, source_labels: Iterable[str] | None = None,
                 layout: str = "hero_three") -> str:
    """Render every property type with one hero image above three detail images."""
    del style
    candidates = [source_image]
    candidates.extend(list(source_images or ()))
    images = _usable_images(candidates)
    if not images:
        raise ValueError("cover_no_usable_images")
    canvas = Image.new("RGB", CANVAS, (244, 240, 233))
    _paste_gallery(canvas, images, layout=layout)
    _overlay(canvas, data)
    _gallery_labels(canvas, list(source_labels or ()), min(3, max(0, len(images) - 1)))
    output = Path(output_path).expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.suffix.lower() in {".jpg", ".jpeg"}:
        canvas.save(output, format="JPEG", quality=92, optimize=True)
    else:
        canvas.save(output, format="PNG", optimize=True)
    return str(output)

__all__ = ["CoverRenderData", "render_cover"]
