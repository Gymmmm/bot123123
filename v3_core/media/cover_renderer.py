"""DB-free Pillow renderer for Telegram property covers."""
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable
from PIL import Image, ImageDraw, ImageFont, ImageOps

CANVAS = (1080, 1350)
GUTTER = 8
INFO_TOP = 820
INFO_HEIGHT = 150
THUMBS_TOP = 970
THUMBS_HEIGHT = 286
BRAND_TOP = 1256
BRAND_HEIGHT = 94
FONT_CANDIDATES = (
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
        if negotiable:
            return raw
        numeric = raw.replace("$", "").replace(",", "").replace("/月", "").strip()
        try:
            value = float(numeric)
            rendered = f"{int(value):,}" if value.is_integer() else f"{value:,.2f}".rstrip("0").rstrip(".")
            price = f"${rendered}"
        except (TypeError, ValueError):
            price = raw if raw.startswith("$") else "$" + raw
        return price + "/月" if str(self.deal_type or "rent").lower() == "rent" and "/月" not in price else price

def _display_size(value: Any) -> str:
    text = str(value or "").strip().replace("平方米", "㎡").replace("平米", "㎡")
    if text and text.replace(".", "", 1).isdigit():
        text += "㎡"
    return text

def _font(size: int, *, bold: bool = False) -> ImageFont.ImageFont:
    candidates = list(FONT_CANDIDATES)
    if bold:
        candidates = [
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

def _paste_gallery(canvas: Image.Image, images: list[Image.Image]) -> None:
    canvas.paste(_fit(images[0], (1080, INFO_TOP)), (0, 0))
    secondary = images[1:4]
    if not secondary:
        return
    count = len(secondary)
    total_gutter = GUTTER * (count - 1)
    widths = [(1080 - total_gutter) // count] * count
    widths[-1] += 1080 - total_gutter - sum(widths)
    x = 0
    for image, width in zip(secondary, widths):
        canvas.paste(_fit(image, (width, THUMBS_HEIGHT)), (x, THUMBS_TOP))
        x += width + GUTTER

def _text(draw: ImageDraw.ImageDraw, xy: tuple[int, int], value: str, *, size: int, bold: bool = False) -> None:
    if value:
        draw.text(xy, value, font=_font(size, bold=bold), fill=(255, 255, 255), stroke_width=1, stroke_fill=(20, 20, 20))

def _overlay(canvas: Image.Image, data: CoverRenderData) -> None:
    """Formal channel cover: facts on hero, thumbnails unobstructed, white brand strip."""
    layer = Image.new("RGBA", CANVAS, (0, 0, 0, 0))
    draw = ImageDraw.Draw(layer)
    fade_top = 585
    for y in range(fade_top, INFO_TOP):
        progress = (y - fade_top) / max(1, INFO_TOP - fade_top)
        alpha = int(205 * (progress ** 1.35))
        draw.line([(0, y), (1080, y)], fill=(8, 8, 8, alpha), width=1)
    location = str(data.project or data.project_alias or data.area or "").strip()
    layout = str(data.layout or "").strip()
    identity = "｜".join(part for part in (location, layout) if part)
    if identity:
        draw.text((42, 682), identity, font=_font(38, bold=True), fill=(255, 255, 255))
    price = data.price_line()
    if price:
        draw.text((42, 742), price, font=_font(60, bold=True), fill=(246, 207, 113))
    draw.rectangle((0, INFO_TOP, 1080, THUMBS_TOP), fill=(244, 240, 233, 255))
    draw.rectangle((0, BRAND_TOP, 1080, 1350), fill=(255, 255, 255, 255))
    draw.text((42, BRAND_TOP + 12), "侨联地产", font=_font(31, bold=True), fill=(28, 26, 22))
    draw.text((210, BRAND_TOP + 22), "OVERSEAS UNITED REAL ESTATE", font=_font(16), fill=(105, 100, 92))
    draw.line([(575, BRAND_TOP + 45), (790, BRAND_TOP + 45)], fill=(198, 154, 67), width=2)
    draw.rounded_rectangle((835, BRAND_TOP + 15, 1040, BRAND_TOP + 75), radius=30, fill=(246, 232, 197))
    draw.text((876, BRAND_TOP + 27), "出租房源", font=_font(24, bold=True), fill=(45, 40, 32))
    canvas.paste(layer, (0, 0), layer)

def render_cover(*, style: str, source_image: str, output_path: str, data: CoverRenderData,
                 source_images: Iterable[str] | None = None) -> str:
    """Render a fixed 1080x1350 Telegram cover using readable source photos."""
    del style
    candidates = [source_image]
    candidates.extend(list(source_images or ()))
    images = _usable_images(candidates)
    if not images:
        raise ValueError("cover_no_usable_images")
    canvas = Image.new("RGB", CANVAS, (244, 240, 233))
    _paste_gallery(canvas, images)
    _overlay(canvas, data)
    output = Path(output_path).expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.suffix.lower() in {".jpg", ".jpeg"}:
        canvas.save(output, format="JPEG", quality=92, optimize=True)
    else:
        canvas.save(output, format="PNG", optimize=True)
    return str(output)

__all__ = ["CoverRenderData", "render_cover"]
