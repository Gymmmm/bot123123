"""DB-free Pillow renderer for Telegram property covers."""
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable
from PIL import Image, ImageDraw, ImageFont, ImageOps

CANVAS = (1080, 1350)
GUTTER = 8
BOTTOM_TOP = 858
BOTTOM_HEIGHT = 492
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
    layer = Image.new("RGBA", CANVAS, (0, 0, 0, 0))
    draw = ImageDraw.Draw(layer)
    draw.rectangle((0, INFO_TOP, 1080, INFO_TOP + INFO_HEIGHT), fill=(248, 246, 241, 255))
    identity = " · ".join(part for part in (
        str(data.project or data.project_alias or data.area or "").strip(),
        str(data.layout or "").strip(),
    ) if part)
    if identity:
        draw.text((42, INFO_TOP + 25), identity, font=_font(32, bold=True), fill=(35, 40, 45))
    price = data.price_line()
    if price:
        draw.text((42, INFO_TOP + 76), price, font=_font(54, bold=True), fill=(20, 65, 94))
    draw.rectangle((0, BRAND_TOP, 1080, 1350), fill=(20, 65, 94, 255))
    draw.text((42, BRAND_TOP + 13), "侨联地产", font=_font(30, bold=True), fill=(255, 255, 255))
    draw.text((210, BRAND_TOP + 20), "OVERSEAS UNITED REAL ESTATE", font=_font(17), fill=(224, 234, 239))
    draw.text((890, BRAND_TOP + 20), "出租房源", font=_font(24, bold=True), fill=(255, 255, 255))
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
