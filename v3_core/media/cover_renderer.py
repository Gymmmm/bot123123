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
THUMBS_TOP = 978
THUMBS_HEIGHT = 278
BRAND_TOP = 1264
BRAND_HEIGHT = 86
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
    """Single locked Channel Cover visual: photo overlay + three shots + white brand strip."""
    layer = Image.new("RGBA", CANVAS, (0, 0, 0, 0))
    draw = ImageDraw.Draw(layer)
    # Keep core facts on the main-photo safe area; never insert a web-card band.
    shade_top = 650
    for y in range(shade_top, THUMBS_TOP):
        progress = (y - shade_top) / max(1, THUMBS_TOP - shade_top)
        alpha = int(35 + 170 * progress)
        draw.line([(0, y), (1080, y)], fill=(0, 0, 0, alpha))
    identity = "｜".join(part for part in (
        str(data.project or data.project_alias or data.area or "").strip(),
        str(data.layout or "").strip(),
    ) if part)
    if identity:
        draw.text((42, 748), identity, font=_font(38, bold=True), fill=(255, 255, 255), stroke_width=2, stroke_fill=(20, 20, 20))
    price = data.price_line()
    if price:
        draw.text((42, 805), price, font=_font(62, bold=True), fill=(255, 255, 255), stroke_width=2, stroke_fill=(20, 20, 20))

    # Locked white brand strip.
    draw.rectangle((0, BRAND_TOP, 1080, 1350), fill=(255, 255, 255, 255))
    draw.text((42, BRAND_TOP + 10), "侨联地产", font=_font(31, bold=True), fill=(30, 28, 24))
    draw.text((42, BRAND_TOP + 53), "OVERSEAS UNITED REAL ESTATE", font=_font(15), fill=(95, 91, 84))
    draw.line((390, BRAND_TOP + 47, 815, BRAND_TOP + 47), fill=(198, 154, 67), width=2)
    draw.text((860, BRAND_TOP + 28), "出租房源", font=_font(24, bold=True), fill=(45, 42, 36))
    canvas.paste(layer, (0, 0), layer)

