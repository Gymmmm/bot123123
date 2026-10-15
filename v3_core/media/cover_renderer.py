"""DB-free Pillow renderer for Telegram property covers (Gym 2026-10-11).

Templates (chosen upstream by ``cover_plan``; ``auto`` follows the property type):

* ``apartment_3x2``: 1200x800 landscape. Hero on the left (798x738) and a
  right column of three stacked thumbnails (396 wide), 6px white separators,
  62px dark brand footer. Same proportions as the approved 1080x720 sample.
* ``villa_black_gold`` (default villa template, ``V3_VILLA_COVER_TEMPLATE``): Gym's
  1080x1388 reference — hero 700, black-gold info bar 700–868 (📍 title, gold rule,
  类型 | N房 | 押付·租期, gold price card), three rounded 346x384 thumbs, white footer
  with the 侨联地产 lockup and 出租房源 pill. See ``villa_card``.
* ``villa_4x5``: 1080x1350 portrait. Hero 1080x878 (good exterior,
  straightened upstream), three 356x402 thumbnails below, 64px footer.
* ``landscape_3x2``: villa without a good exterior — apartment geometry.
* ``grid_2x2``: 1200x800, four equal photos (no standout hero).
* ``single``: one photo fills the photo area (no padding with repeats).

Shared treatment: 📍 title (小区/区域) + gold 户型 · 类型 pill top-left, price
card bottom-right of the hero as the biggest text, thin top/bottom shades
only, thumbnails labelled by room, footer = house icon + 侨联地产. No internal
listing id, no 出租房源 pill, no white frame around the image.
"""
from __future__ import annotations
from dataclasses import dataclass, replace
import os
from pathlib import Path
from typing import Any, Iterable
from PIL import Image, ImageDraw, ImageFilter, ImageFont, ImageOps

from .hero_layout import frame_offset, text_slot


@dataclass(frozen=True)
class CoverLayout:
    kind: str                 # "apartment" (side column) or "tall" (row below)
    canvas: tuple[int, int]
    hero: tuple[int, int]     # hero width, height
    thumb_extent: int         # apartment: column width; tall: row height
    separator: int
    footer_height: int
    margin: int
    title_size: int
    price_size: int

    @property
    def footer_top(self) -> int:
        return self.canvas[1] - self.footer_height


SEPARATOR = 6
APARTMENT_LAYOUT = CoverLayout(
    kind="apartment", canvas=(1200, 800), hero=(798, 738), thumb_extent=396,
    separator=SEPARATOR, footer_height=62, margin=36, title_size=49, price_size=71,
)
GRID_LAYOUT = CoverLayout(
    kind="grid", canvas=(1200, 800), hero=(597, 366), thumb_extent=366,
    separator=SEPARATOR, footer_height=62, margin=30, title_size=42, price_size=60,
)
TALL_LAYOUT = CoverLayout(
    kind="tall", canvas=(1080, 1350), hero=(1080, 878), thumb_extent=402,
    separator=SEPARATOR, footer_height=64, margin=32, title_size=52, price_size=80,
)
# V-B (Gym 2026-10-11 villa feedback): shorter hero, taller detail row.
TALL_COMPACT_LAYOUT = CoverLayout(
    kind="tall", canvas=(1080, 1200), hero=(1080, 700), thumb_extent=430,
    separator=SEPARATOR, footer_height=64, margin=32, title_size=50, price_size=76,
)
MOBILE_LAYOUT = CoverLayout(
    kind="tall", canvas=(1080, 1350), hero=(1080, 765), thumb_extent=510,
    separator=6, footer_height=69, margin=32, title_size=52, price_size=78,
)
MOBILE_SINGLE_LAYOUT = CoverLayout(
    kind="apartment", canvas=(1200, 900), hero=(1200, 838), thumb_extent=0,
    separator=0, footer_height=62, margin=42, title_size=58, price_size=82,
)
TALL_CANVAS = TALL_LAYOUT.canvas
APARTMENT_CANVAS = APARTMENT_LAYOUT.canvas
TALL_HERO_HEIGHT = TALL_LAYOUT.hero[1]
APARTMENT_HERO_HEIGHT = APARTMENT_LAYOUT.hero[1]

PAGE_BG = (255, 255, 255)      # only visible as the thin separators
FOOTER_BG = (22, 26, 30)
SHADE = (12, 14, 17)
GOLD = (226, 190, 105)
GOLD_LIGHT = (249, 226, 164)
SUBTLE_WHITE = (214, 219, 223)
CHIP_DARK = (40, 48, 55)
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
    terms: str = ""              # 押付·租期 from the offer (deposit/payment + contract); empty = omitted

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



def _brand_lockup() -> Image.Image | None:
    path = Path(__file__).resolve().parents[2] / "media_pipeline" / "assets" / "qiaolian_logo_black_gold_lockup.png"
    try:
        with Image.open(path) as raw:
            image = raw.convert("RGBA")
        bbox = image.getchannel("A").getbbox()
        return image.crop(bbox) if bbox else None
    except (OSError, ValueError):
        return None


def _scaled(image: Image.Image, height: int) -> Image.Image:
    width = max(1, round(image.width * height / image.height))
    return image.resize((width, height), Image.Resampling.LANCZOS)


def _fit_text(draw: ImageDraw.ImageDraw, value: str, *, max_width: int, start_size: int, min_size: int = 28,
              bold: bool = True) -> ImageFont.ImageFont:
    size = start_size
    while size > min_size:
        font = _font(size, bold=bold)
        if draw.textbbox((0, 0), value, font=font)[2] <= max_width:
            return font
        size -= 2
    return _font(min_size, bold=bold)


def _fit_line(draw: ImageDraw.ImageDraw, value: str, *, max_width: int, start_size: int, min_size: int,
              bold: bool = True) -> tuple[str, ImageFont.ImageFont]:
    """Shrink to ``min_size``; if still too wide, truncate with an ellipsis. Never clips."""
    font = _fit_text(draw, value, max_width=max_width, start_size=start_size, min_size=min_size, bold=bold)
    if draw.textlength(value, font=font) <= max_width:
        return value, font
    text = value
    while text and draw.textlength(text.rstrip() + "…", font=font) > max_width:
        text = text[:-1]
    return (text.rstrip() + "…") if text else "", font


def _cover_subtitle(data: CoverRenderData) -> str:
    """One fact line: property type + layout, without duplicated taxonomy."""
    kind = str(data.property_type or "").strip()
    layout = str(data.layout or "").strip()
    if not layout:
        return kind
    if not kind or kind in layout or layout in kind:
        return layout
    return f"{kind} | {layout}"


def _cover_title(data: CoverRenderData) -> str:
    return str(data.project or data.project_alias or data.area or data.property_type or "精选房源").strip()


def _pin_icon(size: int, color: tuple[int, int, int]) -> Image.Image:
    s = size * 4
    icon = Image.new("RGBA", (s, s), (0, 0, 0, 0))
    draw = ImageDraw.Draw(icon)
    cx, cy, r = s // 2, int(s * 0.38), int(s * 0.30)
    stroke = max(4, s // 9)
    draw.polygon([(cx - int(r * 0.78), cy + int(r * 0.6)), (cx + int(r * 0.78), cy + int(r * 0.6)), (cx, int(s * 0.95))],
                 fill=(*color, 255))
    draw.ellipse((cx - r, cy - r, cx + r, cy + r), fill=(*color, 255))
    inner = r - stroke
    draw.ellipse((cx - inner, cy - inner, cx + inner, cy + inner), fill=(0, 0, 0, 0))
    hole = int(r * 0.32)
    draw.ellipse((cx - hole, cy - hole, cx + hole, cy + hole), fill=(*color, 255))
    return icon.resize((size, size), Image.Resampling.LANCZOS)


def _pill(layer: Image.Image, box: tuple[int, int, int, int], *, fill, outline=None, width: int = 2) -> None:
    left, top, right, bottom = box
    radius = (bottom - top) // 2
    ImageDraw.Draw(layer).rounded_rectangle(box, radius=radius, fill=fill, outline=outline, width=width)


def _center_text(draw: ImageDraw.ImageDraw, box: tuple[int, int, int, int], text: str, font, fill) -> None:
    """Centre ``text`` on its ink box inside ``box`` (optical centring)."""
    ink = draw.textbbox((0, 0), text, font=font, anchor="ls")
    x = box[0] + (box[2] - box[0] - (ink[2] - ink[0])) // 2 - ink[0]
    y = box[1] + (box[3] - box[1] - (ink[3] - ink[1])) // 2 - ink[1]
    draw.text((x, y), text, font=font, fill=fill, anchor="ls")


def _draw_price_card(
    draw: ImageDraw.ImageDraw,
    data: CoverRenderData,
    *,
    right: int,
    bottom: int,
    height: int,
    amount_size: int,
    suffix_size: int,
    min_width: int,
    max_width: int,
) -> int | None:
    """Dark card with gold border; ``$amount`` + ``/月`` centred on their ink box.

    Returns the card's left edge (or None when there is no price).
    """
    price = data.price_line()
    if not price:
        return None
    suffix = "/月" if price.endswith("/月") else ""
    amount = price[:-2] if suffix else price
    pad_x = max(18, height // 3)
    gap = max(4, amount_size // 8)
    suffix_font = _font(suffix_size, bold=True)
    suffix_w = draw.textlength(suffix, font=suffix_font) if suffix else 0
    amount, amount_font = _fit_line(
        draw, amount, max_width=int(max_width - 2 * pad_x - suffix_w - gap),
        start_size=amount_size, min_size=max(18, amount_size * 3 // 5),
    )
    amount_box = draw.textbbox((0, 0), amount, font=amount_font, anchor="ls")
    suffix_box = draw.textbbox((0, 0), suffix, font=suffix_font, anchor="ls") if suffix else (0, 0, 0, 0)
    row_w = (amount_box[2] - amount_box[0]) + (gap + suffix_box[2] - suffix_box[0] if suffix else 0)
    ink_top = min(amount_box[1], suffix_box[1])
    ink_h = max(amount_box[3], suffix_box[3]) - ink_top
    width = int(min(max_width, max(min_width, row_w + 2 * pad_x)))
    left, top = right - width, bottom - height
    draw.rounded_rectangle((left, top, right, bottom), radius=10, fill=(*CHIP_DARK, 225), outline=(*GOLD, 255), width=2)
    baseline = top + (height - ink_h) // 2 - ink_top
    x = left + (width - row_w) // 2 - amount_box[0]
    draw.text((x, baseline), amount, font=amount_font, fill=GOLD_LIGHT, anchor="ls")
    if suffix:
        draw.text((x + amount_box[2] + gap - suffix_box[0], baseline), suffix, font=suffix_font,
                  fill=(255, 255, 255), anchor="ls")
    return left




def _hero_tag(data: CoverRenderData) -> str:
    """户型 · 类型 in one pill, without repeating the same taxonomy."""
    kind = str(data.property_type or "").strip()
    layout = str(data.layout or "").strip()
    if not layout:
        return kind
    if not kind or kind in layout or layout in kind:
        return layout
    return f"{layout} · {kind}"


def _shade(layer: Image.Image, box: tuple[int, int, int, int], *, top_alpha: int, bottom_alpha: int,
           top_span: int = 210, bottom_span: int = 280) -> None:
    """Dark only in a top band (title) and a bottom band (price); the middle stays untouched."""
    x0, y0, x1, y1 = box
    draw = ImageDraw.Draw(layer)
    for y in range(y0, y1):
        alpha = 0.0
        if y < y0 + top_span:
            alpha = max(alpha, top_alpha * (1 - (y - y0) / top_span) ** 1.5)
        if y > y1 - bottom_span:
            alpha = max(alpha, bottom_alpha * ((y - (y1 - bottom_span)) / bottom_span) ** 1.4)
        if alpha > 0:
            draw.line((x0, y, x1 - 1, y), fill=(*SHADE, int(alpha)))


def _title_parts(draw: ImageDraw.ImageDraw, data: CoverRenderData, *, max_width: int, size: int):
    pin = _pin_icon(int(size * 0.62), GOLD)
    title, title_font = _fit_line(draw, _cover_title(data), max_width=max_width - pin.width - 10,
                                  start_size=size, min_size=26)
    title_w = pin.width + 10 + int(draw.textlength(title, font=title_font))
    tag = _hero_tag(data)
    tag_font, tag_w = None, 0
    if tag:
        tag, tag_font = _fit_line(draw, tag, max_width=max_width - 36, start_size=int(size * 0.5), min_size=18)
        tag_w = int(draw.textlength(tag, font=tag_font)) + 36
    height = size + (16 + int(size * 0.5) + 20 if tag else 0) + 8
    return title, title_font, tag, tag_font, max(title_w, tag_w), height


def _title_block_size(data: CoverRenderData, *, max_width: int, size: int) -> tuple[int, int]:
    probe = ImageDraw.Draw(Image.new("RGBA", (8, 8)))
    *_, width, height = _title_parts(probe, data, max_width=max_width, size=size)
    return width, height


def _draw_title_block(layer: Image.Image, data: CoverRenderData, *, x: int, y: int, max_width: int, size: int,
                      align: str = "left", dark: bool = False) -> None:
    """📍 title + 户型·类型 pill. ``align`` centres both lines on the block; ``dark`` ink for bright sky."""
    draw = ImageDraw.Draw(layer)
    title, title_font, tag, tag_font, block_w, _ = _title_parts(draw, data, max_width=max_width, size=size)
    pin = _pin_icon(int(size * 0.62), DARK_GOLD if dark else GOLD)
    line_w = pin.width + 10 + int(draw.textlength(title, font=title_font))
    tx = x + (block_w - line_w) // 2 if align == "center" else (x + block_w - line_w if align == "right" else x)
    baseline = y + size
    layer.alpha_composite(pin, (tx, baseline - int(size * 0.78)))
    ink = (*DARK_INK, 255) if dark else (255, 255, 255, 255)
    draw.text((tx + pin.width + 10, baseline), title, font=title_font, fill=ink, anchor="ls")
    if not tag:
        return
    tag_w = int(draw.textlength(tag, font=tag_font))
    px = x + (block_w - tag_w - 36) // 2 if align == "center" else (x + block_w - tag_w - 36 if align == "right" else x)
    box = (px, baseline + 16, px + tag_w + 36, baseline + 16 + int(size * 0.5) + 20)
    _pill(layer, box, fill=(20, 24, 28, 200), outline=(*GOLD, 255), width=2)
    _center_text(draw, box, tag, tag_font, (*GOLD_LIGHT, 255))


def _soft_scrim(layer: Image.Image, box: tuple[int, int, int, int], alpha: int) -> None:
    """Blurred dark plate behind white ink on a mid-tone sky (no hard band across the photo)."""
    pad = 36
    x0, y0, x1, y1 = box
    plate = Image.new("L", (x1 - x0 + 2 * pad, y1 - y0 + 2 * pad), 0)
    ImageDraw.Draw(plate).rounded_rectangle((pad, pad, plate.width - pad, plate.height - pad), radius=28, fill=alpha)
    plate = plate.filter(ImageFilter.GaussianBlur(pad / 2))
    tint = Image.new("RGBA", plate.size, (*SHADE, 0))
    tint.putalpha(plate)
    layer.alpha_composite(tint, (x0 - pad, y0 - pad))


def _draw_price(layer: Image.Image, data: CoverRenderData, *, right: int, bottom: int, size: int,
                max_width: int) -> int | None:
    """Gold-bordered dark card; ``$amount`` (biggest text on the cover) + ``/月`` centred on their ink."""
    price = data.price_line()
    if not price:
        return None
    draw = ImageDraw.Draw(layer)
    suffix = "/月" if price.endswith("/月") else ""
    amount = price[:-2] if suffix else price
    for amount_size in range(size, max(24, size // 2) - 1, -2):
        amount_font = _font(amount_size, bold=True)
        suffix_font = _font(int(amount_size * 0.4), bold=True)
        pad, gap, height = int(amount_size * 0.4), 8, int(amount_size * 1.5)
        a_box = draw.textbbox((0, 0), amount, font=amount_font, anchor="ls")
        s_box = draw.textbbox((0, 0), suffix, font=suffix_font, anchor="ls") if suffix else (0, 0, 0, 0)
        width = (a_box[2] - a_box[0]) + ((gap + s_box[2] - s_box[0]) if suffix else 0) + 2 * pad
        if width <= max_width:
            break
    left, top = right - width, bottom - height
    draw.rounded_rectangle((left, top, right, bottom), radius=14, fill=(20, 24, 28, 215),
                           outline=(*GOLD, 255), width=3)
    ink_top = min(a_box[1], s_box[1]) if suffix else a_box[1]
    ink_bottom = max(a_box[3], s_box[3]) if suffix else a_box[3]
    baseline = top + (height - (ink_bottom - ink_top)) // 2 - ink_top
    x = left + pad - a_box[0]
    draw.text((x, baseline), amount, font=amount_font, fill=(*GOLD_LIGHT, 255), anchor="ls")
    if suffix:
        draw.text((x + a_box[2] + gap - s_box[0], baseline), suffix, font=suffix_font,
                  fill=(255, 255, 255, 255), anchor="ls")
    return left


def _thumb_label(layer: Image.Image, x: int, y: int, width: int, text: str) -> None:
    draw = ImageDraw.Draw(layer)
    text, font = _fit_line(draw, text or "实拍", max_width=max(40, width - 44), start_size=18, min_size=12)
    text_w = int(draw.textlength(text, font=font))
    box = (x + 10, y + 10, x + 10 + text_w + 24, y + 42)
    _pill(layer, box, fill=(20, 22, 25, 150))
    _center_text(draw, box, text, font, (255, 255, 255, 245))


def _draw_footer(layer: Image.Image, geo: CoverLayout, lockup: Image.Image | None) -> None:
    draw = ImageDraw.Draw(layer)
    W, H = layer.size
    top, height = geo.footer_top, geo.footer_height
    draw.rectangle((0, top, W, H), fill=(*FOOTER_BG, 255))
    mid = top + height // 2
    x = geo.margin + 8
    if lockup is not None:
        house = lockup.crop((0, 0, round(lockup.width * 0.27), lockup.height))
        bbox = house.getchannel("A").getbbox()
        if bbox:
            icon = _scaled(house.crop(bbox), int(height * 0.48))
            layer.alpha_composite(icon, (x, mid - icon.height // 2))
            x += icon.width + 10
    draw.text((x, mid), "侨联地产", font=_font(int(height * 0.4), bold=True), fill=(*GOLD_LIGHT, 255), anchor="lm")


def _is_apartment(data: CoverRenderData) -> bool:
    value = str(data.property_type or "").strip().lower()
    return any(token in value for token in ("公寓", "服务式", "apartment", "condo", "studio"))


TEMPLATES = ("apartment_3x2", "villa_black_gold", "villa_4x5", "villa_compact", "landscape_3x2", "grid_2x2", "single", "mobile_4x5", "mobile_3x2", "mobile_single_4x3", "mobile_duo_4x5", "mobile_collage_4x5")
VILLA_TEMPLATES = ("villa_black_gold", "villa_4x5", "villa_compact")
DEFAULT_VILLA_TEMPLATE = "villa_black_gold"
SKY_TEXT_TEMPLATES = {"villa_4x5", "villa_compact"}
DARK_INK = (24, 30, 36)
DARK_GOLD = (170, 122, 30)
BRIGHT_SKY_LUMINANCE = 168


def _resolve_template(data: CoverRenderData, template: str | None) -> str:
    value = str(template or "").strip().lower()
    if value in TEMPLATES:
        return value
    return "apartment_3x2" if _is_apartment(data) else villa_template_from_env()


def villa_template_from_env() -> str:
    """``V3_VILLA_COVER_TEMPLATE``: villa_black_gold (default, Gym's 1080x1388 reference layout),
    villa_4x5 (V-A 1080x1350, title in the sky) or villa_compact (V-B 1080x1200)."""
    value = (os.environ.get("V3_VILLA_COVER_TEMPLATE") or "").strip().lower()
    return value if value in VILLA_TEMPLATES else DEFAULT_VILLA_TEMPLATE


def _layout(data: CoverRenderData, template: str | None = None) -> CoverLayout:
    value = _resolve_template(data, template)
    if value in {"mobile_duo_4x5", "mobile_collage_4x5"}:
        return MOBILE_LAYOUT
    if value == "mobile_single_4x3":
        return MOBILE_SINGLE_LAYOUT
    if value == "mobile_3x2":
        # Telegram displays landscape covers at the full message width. Reuse
        # the proven 3:2 left-hero/right-details geometry for every property
        # type so room photos and price text remain legible on a phone.
        return APARTMENT_LAYOUT
    if value == "mobile_4x5":
        return MOBILE_LAYOUT
    if value == "grid_2x2":
        return GRID_LAYOUT
    if value in ("villa_4x5", "villa_black_gold"):
        return TALL_LAYOUT
    if value == "villa_compact":
        return TALL_COMPACT_LAYOUT
    if value == "single":
        return APARTMENT_LAYOUT if _is_apartment(data) else TALL_LAYOUT
    return APARTMENT_LAYOUT


def _thumb_boxes(geo: CoverLayout, count: int) -> list[tuple[int, int, int, int]]:
    """(x, y, w, h) for up to three thumbnails filling the non-hero area edge to edge."""
    if count <= 0:
        return []
    sep = geo.separator
    if geo.kind == "grid":
        w = (geo.canvas[0] - sep) // 2
        h = (geo.footer_top - sep) // 2
        cells = [(0, 0, w, h), (w + sep, 0, geo.canvas[0] - w - sep, h),
                 (0, h + sep, w, geo.footer_top - h - sep), (w + sep, h + sep, geo.canvas[0] - w - sep, geo.footer_top - h - sep)]
        return cells[1:1 + count]
    if geo.kind == "apartment":
        x, total = geo.hero[0] + sep, geo.hero[1] - sep * (count - 1)
        heights = [total // count] * count
        heights[-1] += total - sum(heights)
        boxes, y = [], 0
        for h in heights:
            boxes.append((x, y, geo.thumb_extent, h))
            y += h + sep
        return boxes
    y, total = geo.hero[1] + sep, geo.canvas[0] - sep * (count - 1)
    widths = [total // count] * count
    widths[-1] += total - sum(widths)
    boxes, x = [], 0
    for w in widths:
        boxes.append((x, y, w, geo.thumb_extent))
        x += w + sep
    return boxes


def _layout_geometry(data: CoverRenderData, template: str | None = None) -> dict[str, Any]:
    if _resolve_template(data, template) == "villa_black_gold":
        from .villa_card import GEOMETRY as g, thumb_columns

        return {"canvas": g.canvas, "hero": (g.canvas[0], g.hero_height),
                "thumbs": [(x, g.thumb_top, w, g.thumb_bottom - g.thumb_top) for x, w in thumb_columns(3)],
                "footer_top": g.footer_top, "footer_height": g.canvas[1] - g.footer_top,
                "bar": (g.hero_height, g.bar_bottom), "price_card": g.price_card, "template": "villa_black_gold"}
    geo = _layout(data, template)
    hero = geo.hero
    if _resolve_template(data, template) in {"single", "mobile_single_4x3"}:
        hero = (geo.canvas[0], geo.footer_top)
    return {"canvas": geo.canvas, "hero": hero, "thumbs": [] if _resolve_template(data, template) in {"single", "mobile_single_4x3"}
            else _thumb_boxes(geo, 3), "footer_top": geo.footer_top, "footer_height": geo.footer_height,
            "template": _resolve_template(data, template)}


def render_cover(*, style: str, source_image: str, output_path: str, data: CoverRenderData,
                 source_images: Iterable[str] | None = None, source_labels: Iterable[str] | None = None,
                 layout: str = "auto") -> str:
    """Render the hero plus up to three photos in the planned template.

    ``layout`` is a template name from ``TEMPLATES`` (``auto``/unknown → by
    property type). Empty labels are not drawn: an uncertain room is shown as a
    plain photo, never with a guessed name.
    """
    del style
    template = _resolve_template(data, layout)
    if template == "mobile_single_4x3":
        # The channel caption already carries type/layout. Keep the cover to
        # two instant-scan facts: location/title and price.
        data = replace(data, property_type="", layout="")
    candidates = [source_image]
    if template not in {"single", "mobile_single_4x3"}:
        candidates.extend(list(source_images or ()))
    images = _usable_images(candidates)
    if not images:
        raise ValueError("cover_no_usable_images")
    if template == "mobile_collage_4x5" and len(images) >= 3:
        # Phone-feed 3:2: one dominant photo plus two or three supporting
        # photos. Geometry is tuned separately so supporting rooms remain clear.
        W, H, gap = 1200, 800, 6
        canvas = Image.new("RGB", (W, H), PAGE_BG)
        chosen = images[:4]
        side_count = len(chosen) - 1
        hero_w = round(W * (0.58 if side_count == 2 else 0.62))
        side_x = hero_w + gap
        side_w = W - side_x
        usable_height = H - gap * (side_count - 1)
        ratios = (0.56, 0.44) if side_count == 2 else (0.36, 0.34, 0.30)
        heights = [round(usable_height * ratio) for ratio in ratios[:-1]]
        heights.append(usable_height - sum(heights))
        boxes = [(0, 0, hero_w, H)]
        y = 0
        for height in heights:
            boxes.append((side_x, y, side_w, height))
            y += height + gap
        for image, (x, y, w, h) in zip(chosen, boxes):
            canvas.paste(_fit(image, (w, h)), (x, y))

        layer = Image.new("RGBA", (W, H), (0, 0, 0, 0))
        draw = ImageDraw.Draw(layer)
        title = _cover_title(data)
        detail = _cover_subtitle(data)
        price = data.price_line()
        # Float a compact information lockup over the quiet upper-left wall.
        # A soft offset shadow supplies contrast without an ad-like panel.
        if price:
            price, price_font = _fit_line(
                draw, price, max_width=hero_w - 80, start_size=62, min_size=42, bold=True
            )
            draw.text((45, 40), price, font=price_font, fill=(0, 0, 0, 120))
            draw.text((42, 36), price, font=price_font, fill=(255, 255, 255, 255))
        meta = " · ".join(part for part in (title, detail.replace(" | ", " ")) if part)
        if meta:
            meta, meta_font = _fit_line(
                draw, meta, max_width=hero_w - 80, start_size=28, min_size=21, bold=False
            )
            draw.text((46, 115), meta, font=meta_font, fill=(0, 0, 0, 115))
            draw.text((44, 112), meta, font=meta_font, fill=(255, 255, 255, 245))
        brand_font = _font(24, bold=True)
        brand = "侨联地产"
        brand_w = draw.textlength(brand, font=brand_font)
        brand_pos = (hero_w - 28 - brand_w, H - 42)
        draw.text((brand_pos[0] + 2, brand_pos[1] + 2), brand, font=brand_font, fill=(0, 0, 0, 80))
        draw.text(brand_pos, brand, font=brand_font, fill=(255, 255, 255, 155))
        canvas.paste(layer, (0, 0), layer)
        output = Path(output_path).expanduser().resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        canvas.save(output, format="JPEG" if output.suffix.lower() in {".jpg", ".jpeg"} else "PNG", quality=94, optimize=True)
        return str(output)
    if template == "mobile_duo_4x5" and len(images) > 1:
        # Phone-first editorial cover: two genuinely large photos, no miniature
        # thumbnail rail.  A single compact glass band carries the only facts
        # needed before the reader opens the full caption.
        W, H = 1080, 1350
        seam = 675
        canvas = Image.new("RGB", (W, H), PAGE_BG)
        canvas.paste(_fit(images[0], (W, seam)), (0, 0))
        canvas.paste(_fit(images[1], (W, H - seam)), (0, seam))
        layer = Image.new("RGBA", (W, H), (0, 0, 0, 0))
        draw = ImageDraw.Draw(layer)
        band_top, band_bottom = 560, 790
        draw.rectangle((0, band_top, W, band_bottom), fill=(10, 13, 16, 218))
        draw.rectangle((0, band_top, W, band_top + 5), fill=(*GOLD, 255))

        title = _cover_title(data)
        title, title_font = _fit_line(
            draw, title, max_width=620, start_size=58, min_size=38, bold=True
        )
        draw.text((44, 596), title, font=title_font, fill=(255, 255, 255, 255))
        detail = _cover_subtitle(data)
        if detail:
            detail, detail_font = _fit_line(
                draw, detail, max_width=620, start_size=34, min_size=25, bold=False
            )
            draw.text((46, 684), detail, font=detail_font, fill=(*SUBTLE_WHITE, 255))

        price = data.price_line()
        if price:
            suffix = "/月" if price.endswith("/月") else ""
            amount = price[:-2] if suffix else price
            amount, amount_font = _fit_line(
                draw, amount, max_width=330, start_size=72, min_size=48, bold=True
            )
            amount_width = draw.textlength(amount, font=amount_font)
            draw.text((W - 44 - amount_width, 604), amount, font=amount_font, fill=(*GOLD_LIGHT, 255))
            if suffix:
                draw.text((W - 47, 696), suffix, font=_font(27, bold=True), fill=(255, 255, 255, 230), anchor="ra")

        draw.text((44, 34), "侨联地产", font=_font(34, bold=True), fill=(255, 255, 255, 235), stroke_width=2, stroke_fill=(0, 0, 0, 130))
        canvas.paste(layer, (0, 0), layer)
        output = Path(output_path).expanduser().resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        canvas.save(output, format="JPEG" if output.suffix.lower() in {".jpg", ".jpeg"} else "PNG", quality=94, optimize=True)
        return str(output)
    if template == "villa_black_gold":
        if len(images) > 1:
            from .villa_card import render_villa_card

            return str(render_villa_card(data=data, images=images, labels=source_labels or (),
                                         output=Path(output_path).expanduser().resolve()))
        template = "villa_4x5"              # no detail photos: the full-height hero template
    geo = _layout(data, template)
    W, H = geo.canvas
    canvas = Image.new("RGB", geo.canvas, PAGE_BG)
    hero_w, hero_h = geo.hero
    thumbs = images[1:4]
    if template == "grid_2x2" and len(thumbs) < 3:
        template, geo = "apartment_3x2", APARTMENT_LAYOUT
        W, H = geo.canvas
        canvas = Image.new("RGB", geo.canvas, PAGE_BG)
        hero_w, hero_h = geo.hero
    if not thumbs:
        # No detail photos: the hero takes the whole photo area (no empty band).
        hero_w, hero_h = W, geo.footer_top
    sky_text = template in SKY_TEXT_TEMPLATES and bool(thumbs)
    m = geo.margin
    block = _title_block_size(data, max_width=hero_w - 2 * m - 16, size=geo.title_size)
    if sky_text:
        centering = frame_offset(images[0], (hero_w, hero_h), text_room=block[1] + 2 * m)
        hero = ImageOps.fit(images[0], (hero_w, hero_h), method=Image.Resampling.LANCZOS, centering=centering)
    else:
        hero = _fit(images[0], (hero_w, hero_h))
    canvas.paste(hero, (0, 0))
    slot = text_slot(hero, block, margin=m + 8) if sky_text else None
    layer = Image.new("RGBA", canvas.size, (0, 0, 0, 0))
    labels = list(source_labels or ())
    boxes = _thumb_boxes(geo, len(thumbs))
    for index, (image, (x, y, w, h)) in enumerate(zip(thumbs, boxes)):
        canvas.paste(_fit(image, (w, h)), (x, y))
        label = labels[index] if index < len(labels) else ""
        if label:
            _thumb_label(layer, x, y, w, label)
    if geo.kind == "grid":
        _shade(layer, (0, 0, hero_w, hero_h), top_alpha=170, bottom_alpha=0, top_span=200, bottom_span=1)
        last = boxes[-1] if boxes else (0, 0, hero_w, hero_h)
        _shade(layer, (last[0], last[1], last[0] + last[2], last[1] + last[3]), top_alpha=0, bottom_alpha=200,
               top_span=1, bottom_span=200)
        _draw_price(layer, data, right=last[0] + last[2] - m, bottom=last[1] + last[3] - m,
                    size=geo.price_size, max_width=last[2] - 2 * m)
        _draw_title_block(layer, data, x=m, y=m - 6, max_width=hero_w - 2 * m, size=geo.title_size)
    else:
        tall = geo.kind == "tall"
        if slot is not None:
            # Title sits in the open sky: no top band; bottom shade only for the price card.
            _shade(layer, (0, 0, hero_w, hero_h), top_alpha=0, bottom_alpha=210, top_span=1)
            dark = slot.luminance >= BRIGHT_SKY_LUMINANCE
            if not dark:
                _soft_scrim(layer, (slot.x, slot.y, slot.x + slot.width, slot.y + slot.height), 120)
        else:
            _shade(layer, (0, 0, hero_w, hero_h), top_alpha=170, bottom_alpha=210 if tall else 200)
        _draw_price(layer, data, right=hero_w - m - (8 if tall else 0),
                    bottom=hero_h - m - (4 if tall else 0),
                    size=geo.price_size, max_width=hero_w - 2 * m)
        if slot is not None:
            _draw_title_block(layer, data, x=slot.x, y=slot.y, max_width=hero_w - 2 * m - 16,
                              size=geo.title_size, align=slot.align, dark=slot.luminance >= BRIGHT_SKY_LUMINANCE)
        else:
            _draw_title_block(layer, data, x=m + (8 if tall else 0), y=m - (0 if tall else 6),
                              max_width=640 if tall else (hero_w - 2 * m), size=geo.title_size)
    _draw_footer(layer, geo, _brand_lockup())
    canvas.paste(layer, (0, 0), layer)
    output = Path(output_path).expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.suffix.lower() in {".jpg", ".jpeg"}:
        canvas.save(output, format="JPEG", quality=92, optimize=True)
    else:
        canvas.save(output, format="PNG", optimize=True)
    return str(output)


__all__ = ["CoverRenderData", "DEFAULT_VILLA_TEMPLATE", "TEMPLATES", "VILLA_TEMPLATES", "render_cover", "villa_template_from_env"]
