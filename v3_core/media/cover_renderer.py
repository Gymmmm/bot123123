"""DB-free Pillow renderer for Telegram property covers (Gym 2026-10-11).

Templates (chosen upstream by ``cover_plan``; ``auto`` follows the property type):

* ``apartment_3x2``: 1200x800 landscape. Hero on the left (798x738) and a
  right column of three stacked thumbnails (396 wide), 6px white separators,
  62px dark brand footer. Same proportions as the approved 1080x720 sample.
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
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable
from PIL import Image, ImageDraw, ImageFont, ImageOps


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


def _draw_title_block(layer: Image.Image, data: CoverRenderData, *, x: int, y: int, max_width: int, size: int) -> None:
    draw = ImageDraw.Draw(layer)
    pin = _pin_icon(int(size * 0.62), GOLD)
    title, title_font = _fit_line(draw, _cover_title(data), max_width=max_width - pin.width - 10,
                                  start_size=size, min_size=26)
    baseline = y + size
    layer.alpha_composite(pin, (x, baseline - int(size * 0.78)))
    draw.text((x + pin.width + 10, baseline), title, font=title_font, fill=(255, 255, 255, 255), anchor="ls")
    tag = _hero_tag(data)
    if not tag:
        return
    tag, tag_font = _fit_line(draw, tag, max_width=max_width - 36, start_size=int(size * 0.5), min_size=18)
    tag_w = int(draw.textlength(tag, font=tag_font))
    box = (x, baseline + 16, x + tag_w + 36, baseline + 16 + int(size * 0.5) + 20)
    _pill(layer, box, fill=(20, 24, 28, 200), outline=(*GOLD, 255), width=2)
    _center_text(draw, box, tag, tag_font, (*GOLD_LIGHT, 255))


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


TEMPLATES = ("apartment_3x2", "villa_4x5", "landscape_3x2", "grid_2x2", "single")


def _resolve_template(data: CoverRenderData, template: str | None) -> str:
    value = str(template or "").strip().lower()
    if value in TEMPLATES:
        return value
    return "apartment_3x2" if _is_apartment(data) else "villa_4x5"


def _layout(data: CoverRenderData, template: str | None = None) -> CoverLayout:
    value = _resolve_template(data, template)
    if value == "grid_2x2":
        return GRID_LAYOUT
    if value == "villa_4x5":
        return TALL_LAYOUT
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
    geo = _layout(data, template)
    hero = geo.hero
    if _resolve_template(data, template) == "single":
        hero = (geo.canvas[0], geo.footer_top)
    return {"canvas": geo.canvas, "hero": hero, "thumbs": [] if _resolve_template(data, template) == "single"
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
    candidates = [source_image]
    if template != "single":
        candidates.extend(list(source_images or ()))
    images = _usable_images(candidates)
    if not images:
        raise ValueError("cover_no_usable_images")
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
    canvas.paste(_fit(images[0], (hero_w, hero_h)), (0, 0))
    layer = Image.new("RGBA", canvas.size, (0, 0, 0, 0))
    labels = list(source_labels or ())
    boxes = _thumb_boxes(geo, len(thumbs))
    for index, (image, (x, y, w, h)) in enumerate(zip(thumbs, boxes)):
        canvas.paste(_fit(image, (w, h)), (x, y))
        label = labels[index] if index < len(labels) else ""
        if label:
            _thumb_label(layer, x, y, w, label)
    m = geo.margin
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
        _shade(layer, (0, 0, hero_w, hero_h), top_alpha=170, bottom_alpha=210 if tall else 200)
        _draw_price(layer, data, right=hero_w - m - (8 if tall else 0),
                    bottom=hero_h - m - (4 if tall else 0),
                    size=geo.price_size, max_width=hero_w - 2 * m)
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


__all__ = ["CoverRenderData", "TEMPLATES", "render_cover"]
