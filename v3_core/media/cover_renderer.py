"""DB-free Pillow renderer for Telegram property covers.

Layout follows Gym's 2026-10-10 reference PDF (measured at 1080px wide):

* villa / house / unrecognised types: 1080x1350 (4:5). Tall hero with a small
  brand pill top-left, pin + title + subtitle on a bottom gradient, price card
  bottom-right; three rounded thumbnails on light grey; light footer.
* apartment / 服务式 / condo / studio: 1080x810 (4:3). Shorter hero with a
  type tag top-left, location pill top-right, price card bottom-right; three
  rounded thumbnails; same footer.

Brand artwork is always our gold house lockup (侨联地产 / QIAO LIAN PROPERTY).
"""
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable
from PIL import Image, ImageDraw, ImageFont, ImageOps


@dataclass(frozen=True)
class CoverLayout:
    canvas: tuple[int, int]
    hero_height: int
    margin: int
    gallery_top: int
    gallery_height: int
    gallery_gap: int
    gallery_radius: int
    footer_top: int


# Geometry measured from the reference PDF rendered at 1080px wide.
# Villa: shorter hero than the PDF (1011) so thumbnails are near-square
# (~322x350) instead of flat strips; 16px gaps and the 86px footer are kept.
TALL_LAYOUT = CoverLayout(
    canvas=(1080, 1350), hero_height=881, margin=40,
    gallery_top=897, gallery_height=350, gallery_gap=16, gallery_radius=14,
    footer_top=1264,
)
APARTMENT_LAYOUT = CoverLayout(
    canvas=(1080, 810), hero_height=470, margin=32,
    gallery_top=485, gallery_height=140, gallery_gap=14, gallery_radius=14,
    footer_top=640,
)
TALL_CANVAS = TALL_LAYOUT.canvas
APARTMENT_CANVAS = APARTMENT_LAYOUT.canvas
TALL_HERO_HEIGHT = TALL_LAYOUT.hero_height
APARTMENT_HERO_HEIGHT = APARTMENT_LAYOUT.hero_height

PAGE_BG = (238, 240, 243)
DIVIDER = (220, 221, 223)
GOLD = (226, 190, 105)
GOLD_LIGHT = (249, 226, 164)
GOLD_DEEP = (168, 128, 52)  # gold that still reads on the light footer
INK = (38, 48, 57)
MUTED = (122, 130, 138)
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



def _rounded_mask(size: tuple[int, int], radius: int, scale: int = 4) -> Image.Image:
    w, h = size
    mask = Image.new("L", (w * scale, h * scale), 0)
    ImageDraw.Draw(mask).rounded_rectangle((0, 0, w * scale - 1, h * scale - 1), radius=radius * scale, fill=255)
    return mask.resize((w, h), Image.Resampling.LANCZOS)


def _paste_gallery(canvas: Image.Image, images: list[Image.Image], geo: CoverLayout) -> list[tuple[int, int]]:
    """Hero full-bleed, then up to three rounded thumbnails. Returns thumb (x, width)."""
    canvas.paste(_fit(images[0], (canvas.width, geo.hero_height)), (0, 0))
    secondary = images[1:4]
    if not secondary:
        return []
    count = len(secondary)
    inner = canvas.width - 2 * geo.margin - geo.gallery_gap * (count - 1)
    widths = [inner // count] * count
    widths[-1] += inner - sum(widths)
    cells: list[tuple[int, int]] = []
    x = geo.margin
    for image, width in zip(secondary, widths):
        tile = _fit(image, (width, geo.gallery_height))
        canvas.paste(tile, (x, geo.gallery_top), _rounded_mask(tile.size, geo.gallery_radius))
        cells.append((x, width))
        x += width + geo.gallery_gap
    return cells


def _brand_lockup() -> Image.Image | None:
    path = Path(__file__).resolve().parents[2] / "media_pipeline" / "assets" / "qiaolian_logo_black_gold_lockup.png"
    try:
        with Image.open(path) as raw:
            image = raw.convert("RGBA")
        bbox = image.getchannel("A").getbbox()
        return image.crop(bbox) if bbox else None
    except (OSError, ValueError):
        return None


def _footer_lockup(lockup: Image.Image) -> Image.Image:
    """Same lockup for the light footer: house in deeper gold, wordmark inked dark."""
    house_right = round(lockup.width * 0.27)
    cn_bottom = round(lockup.height * 0.70)
    alpha = lockup.getchannel("A")
    out = lockup.copy()
    ink = Image.new("RGBA", lockup.size, (*INK, 255))
    muted = Image.new("RGBA", lockup.size, (*MUTED, 255))
    cn_mask = Image.new("L", lockup.size, 0)
    cn_mask.paste(alpha.crop((house_right, 0, lockup.width, cn_bottom)), (house_right, 0))
    en_mask = Image.new("L", lockup.size, 0)
    en_mask.paste(alpha.crop((house_right, cn_bottom, lockup.width, lockup.height)), (house_right, cn_bottom))
    house_mask = Image.new("L", lockup.size, 0)
    house_mask.paste(alpha.crop((0, 0, house_right, lockup.height)), (0, 0))
    out.paste(Image.new("RGBA", lockup.size, (*GOLD_DEEP, 255)), (0, 0), house_mask)
    out.paste(ink, (0, 0), cn_mask)
    out.paste(muted, (0, 0), en_mask)
    out.putalpha(alpha)
    return out


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


def _type_tag(data: CoverRenderData) -> str:
    return str(data.layout or data.property_type or "").strip()


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


def _gallery_labels(layer: Image.Image, labels: list[str], cells: list[tuple[int, int]], geo: CoverLayout) -> None:
    draw = ImageDraw.Draw(layer)
    small = geo.gallery_height < 200
    size, h, inset = (16, 28, 10) if small else (18, 32, 12)
    for index, (x, width) in enumerate(cells):
        label = labels[index] if index < len(labels) and labels[index] else "实拍"
        label, font = _fit_line(draw, label, max_width=width - 2 * inset - 24, start_size=size, min_size=12)
        text_w = int(draw.textlength(label, font=font))
        box = (x + inset, geo.gallery_top + inset, x + inset + text_w + 24, geo.gallery_top + inset + h)
        _pill(layer, box, fill=(20, 22, 25, 150))
        _center_text(draw, box, label, font, (255, 255, 255, 245))


def _hero_gradient(layer: Image.Image, *, top: int, bottom: int, max_alpha: int) -> None:
    draw = ImageDraw.Draw(layer)
    span = max(1, bottom - top)
    for y in range(top, bottom):
        t = (y - top) / span
        draw.line((0, y, layer.width, y), fill=(22, 26, 30, int(max_alpha * (t ** 1.4))))


def _draw_villa_hero(layer: Image.Image, data: CoverRenderData, geo: CoverLayout, lockup: Image.Image | None) -> None:
    draw = ImageDraw.Draw(layer)
    m, hero_h, W = geo.margin, geo.hero_height, layer.width
    # Small brand pill top-left (our gold house lockup).
    if lockup is not None:
        logo = _scaled(lockup, 38)
        box = (m, m, m + logo.width + 32, m + 56)
        _pill(layer, box, fill=(18, 20, 23, 135))
        layer.alpha_composite(logo, (m + 16, m + 9))
    else:
        box = (m, m, m + 150, m + 46)
        _pill(layer, box, fill=(18, 20, 23, 135))
        _center_text(draw, box, "侨联地产", _font(20, bold=True), (255, 255, 255, 255))
    # Bottom gradient + facts.
    _hero_gradient(layer, top=hero_h - 300, bottom=hero_h, max_alpha=215)
    price_left = _draw_price_card(draw, data, right=W - m, bottom=hero_h - 37, height=78,
                                  amount_size=40, suffix_size=19, min_width=200, max_width=340)
    text_right = (price_left if price_left is not None else W - m) - 24
    pin = _pin_icon(22, GOLD)
    title_x = m + pin.width + 10
    title, title_font = _fit_line(draw, _cover_title(data), max_width=text_right - title_x, start_size=42, min_size=28)
    title_baseline = hero_h - 80
    layer.alpha_composite(pin, (m, title_baseline - 30))
    draw.text((title_x, title_baseline), title, font=title_font, fill=(255, 255, 255, 255), anchor="ls")
    subtitle = _cover_subtitle(data)
    if subtitle:
        subtitle, sub_font = _fit_line(draw, subtitle, max_width=text_right - m, start_size=23, min_size=18, bold=False)
        draw.text((m + 2, hero_h - 41), subtitle, font=sub_font, fill=(*SUBTLE_WHITE, 255), anchor="ls")


def _draw_apartment_hero(layer: Image.Image, data: CoverRenderData, geo: CoverLayout) -> None:
    draw = ImageDraw.Draw(layer)
    m, hero_h, W = 28, geo.hero_height, layer.width
    # Gentle top shade so both chips read on bright ceilings.
    for y in range(0, 120):
        draw.line((0, y, W, y), fill=(18, 20, 23, int(70 * (1 - y / 120))))
    # Location pill top-right (project name).
    pin = _pin_icon(18, (255, 255, 255))
    loc, loc_font = _fit_line(draw, _cover_title(data), max_width=480, start_size=19, min_size=14)
    loc_w = int(draw.textlength(loc, font=loc_font))
    pill_w = 18 + pin.width + 8 + loc_w + 20
    loc_box = (W - m - pill_w, m, W - m, m + 38)
    _pill(layer, loc_box, fill=(*CHIP_DARK, 230))
    layer.alpha_composite(pin, (loc_box[0] + 18, m + 10))
    _center_text(draw, (loc_box[0] + 18 + pin.width + 8, m, loc_box[0] + 18 + pin.width + 8 + loc_w, m + 38),
                 loc, loc_font, (255, 255, 255, 255))
    # Type tag top-left (layout, fallback property type); never collides with the location pill.
    tag = _type_tag(data)
    if tag:
        max_tag = max(80, loc_box[0] - 2 * m - 16 - 36)
        tag, tag_font = _fit_line(draw, tag, max_width=min(360, max_tag), start_size=19, min_size=14)
        tag_w = int(draw.textlength(tag, font=tag_font))
        tag_box = (m, m, m + tag_w + 36, m + 40)
        _pill(layer, tag_box, fill=(*CHIP_DARK, 230), outline=(*GOLD, 255), width=2)
        _center_text(draw, tag_box, tag, tag_font, (*GOLD_LIGHT, 255))
    _draw_price_card(draw, data, right=W - 29, bottom=hero_h - 30, height=63,
                     amount_size=32, suffix_size=16, min_width=150, max_width=320)


def _draw_footer(layer: Image.Image, data: CoverRenderData, geo: CoverLayout, lockup: Image.Image | None) -> None:
    draw = ImageDraw.Draw(layer)
    W, H = layer.size
    top = geo.footer_top
    draw.rectangle((0, top, W, H), fill=(*PAGE_BG, 255))
    draw.line((0, top - 1, W, top - 1), fill=(*DIVIDER, 255))
    mid = top + (H - top) // 2
    tall = geo is TALL_LAYOUT or geo.canvas == TALL_CANVAS
    m = geo.margin
    if lockup is not None:
        logo = _scaled(_footer_lockup(lockup), 44 if tall else 40)
        layer.alpha_composite(logo, (m, mid - logo.height // 2))
    else:
        draw.text((m, mid), "侨联地产", font=_font(24, bold=True), fill=(*INK, 255), anchor="lm")
    label = "出租房源" if str(data.deal_type or "rent").lower() == "rent" else "出售房源"
    font = _font(20 if tall else 18, bold=True)
    label_w = int(draw.textlength(label, font=font))
    pill_h = 46 if tall else 40
    right = W - m
    box = (right - label_w - 44, mid - pill_h // 2, right, mid - pill_h // 2 + pill_h)
    _pill(layer, box, fill=(*PAGE_BG, 255), outline=(*GOLD_DEEP, 255), width=2)
    _center_text(draw, box, label, font, (*GOLD_DEEP, 255))


def _is_apartment(data: CoverRenderData) -> bool:
    value = str(data.property_type or "").strip().lower()
    return any(token in value for token in ("公寓", "服务式", "apartment", "condo", "studio"))


def _layout(data: CoverRenderData) -> CoverLayout:
    return APARTMENT_LAYOUT if _is_apartment(data) else TALL_LAYOUT


def _layout_geometry(data: CoverRenderData) -> tuple[tuple[int, int], int, int, int, int]:
    """Return canvas, hero height, gallery top/height and footer top."""
    geo = _layout(data)
    return geo.canvas, geo.hero_height, geo.gallery_top, geo.gallery_height, geo.footer_top


def render_cover(*, style: str, source_image: str, output_path: str, data: CoverRenderData,
                 source_images: Iterable[str] | None = None, source_labels: Iterable[str] | None = None,
                 layout: str = "hero_three") -> str:
    """Render every property type with one hero image above three detail images."""
    # All style aliases intentionally converge on the same channel-first layout.
    del style, layout
    candidates = [source_image]
    candidates.extend(list(source_images or ()))
    images = _usable_images(candidates)
    if not images:
        raise ValueError("cover_no_usable_images")
    geo = _layout(data)
    canvas = Image.new("RGB", geo.canvas, PAGE_BG)
    cells = _paste_gallery(canvas, images, geo)
    layer = Image.new("RGBA", canvas.size, (0, 0, 0, 0))
    lockup = _brand_lockup()
    if geo is APARTMENT_LAYOUT:
        _draw_apartment_hero(layer, data, geo)
    else:
        _draw_villa_hero(layer, data, geo, lockup)
    _gallery_labels(layer, list(source_labels or ()), cells, geo)
    _draw_footer(layer, data, geo, lockup)
    canvas.paste(layer, (0, 0), layer)
    output = Path(output_path).expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.suffix.lower() in {".jpg", ".jpeg"}:
        canvas.save(output, format="JPEG", quality=92, optimize=True)
    else:
        canvas.save(output, format="PNG", optimize=True)
    return str(output)

__all__ = ["CoverRenderData", "render_cover"]
