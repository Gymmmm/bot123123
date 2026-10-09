"""DB-free Pillow renderer for Telegram property covers."""
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable
from PIL import Image, ImageDraw, ImageFont, ImageOps

# 上大下三 card: hero + dark info bar + three detail photos + white footer.
# Apartment: 590 + 145 + 360 + 108 = 1203 (hero ~1.83:1, wider).
# Villa/house: 810 + 145 + 360 + 108 = 1423 (hero 4:3, taller).
GUTTER = 8
APARTMENT_HERO_HEIGHT = 590
TALL_HERO_HEIGHT = 810
INFO_HEIGHT = 145
GALLERY_HEIGHT = 360
FOOTER_HEIGHT = 108
APARTMENT_CANVAS = (1080, APARTMENT_HERO_HEIGHT + INFO_HEIGHT + GALLERY_HEIGHT + FOOTER_HEIGHT)
TALL_CANVAS = (1080, TALL_HERO_HEIGHT + INFO_HEIGHT + GALLERY_HEIGHT + FOOTER_HEIGHT)
INFO_BG = (26, 20, 14)
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

def _paste_gallery(
    canvas: Image.Image,
    images: list[Image.Image],
    *,
    hero_height: int,
    gallery_top: int,
    gallery_height: int,
    layout: str = "hero_three",
) -> None:
    # All style aliases intentionally converge on the same channel-first layout.
    # This keeps apartment/townhouse/villa posts recognisably in one system.
    del layout
    canvas.paste(_fit(images[0], (1080, hero_height)), (0, 0))
    secondary = images[1:4]
    if not secondary:
        return
    count = len(secondary)
    total_gutter = GUTTER * (count - 1)
    widths = [(1080 - total_gutter) // count] * count
    widths[-1] += 1080 - total_gutter - sum(widths)
    x = 0
    for image, width in zip(secondary, widths):
        canvas.paste(_fit(image, (width, gallery_height)), (x, gallery_top))
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


def _cover_subtitle(data: CoverRenderData) -> str:
    """Return one public-facing fact line without duplicated taxonomy."""
    return str(data.layout or "").strip() or str(data.property_type or "").strip()


def _gallery_labels(canvas: Image.Image, labels: list[str], count: int, *, gallery_top: int) -> None:
    if count <= 0:
        return
    layer = Image.new("RGBA", canvas.size, (0, 0, 0, 0))
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
            (x + 16, gallery_top + 15, x + 40 + text_w, gallery_top + 57),
            radius=18,
            fill=(28, 24, 20, 165),
            outline=(255, 255, 255, 190),
            width=2,
        )
        draw.text((x + 28, gallery_top + 22), label, font=font, fill=(255, 255, 255, 245))
        x += width + GUTTER
    canvas.paste(layer, (0, 0), layer)


def _overlay(canvas: Image.Image, data: CoverRenderData, *, hero_height: int, footer_top: int) -> None:
    layer = Image.new("RGBA", canvas.size, (0, 0, 0, 0))
    draw = ImageDraw.Draw(layer)
    # Gentle top shade for the gold brand; facts live in the info bar below.
    for y in range(0, 210):
        alpha = int(92 * (1 - y / 210))
        draw.rectangle((0, y, 1080, y + 1), fill=(20, 17, 13, alpha))

    _place_brand(layer, x=42, y=38, width=300 if _is_apartment(data) else 360)

    # Solid dark info bar directly under the hero (same height for all types).
    info_top = hero_height
    info_bottom = hero_height + INFO_HEIGHT
    draw.rectangle((0, info_top, 1080, info_bottom), fill=(*INFO_BG, 255))
    draw.rectangle((0, info_top, 1080, info_top + 1), fill=(*GOLD, 200))

    price_left = _draw_dark_price_pill(
        draw, data, right=1080 - 36, top=info_top, bottom=info_bottom, max_width=340
    )
    text_right = (price_left if price_left is not None else 1080 - 36) - 28
    title = str(data.project or data.project_alias or data.area or data.property_type or "精选房源").strip()
    title_font = _fit_text(draw, title, max_width=text_right - 42, start_size=46, min_size=28)
    subtitle = _cover_subtitle(data)
    sub_font = _fit_text(draw, subtitle, max_width=text_right - 44, start_size=28, min_size=20) if subtitle else None
    # Vertically centre the title/subtitle block inside the bar using ink boxes.
    title_box = draw.textbbox((0, 0), title, font=title_font, anchor="ls")
    title_h = title_box[3] - title_box[1]
    line_gap = 16
    if subtitle and sub_font is not None:
        sub_box = draw.textbbox((0, 0), subtitle, font=sub_font, anchor="ls")
        sub_h = sub_box[3] - sub_box[1]
        block_h = title_h + line_gap + sub_h
    else:
        sub_box, sub_h, block_h = None, 0, title_h
    block_top = info_top + (INFO_HEIGHT - block_h) // 2
    title_baseline = block_top - title_box[1]
    draw.text((42, title_baseline), title, font=title_font, fill=(255, 255, 255), anchor="ls")
    # The approved hierarchy is one project title plus one concise fact line.
    # Never repeat values such as "双拼别墅 · 双拼别墅" on the public cover.
    if sub_box is not None:
        hair_y = block_top + title_h + line_gap // 2
        title_w = draw.textbbox((42, 0), title, font=title_font)[2]
        draw.rectangle((42, hair_y, min(text_right, title_w + 8), hair_y + 1), fill=(*GOLD, 220))
        sub_baseline = block_top + title_h + line_gap - sub_box[1]
        draw.text((44, sub_baseline), subtitle, font=sub_font, fill=(235, 230, 220), anchor="ls")

    # White footer matching the reference's clean real-estate card finish.
    footer_height = canvas.height - footer_top
    draw.rectangle((0, footer_top, 1080, canvas.height), fill=(255, 255, 255, 255))
    brand_y = footer_top + max(9, (footer_height - 52) // 2)
    draw.text((38, brand_y), "侨联地产", font=_font(28, bold=True), fill=INK)
    draw.text((181, brand_y + 17), "QIAO LIAN PROPERTY", font=_font(12), fill=(155, 126, 72))
    footer_label = "出租房源" if str(data.deal_type or "rent").lower() == "rent" else "出售房源"
    footer_font = _font(23, bold=True)
    footer_w = draw.textbbox((0, 0), footer_label, font=footer_font)[2]
    badge_w = footer_w + 42
    badge_h = 44
    badge_left = 1042 - badge_w
    badge_top = footer_top + max(8, (footer_height - badge_h) // 2)
    draw.rounded_rectangle(
        (badge_left, badge_top, 1042, badge_top + badge_h),
        radius=22,
        fill=(237, 214, 159),
    )
    draw.text(
        (badge_left + (badge_w - footer_w) // 2, badge_top + 5),
        footer_label,
        font=footer_font,
        fill=INK,
    )
    canvas.paste(layer, (0, 0), layer)


def _is_apartment(data: CoverRenderData) -> bool:
    value = str(data.property_type or "").strip().lower()
    return any(token in value for token in ("公寓", "服务式", "apartment", "condo", "studio"))


def _layout_geometry(data: CoverRenderData) -> tuple[tuple[int, int], int, int, int, int]:
    """Return canvas, hero height, gallery top/height and footer top."""
    if _is_apartment(data):
        canvas = APARTMENT_CANVAS
        hero_height = APARTMENT_HERO_HEIGHT
    else:
        canvas = TALL_CANVAS
        hero_height = TALL_HERO_HEIGHT
    gallery_height = GALLERY_HEIGHT
    gallery_top = hero_height + INFO_HEIGHT
    footer_top = gallery_top + gallery_height
    assert canvas[1] - footer_top == FOOTER_HEIGHT
    return canvas, hero_height, gallery_top, gallery_height, footer_top


def _draw_dark_price_pill(
    draw: ImageDraw.ImageDraw,
    data: CoverRenderData,
    *,
    right: int,
    top: int,
    bottom: int,
    max_width: int = 340,
) -> int | None:
    """Draw the gold-bordered price card vertically centred in ``top..bottom``.

    The ``$amount`` + ``/月`` row is centred on its *ink* box so it reads
    optically centred inside the card. Returns the card's left edge.
    """
    price = data.price_line()
    if not price:
        return None
    suffix = "/月" if price.endswith("/月") else ""
    amount = price[:-2] if suffix else price
    pad_x = 28
    gap = 6
    suffix_font = _font(24, bold=True)
    suffix_w = draw.textlength(suffix, font=suffix_font) if suffix else 0
    amount_font = _fit_text(
        draw, amount, max_width=int(max_width - 2 * pad_x - suffix_w - gap), start_size=48, min_size=28
    )
    amount_box = draw.textbbox((0, 0), amount, font=amount_font, anchor="ls")
    suffix_box = draw.textbbox((0, 0), suffix, font=suffix_font, anchor="ls") if suffix else (0, 0, 0, 0)
    amount_w = amount_box[2] - amount_box[0]
    row_w = amount_w + (gap + suffix_box[2] - suffix_box[0] if suffix else 0)
    ink_top = min(amount_box[1], suffix_box[1])
    ink_bottom = max(amount_box[3], suffix_box[3])
    ink_h = ink_bottom - ink_top
    width = min(max_width, max(200, row_w + 2 * pad_x))
    height = 84
    left = right - width
    card_top = top + (bottom - top - height) // 2
    draw.rounded_rectangle(
        (left, card_top, right, card_top + height),
        radius=16,
        fill=(17, 15, 13, 245),
        outline=GOLD,
        width=3,
    )
    baseline = card_top + (height - ink_h) // 2 - ink_top
    x = left + (width - row_w) // 2 - amount_box[0]
    draw.text((x, baseline), amount, font=amount_font, fill=GOLD_LIGHT, anchor="ls")
    if suffix:
        draw.text((x + amount_box[2] + gap - suffix_box[0], baseline), suffix, font=suffix_font, fill=(255, 255, 255), anchor="ls")
    return left


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
    canvas_size, hero_height, gallery_top, gallery_height, footer_top = _layout_geometry(data)
    canvas = Image.new("RGB", canvas_size, (244, 240, 233))
    _paste_gallery(
        canvas,
        images,
        hero_height=hero_height,
        gallery_top=gallery_top,
        gallery_height=gallery_height,
        layout=layout,
    )
    _overlay(canvas, data, hero_height=hero_height, footer_top=footer_top)
    _gallery_labels(
        canvas,
        list(source_labels or ()),
        min(3, max(0, len(images) - 1)),
        gallery_top=gallery_top,
    )
    output = Path(output_path).expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.suffix.lower() in {".jpg", ".jpeg"}:
        canvas.save(output, format="JPEG", quality=92, optimize=True)
    else:
        canvas.save(output, format="PNG", optimize=True)
    return str(output)

__all__ = ["CoverRenderData", "render_cover"]
