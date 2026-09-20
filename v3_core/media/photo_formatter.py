from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Sequence

from PIL import Image, ImageDraw, ImageEnhance, ImageFilter, ImageFont, ImageOps

JPEG_QUALITY = 94
BG_COLOR = (255, 255, 255)
PADDING = 24
LANDSCAPE_THRESHOLD = 1.10
PORTRAIT_THRESHOLD = 0.90

CANVAS_PRESETS = {
    "landscape": {"size": (1200, 900), "logo_width_ratio": 0.22},
    "portrait": {"size": (900, 1200), "logo_width_ratio": 0.22},
    "square": {"size": (1080, 1080), "logo_width_ratio": 0.22},
}

LOGO_MARGIN_X_RATIO = 0.03
LOGO_MARGIN_Y_RATIO = 0.03
LOGO_OPACITY = 0.94
LOGO_MAX_PHOTO_WIDTH_RATIO = 0.32
LOGO_MAX_PHOTO_HEIGHT_RATIO = 0.22
IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp"}

ROOT = Path(__file__).resolve().parent
CLASSIC_BLUE_LOGO = ROOT / "assets" / "qiaolian_corner_classic_blue.png"
RIGHT_PRICE_LOGO = ROOT / "assets" / "qiaolian_corner_right_price.png"
BLACK_GOLD_LOGO = ROOT / "assets" / "qiaolian_corner_black_gold.png"
DEFAULT_LOGO_CANDIDATES = (
    RIGHT_PRICE_LOGO,
    CLASSIC_BLUE_LOGO,
    ROOT / "assets" / "qiaolian_logo_white.png",
    ROOT / "assets" / "brand" / "qiaolian_corner_mark_120x40.png",
)
_BLACK_GOLD_STYLE_KEYS = frozenset({"black_gold", "villa_premium", "dark_glass"})
_CLASSIC_BLUE_STYLE_KEYS = frozenset(
    {"classic_blue", "classic", "left_info", "minimal", "minimal_white", "blue_banner", "premium_4image"}
)
NOTO_FONT_CANDIDATES = (
    "/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc",
    "/usr/share/fonts/truetype/noto/NotoSansCJK-Bold.ttc",
    "/System/Library/Fonts/PingFang.ttc",
)
NOTO_CJK_SC_INDEX = 2


def detect_orientation(width: int, height: int) -> str:
    ratio = width / max(height, 1)
    if ratio >= LANDSCAPE_THRESHOLD:
        return "landscape"
    if ratio <= PORTRAIT_THRESHOLD:
        return "portrait"
    return "square"


def enhance_property_photo(image: Image.Image) -> Image.Image:
    """Mild production enhance only — subtle lift, no heavy sharpen/saturate.

    Geometry and scene contents stay untouched. Safe default for gallery / scrub
    derivatives; skip when callers pass enhance=False.
    """
    image = ImageOps.autocontrast(image, cutoff=0.4)
    image = ImageEnhance.Brightness(image).enhance(1.025)
    image = ImageEnhance.Contrast(image).enhance(1.03)
    image = ImageEnhance.Color(image).enhance(1.02)
    return image.filter(ImageFilter.UnsharpMask(radius=1.0, percent=40, threshold=5))


def apply_logo_opacity(logo: Image.Image, opacity: float = LOGO_OPACITY) -> Image.Image:
    logo = logo.convert("RGBA")
    alpha = logo.getchannel("A")
    alpha = alpha.point(lambda p: int(p * max(0.0, min(1.0, opacity))))
    logo.putalpha(alpha)
    return logo


def contain_image(
    src: Image.Image,
    canvas_size: tuple[int, int],
    padding: int = PADDING,
    bg_color: tuple[int, int, int] = BG_COLOR,
) -> tuple[Image.Image, tuple[int, int, int, int]]:
    """Contain the complete source without crop/stretch and return its real image box."""
    canvas_w, canvas_h = canvas_size
    max_w = max(1, canvas_w - padding * 2)
    max_h = max(1, canvas_h - padding * 2)
    scale = min(max_w / max(1, src.width), max_h / max(1, src.height))
    new_w = max(1, int(round(src.width * scale)))
    new_h = max(1, int(round(src.height * scale)))
    resized = src.resize((new_w, new_h), Image.Resampling.LANCZOS)
    canvas = Image.new("RGB", canvas_size, bg_color)
    x = (canvas_w - new_w) // 2
    y = (canvas_h - new_h) // 2
    canvas.paste(resized, (x, y))
    return canvas, (x, y, new_w, new_h)


def _load_font(size: int) -> ImageFont.ImageFont:
    for raw in NOTO_FONT_CANDIDATES:
        if os.path.isfile(raw):
            try:
                return ImageFont.truetype(raw, size, index=NOTO_CJK_SC_INDEX)
            except Exception:
                try:
                    return ImageFont.truetype(raw, size)
                except Exception:
                    continue
    return ImageFont.load_default()


def _fallback_brand_logo(canvas_width: int) -> Image.Image:
    """Two-line cover-matching mark when PNG assets are missing."""
    cn_font = _load_font(max(28, int(canvas_width * 0.045)))
    en_font = _load_font(max(14, int(canvas_width * 0.014)))
    cn, en = "侨联地产", "QIAO LIAN"
    probe = Image.new("RGBA", (8, 8), (0, 0, 0, 0))
    probe_draw = ImageDraw.Draw(probe)
    cn_box = probe_draw.textbbox((0, 0), cn, font=cn_font)
    en_box = probe_draw.textbbox((0, 0), en, font=en_font)
    cn_w, cn_h = cn_box[2] - cn_box[0], cn_box[3] - cn_box[1]
    en_w, en_h = en_box[2] - en_box[0], en_box[3] - en_box[1]
    gap = max(6, int(canvas_width * 0.006))
    pad_x, pad_y = 14, 10
    width = max(cn_w, en_w) + pad_x * 2
    height = cn_h + gap + en_h + pad_y * 2
    out = Image.new("RGBA", (width, height), (0, 0, 0, 0))
    d = ImageDraw.Draw(out)
    cn_x = pad_x + (max(cn_w, en_w) - cn_w) // 2
    en_x = pad_x + (max(cn_w, en_w) - en_w) // 2
    cn_y = pad_y - cn_box[1]
    en_y = pad_y + cn_h + gap - en_box[1]
    for dx, dy, a in ((2, 2, 100), (1, 1, 120)):
        d.text((cn_x + dx, cn_y + dy), cn, font=cn_font, fill=(0, 0, 0, a))
        d.text((en_x + dx, en_y + dy), en, font=en_font, fill=(0, 0, 0, a))
    d.text((cn_x, cn_y), cn, font=cn_font, fill=(255, 255, 255, 245))
    d.text((en_x, en_y), en, font=en_font, fill=(255, 255, 255, 235))
    return out


def resolve_gallery_logo_path(cover_style: str | None) -> Path | None:
    """Map cover style to the matching gallery corner mark asset.

    classic_blue → white/brand-matched text mark (no blue plate)
    right_price  → white text mark
    black_gold   → champagne gold mark
    """
    key = str(cover_style or "").strip().lower()
    if key in _BLACK_GOLD_STYLE_KEYS:
        path = BLACK_GOLD_LOGO
    elif key in _CLASSIC_BLUE_STYLE_KEYS:
        path = CLASSIC_BLUE_LOGO
    else:
        path = RIGHT_PRICE_LOGO
    if path.is_file():
        return path
    # Fallbacks keep gallery branding available if one asset is missing.
    for candidate in (RIGHT_PRICE_LOGO, CLASSIC_BLUE_LOGO, BLACK_GOLD_LOGO):
        if candidate.is_file():
            return candidate
    return None


def resolve_logo(logo_path: str | Path | None, canvas_width: int) -> Image.Image:
    candidates: list[Path] = []
    env_path = str(os.getenv("QIAOLIAN_GALLERY_LOGO", "") or "").strip()
    if logo_path:
        candidates.append(Path(logo_path).expanduser())
    if env_path:
        candidates.append(Path(env_path).expanduser())
    candidates.extend(DEFAULT_LOGO_CANDIDATES)
    for path in candidates:
        if path.is_file():
            try:
                with Image.open(path) as raw:
                    return raw.convert("RGBA")
            except Exception:
                continue
    return _fallback_brand_logo(canvas_width)


def _resize_gallery_logo(
    logo: Image.Image,
    *,
    canvas_width: int,
    image_box: tuple[int, int, int, int],
    width_ratio: float,
) -> Image.Image:
    image_w = max(1, image_box[2])
    image_h = max(1, image_box[3])
    target_w = min(
        max(1, int(canvas_width * width_ratio)),
        max(1, int(image_w * LOGO_MAX_PHOTO_WIDTH_RATIO)),
    )
    target_h_cap = max(1, int(image_h * LOGO_MAX_PHOTO_HEIGHT_RATIO))
    scale = min(
        target_w / max(1, logo.width),
        target_h_cap / max(1, logo.height),
    )
    return logo.resize(
        (max(1, int(round(logo.width * scale))), max(1, int(round(logo.height * scale)))),
        Image.Resampling.LANCZOS,
    )


def _logo_anchor(
    image_box: tuple[int, int, int, int],
    logo_size: tuple[int, int],
    position: str,
) -> tuple[int, int]:
    image_x, image_y, image_w, image_h = image_box
    logo_w, logo_h = logo_size
    margin_x = max(8, int(image_w * LOGO_MARGIN_X_RATIO))
    margin_y = max(8, int(image_h * LOGO_MARGIN_Y_RATIO))
    if position == "top_right":
        return image_x + image_w - logo_w - margin_x, image_y + margin_y
    if position == "bottom_left":
        return image_x + margin_x, image_y + image_h - logo_h - margin_y
    if position == "bottom_right":
        return image_x + image_w - logo_w - margin_x, image_y + image_h - logo_h - margin_y
    return image_x + margin_x, image_y + margin_y


def paste_logo(
    canvas: Image.Image,
    logo: Image.Image,
    image_box: tuple[int, int, int, int],
    position: str = "top_left",
) -> tuple[Image.Image, tuple[int, int, int, int]]:
    x, y = _logo_anchor(image_box, logo.size, position)
    image_x, image_y, image_w, image_h = image_box
    x = max(image_x, min(x, image_x + image_w - logo.width))
    y = max(image_y, min(y, image_y + image_h - logo.height))
    overlay = canvas.convert("RGBA")
    overlay.alpha_composite(logo, (x, y))
    return overlay.convert("RGB"), (x, y, logo.width, logo.height)


def format_gallery_photo(
    input_path: str | Path,
    output_path: str | Path,
    logo_path: str | Path | None = None,
    logo_position: str = "top_left",
    quality: int = JPEG_QUALITY,
    add_logo: bool = True,
    enhance: bool = True,
    cover_style: str | None = None,
) -> dict:
    input_path = Path(input_path)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with Image.open(input_path) as source:
        src = ImageOps.exif_transpose(source).convert("RGB")
        if enhance:
            src = enhance_property_photo(src)
        orientation = detect_orientation(src.width, src.height)
        preset = CANVAS_PRESETS[orientation]
        canvas_size = preset["size"]
        canvas, image_box = contain_image(src, canvas_size)

    logo_box = None
    if add_logo:
        effective_logo = logo_path
        if effective_logo is None:
            effective_logo = resolve_gallery_logo_path(cover_style)
        logo = resolve_logo(effective_logo, canvas_size[0])
        logo = _resize_gallery_logo(
            logo,
            canvas_width=canvas_size[0],
            image_box=image_box,
            width_ratio=preset["logo_width_ratio"],
        )
        logo = apply_logo_opacity(logo)
        canvas, logo_box = paste_logo(canvas, logo, image_box, position=logo_position)

    canvas.save(
        output_path,
        "JPEG",
        quality=quality,
        optimize=True,
        progressive=True,
        subsampling=0,
    )
    return {
        "input": str(input_path),
        "output": str(output_path),
        "orientation": orientation,
        "canvas": {"width": canvas_size[0], "height": canvas_size[1]},
        "image_box": {"x": image_box[0], "y": image_box[1], "width": image_box[2], "height": image_box[3]},
        "logo_box": (
            {"x": logo_box[0], "y": logo_box[1], "width": logo_box[2], "height": logo_box[3]}
            if logo_box else None
        ),
        "logo_position": logo_position if add_logo else None,
        "enhanced": bool(enhance),
    }


def _natural_key(path: Path) -> tuple:
    return tuple(int(part) if part.isdigit() else part.casefold() for part in re.split(r"(\d+)", path.name))


def _manifest_items(value: object) -> list[str]:
    if isinstance(value, list):
        raw_items = value
    elif isinstance(value, dict):
        raw_items = value.get("raw_images_json") or value.get("images") or value.get("media") or []
    else:
        raw_items = []
    out: list[str] = []
    for item in raw_items:
        if isinstance(item, str):
            candidate = item
        elif isinstance(item, dict):
            candidate = str(item.get("local_path") or item.get("path") or item.get("file") or item.get("name") or "")
        else:
            candidate = ""
        if candidate:
            out.append(candidate)
    return out


def ordered_source_files(
    input_folder: str | Path,
    *,
    source_order: Sequence[str | Path | dict] | None = None,
    source_manifest: str | Path | None = None,
) -> list[Path]:
    """Prefer collector/source order. Natural filename sorting is fallback only."""
    folder = Path(input_folder).resolve()
    available = [p for p in folder.iterdir() if p.is_file() and p.suffix.lower() in IMAGE_EXTS]
    by_name = {p.name: p for p in available}
    ordered: list[Path] = []

    manifest_order: list[object] = list(source_order or [])
    if not manifest_order and source_manifest:
        manifest_path = Path(source_manifest)
        if manifest_path.is_file():
            try:
                manifest_order = _manifest_items(json.loads(manifest_path.read_text(encoding="utf-8")))
            except Exception:
                manifest_order = []

    for item in manifest_order:
        if isinstance(item, dict):
            raw = str(item.get("local_path") or item.get("path") or item.get("file") or item.get("name") or "")
        else:
            raw = str(item or "")
        if not raw:
            continue
        candidate = Path(raw)
        if not candidate.is_absolute():
            candidate = folder / candidate
        candidate = candidate.resolve()
        if candidate.is_file() and candidate.suffix.lower() in IMAGE_EXTS and candidate not in ordered:
            ordered.append(candidate)
            continue
        fallback = by_name.get(Path(raw).name)
        if fallback and fallback not in ordered:
            ordered.append(fallback)

    for path in sorted(available, key=_natural_key):
        if path not in ordered:
            ordered.append(path)
    return ordered


def format_gallery_folder(
    input_folder: str | Path,
    output_folder: str | Path,
    logo_path: str | Path | None = None,
    logo_position: str = "top_left",
    *,
    source_order: Sequence[str | Path | dict] | None = None,
    source_manifest: str | Path | None = None,
    enhance: bool = True,
    cover_style: str | None = None,
) -> list[dict]:
    output_folder = Path(output_folder)
    output_folder.mkdir(parents=True, exist_ok=True)
    files = ordered_source_files(
        input_folder,
        source_order=source_order,
        source_manifest=source_manifest,
    )
    results: list[dict] = []
    for index, src in enumerate(files, start=1):
        dst = output_folder / f"{index:02d}.jpg"
        info = format_gallery_photo(
            src,
            dst,
            logo_path=logo_path,
            logo_position=logo_position,
            enhance=enhance,
            cover_style=cover_style,
        )
        info["order"] = index
        info["source_order"] = src.name
        results.append(info)
    return results


if __name__ == "__main__":
    import sys

    input_folder = sys.argv[1] if len(sys.argv) > 1 else "houses/QC0089"
    output_folder = sys.argv[2] if len(sys.argv) > 2 else "processed/QC0089/gallery"
    logo_path = sys.argv[3] if len(sys.argv) > 3 else None
    source_manifest = sys.argv[4] if len(sys.argv) > 4 else None
    result = format_gallery_folder(
        input_folder,
        output_folder,
        logo_path=logo_path,
        source_manifest=source_manifest,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
