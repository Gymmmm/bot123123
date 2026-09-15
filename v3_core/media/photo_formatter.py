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
    "landscape": {"size": (1200, 900), "logo_width_ratio": 0.18},
    "portrait": {"size": (900, 1200), "logo_width_ratio": 0.18},
    "square": {"size": (1080, 1080), "logo_width_ratio": 0.18},
}

LOGO_MARGIN_X_RATIO = 0.03
LOGO_MARGIN_Y_RATIO = 0.03
LOGO_OPACITY = 0.90
LOGO_MAX_PHOTO_WIDTH_RATIO = 0.32
LOGO_MAX_PHOTO_HEIGHT_RATIO = 0.15
IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp"}

ROOT = Path(__file__).resolve().parent
DEFAULT_LOGO_CANDIDATES = (
    ROOT / "assets" / "qiaolian_logo_white.png",
    ROOT / "assets" / "brand" / "qiaolian_corner_mark_120x40.png",
)
NOTO_FONT_CANDIDATES = (
    "/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc",
    "/usr/share/fonts/truetype/noto/NotoSansCJK-Bold.ttc",
    "/System/Library/Fonts/PingFang.ttc",
)


def detect_orientation(width: int, height: int) -> str:
    ratio = width / max(height, 1)
    if ratio >= LANDSCAPE_THRESHOLD:
        return "landscape"
    if ratio <= PORTRAIT_THRESHOLD:
        return "portrait"
    return "square"


def enhance_property_photo(image: Image.Image) -> Image.Image:
    """Light factual enhancement only; never alter geometry or scene contents."""
    image = ImageOps.autocontrast(image, cutoff=0.5)
    image = ImageEnhance.Brightness(image).enhance(1.035)
    image = ImageEnhance.Contrast(image).enhance(1.045)
    image = ImageEnhance.Color(image).enhance(1.035)
    return image.filter(ImageFilter.UnsharpMask(radius=1.2, percent=65, threshold=4))


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
                return ImageFont.truetype(raw, size)
            except Exception:
                continue
    return ImageFont.load_default()


def _fallback_brand_logo(canvas_width: int) -> Image.Image:
    font = _load_font(max(24, int(canvas_width * 0.038)))
    text = "侨联地产"
    tmp = Image.new("RGBA", (canvas_width, max(68, int(canvas_width * 0.10))), (0, 0, 0, 0))
    draw = ImageDraw.Draw(tmp)
    box = draw.textbbox((0, 0), text, font=font, stroke_width=1)
    tw = max(1, box[2] - box[0])
    th = max(1, box[3] - box[1])
    out = Image.new("RGBA", (tw + 24, th + 18), (0, 0, 0, 0))
    d = ImageDraw.Draw(out)
    d.text((13, 10), text, font=font, fill=(0, 0, 0, 105), stroke_width=1, stroke_fill=(0, 0, 0, 75))
    d.text((11, 8), text, font=font, fill=(255, 255, 255, 240), stroke_width=1, stroke_fill=(0, 0, 0, 65))
    return out


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
        logo = resolve_logo(logo_path, canvas_size[0])
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
