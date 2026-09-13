#!/usr/bin/env python3
"""Gallery photo formatter with 侨联 带线黑金 logo on every shot.

Uses contain/letterbox from photo_formatter_v1_1, then overlays the locked
gold line lockup top-left (~22–28% width, ~3.5% pad). On bright corners a
translucent dark plate is added so the gold strokes stay readable.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import numpy as np
from PIL import Image, ImageDraw, ImageOps

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from photo_formatter_v1_1 import (  # noqa: E402
    CANVAS_PRESETS,
    JPEG_QUALITY,
    LOGO_MARGIN_X_RATIO,
    LOGO_MARGIN_Y_RATIO,
    LOGO_OPACITY,
    apply_logo_opacity,
    contain_image,
    detect_orientation,
    paste_logo,
    resize_logo,
)

IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp"}
# Locked brand: 带线黑金锁标（用户确认）
DEFAULT_GOLD_LOCKUP = ROOT / "assets" / "qiaolian_logo_topline_gold.png"  # 带顶线+斜划（用户指定）
DEFAULT_GOLD_PLATE = ROOT / "assets" / "qiaolian_logo_black_gold_user.png"
# Back-compat aliases (both point at gold lockup; plate added when bright)
DEFAULT_WHITE_LOGO = DEFAULT_GOLD_LOCKUP
DEFAULT_DARK_LOGO = DEFAULT_GOLD_LOCKUP

# Brightness thresholds (0–255 mean of top-left sample)
BRIGHT_MEAN = 140.0
MID_LOW = 90.0
MID_HIGH = 140.0

# Logo sizing: ~22–28% of canvas width by orientation
LOGO_WIDTH_BY_ORIENT = {
    "landscape": 0.24,
    "portrait": 0.28,
    "square": 0.25,
}
PAD_RATIO = 0.035  # ~3.5%


def top_left_brightness(canvas: Image.Image, sample_ratio: float = 0.18) -> float:
    """Mean luminance of a top-left patch used for logo variant selection."""
    rgb = canvas.convert("RGB")
    w, h = rgb.size
    sw = max(8, int(w * sample_ratio))
    sh = max(8, int(h * sample_ratio * 0.7))
    patch = np.asarray(rgb.crop((0, 0, sw, sh)), dtype=np.float32)
    # Rec.601 luminance
    lum = 0.299 * patch[:, :, 0] + 0.587 * patch[:, :, 1] + 0.114 * patch[:, :, 2]
    return float(lum.mean())


def pick_logo_variant(
    brightness: float,
    white_logo: Path,
    dark_logo: Path,
) -> tuple[str, Path, bool]:
    """
    Returns (variant_name, logo_path, needs_dark_plate).

    Always uses 带线黑金锁标 (gold lockup). On mid/bright corners add a
    translucent dark plate so gold strokes stay readable.
    white_logo / dark_logo args kept for CLI back-compat; both should be
    the gold lockup path.
    """
    logo = white_logo if white_logo.exists() else dark_logo
    if brightness < MID_LOW:
        return "gold", logo, False
    return "gold_on_plate", logo, True


def make_dark_plate(
    logo_w: int,
    logo_h: int,
    pad_x: int,
    pad_y: int,
    radius: int | None = None,
    fill=(8, 8, 10, 200),
) -> Image.Image:
    """Translucent dark rounded rectangle slightly larger than the logo."""
    if radius is None:
        radius = max(6, min(logo_h // 3, 18))
    pw = logo_w + pad_x * 2
    ph = logo_h + pad_y * 2
    plate = Image.new("RGBA", (pw, ph), (0, 0, 0, 0))
    draw = ImageDraw.Draw(plate)
    draw.rounded_rectangle((0, 0, pw - 1, ph - 1), radius=radius, fill=fill)
    return plate


def overlay_logo_auto(
    canvas: Image.Image,
    *,
    white_logo: Path,
    dark_logo: Path,
    orientation: str,
    image_box: tuple[int, int, int, int] | None = None,
    margin_x_ratio: float = PAD_RATIO,
    margin_y_ratio: float = PAD_RATIO,
    logo_opacity: float = LOGO_OPACITY,
) -> dict[str, Any]:
    """Pick logo by corner brightness and composite top-left of photo content."""
    if image_box is not None:
        ix, iy, iw, ih = image_box
        sample = canvas.crop((ix, iy, ix + max(8, int(iw * 0.22)), iy + max(8, int(ih * 0.14))))
        brightness = top_left_brightness(sample, sample_ratio=1.0)
        # Size logo vs photo content width (not letterbox canvas)
        ref_width = iw
        base_x, base_y = ix, iy
        margin_x = int(iw * margin_x_ratio)
        margin_y = int(ih * margin_y_ratio)
    else:
        brightness = top_left_brightness(canvas)
        ref_width = canvas.width
        base_x, base_y = 0, 0
        margin_x = int(canvas.width * margin_x_ratio)
        margin_y = int(canvas.height * margin_y_ratio)

    variant, logo_path, needs_plate = pick_logo_variant(
        brightness, white_logo, dark_logo
    )
    meta: dict[str, Any] = {
        "brightness_mean": round(brightness, 2),
        "logo_variant": variant,
        "logo_path": str(logo_path),
        "needs_plate": needs_plate,
    }
    if not logo_path.exists():
        meta["error"] = f"logo_missing:{logo_path}"
        return {"canvas": canvas, "meta": meta}

    width_ratio = LOGO_WIDTH_BY_ORIENT.get(orientation, 0.24)
    with Image.open(logo_path) as logo_im:
        logo = logo_im.convert("RGBA")
        # resize_logo expects canvas_width; pass content width
        logo = resize_logo(logo, ref_width, width_ratio)
        logo = apply_logo_opacity(logo, opacity=logo_opacity)

    lx = base_x + margin_x
    ly = base_y + margin_y
    overlay = canvas.convert("RGBA")

    if needs_plate:
        plate_pad_x = max(8, logo.width // 12)
        plate_pad_y = max(6, logo.height // 5)
        plate = make_dark_plate(logo.width, logo.height, plate_pad_x, plate_pad_y)
        px = max(0, lx - plate_pad_x)
        py = max(0, ly - plate_pad_y)
        overlay.alpha_composite(plate, (px, py))
        meta["plate"] = {
            "xy": [px, py],
            "size": [plate.width, plate.height],
        }

    overlay.alpha_composite(logo, (lx, ly))
    meta["logo_xy"] = [lx, ly]
    meta["logo_size"] = [logo.width, logo.height]
    meta["logo_width_ratio"] = width_ratio
    return {"canvas": overlay.convert("RGB"), "meta": meta}


def contain_with_box(src: Image.Image, canvas_size, padding=12, bg_color=(255, 255, 255)):
    """Like photo_formatter_v1_1.contain_image but also returns image box (x,y,w,h)."""
    canvas_w, canvas_h = canvas_size
    max_w = canvas_w - padding * 2
    max_h = canvas_h - padding * 2
    scale = min(max_w / src.width, max_h / src.height)
    new_w = max(1, int(src.width * scale))
    new_h = max(1, int(src.height * scale))
    resized = src.resize((new_w, new_h), Image.Resampling.LANCZOS)
    canvas = Image.new("RGB", canvas_size, bg_color)
    x = (canvas_w - new_w) // 2
    y = (canvas_h - new_h) // 2
    canvas.paste(resized, (x, y))
    return canvas, (x, y, new_w, new_h)


def format_gallery_with_logo(
    input_path: str | Path,
    output_path: str | Path,
    *,
    white_logo: str | Path = DEFAULT_WHITE_LOGO,
    dark_logo: str | Path = DEFAULT_DARK_LOGO,
    quality: int = JPEG_QUALITY,
) -> dict[str, Any]:
    """Letterbox (contain) then auto-overlay brand logo. Returns metadata."""
    input_path = Path(input_path)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    white_logo = Path(white_logo)
    dark_logo = Path(dark_logo)

    with Image.open(input_path) as src:
        src = ImageOps.exif_transpose(src).convert("RGB")
        orientation = detect_orientation(src.width, src.height)
        preset = CANVAS_PRESETS[orientation]
        canvas_size = preset["size"]
        canvas, image_box = contain_with_box(src, canvas_size)
        result = overlay_logo_auto(
            canvas,
            white_logo=white_logo,
            dark_logo=dark_logo,
            orientation=orientation,
            image_box=image_box,
        )
        canvas = result["canvas"]
        canvas.save(
            output_path,
            "JPEG",
            quality=quality,
            optimize=True,
            progressive=True,
        )

    return {
        "input": str(input_path),
        "output": str(output_path),
        "orientation": orientation,
        "canvas": {"width": canvas_size[0], "height": canvas_size[1]},
        "image_box": list(image_box),
        **result["meta"],
    }


def collect_inputs(paths: list[Path]) -> list[Path]:
    files: list[Path] = []
    for p in paths:
        p = p.expanduser().resolve()
        if p.is_dir():
            files.extend(
                sorted(
                    x
                    for x in p.iterdir()
                    if x.is_file() and x.suffix.lower() in IMAGE_EXTS
                )
            )
        elif p.is_file() and p.suffix.lower() in IMAGE_EXTS:
            files.append(p)
    # de-dupe preserving order
    seen = set()
    out = []
    for f in files:
        key = str(f)
        if key not in seen:
            seen.add(key)
            out.append(f)
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Format gallery photos with auto dark/white 侨联 logo"
    )
    ap.add_argument(
        "inputs",
        nargs="+",
        type=Path,
        help="Input image path(s) and/or directories",
    )
    ap.add_argument(
        "-o",
        "--output-dir",
        type=Path,
        required=True,
        help="Output directory",
    )
    ap.add_argument(
        "--white-logo",
        type=Path,
        default=DEFAULT_WHITE_LOGO,
        help="White logo path (for dark / mid plates)",
    )
    ap.add_argument(
        "--dark-logo",
        type=Path,
        default=DEFAULT_DARK_LOGO,
        help="Dark logo path (for bright backgrounds)",
    )
    ap.add_argument(
        "--report",
        type=Path,
        default=None,
        help="Optional JSON report path",
    )
    args = ap.parse_args(argv)

    files = collect_inputs(args.inputs)
    if not files:
        print("No input images found.", file=sys.stderr)
        return 1

    args.output_dir.mkdir(parents=True, exist_ok=True)
    results = []
    for idx, src in enumerate(files, start=1):
        dst = args.output_dir / f"{idx:02d}_{src.stem}_logo.jpg"
        info = format_gallery_with_logo(
            src,
            dst,
            white_logo=args.white_logo,
            dark_logo=args.dark_logo,
        )
        results.append(info)
        print(
            f"{src.name} → {dst.name}  "
            f"variant={info.get('logo_variant')}  "
            f"brightness={info.get('brightness_mean')}"
        )

    report_path = args.report or (args.output_dir / "gallery_logo_report.json")
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"report: {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
