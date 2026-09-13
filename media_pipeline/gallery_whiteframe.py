#!/usr/bin/env python3
"""Qiaolian detail-photo renderer: light correction + white frame + small brand lockup."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from PIL import Image, ImageEnhance, ImageOps

ROOT = Path(__file__).resolve().parent
DEFAULT_LOGO = ROOT / "assets" / "qiaolian_logo_black_gold_lockup.png"
CANVAS = (1600, 1200)
OUTER_MARGIN = 28
INNER_PAD = 18
JPEG_QUALITY = 92

FONT_CANDIDATES = [
    Path("/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc"),
    Path("/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc"),
]


def _font_path() -> str | None:
    for p in FONT_CANDIDATES:
        if p.exists():
            return str(p)
    return None


def light_correct(im: Image.Image) -> Image.Image:
    """Conservative listing correction; preserves the physical scene."""
    im = ImageOps.autocontrast(im, cutoff=0.4)
    im = ImageEnhance.Brightness(im).enhance(1.015)
    im = ImageEnhance.Contrast(im).enhance(1.025)
    im = ImageEnhance.Color(im).enhance(1.025)
    im = ImageEnhance.Sharpness(im).enhance(1.08)
    return im


def contain(im: Image.Image, box: tuple[int, int]) -> Image.Image:
    max_w, max_h = box
    scale = min(max_w / im.width, max_h / im.height)
    w = max(1, round(im.width * scale))
    h = max(1, round(im.height * scale))
    return im.resize((w, h), Image.Resampling.LANCZOS)


def render_detail(input_path: str | Path, output_path: str | Path, *, logo_path: str | Path = DEFAULT_LOGO, canvas_size: tuple[int, int] = CANVAS) -> dict[str, Any]:
    input_path, output_path, logo_path = Path(input_path), Path(output_path), Path(logo_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    cw, ch = canvas_size
    with Image.open(input_path) as src:
        src = ImageOps.exif_transpose(src).convert("RGB")
        src = light_correct(src)
        fitted = contain(src, (cw - OUTER_MARGIN * 2, ch - OUTER_MARGIN * 2))
        canvas = Image.new("RGB", (cw, ch), "white")
        x = (cw - fitted.width) // 2
        y = (ch - fitted.height) // 2
        canvas.paste(fitted, (x, y))
        logo_meta: dict[str, Any] = {"logo": None}
        if logo_path.exists():
            with Image.open(logo_path) as lim:
                logo = lim.convert("RGBA")
                target_w = max(150, min(round(fitted.width * 0.13), 230))
                ratio = target_w / logo.width
                logo = logo.resize((target_w, max(1, round(logo.height * ratio))), Image.Resampling.LANCZOS)
                alpha = logo.getchannel("A").point(lambda a: int(a * 0.86))
                logo.putalpha(alpha)
                lx = x + fitted.width - logo.width - INNER_PAD
                ly = y + fitted.height - logo.height - INNER_PAD
                rgba = canvas.convert("RGBA")
                rgba.alpha_composite(logo, (lx, ly))
                canvas = rgba.convert("RGB")
                logo_meta["logo"] = {"path": str(logo_path), "xy": [lx, ly], "size": list(logo.size)}
        canvas.save(output_path, "JPEG", quality=JPEG_QUALITY, optimize=True, progressive=True)
    return {
        "input": str(input_path),
        "output": str(output_path),
        "canvas": list(canvas_size),
        "image_box": [x, y, fitted.width, fitted.height],
        "white_frame_px": min(x, y),
        "correction": {"brightness": 1.015, "contrast": 1.025, "saturation": 1.025, "sharpness": 1.08},
        "font_family": "Noto Sans SC / Noto Sans CJK",
        "font_path": _font_path(),
        **logo_meta,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("inputs", nargs="+", type=Path)
    ap.add_argument("-o", "--output-dir", required=True, type=Path)
    ap.add_argument("--logo", type=Path, default=DEFAULT_LOGO)
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    results = []
    for idx, src in enumerate(args.inputs, 1):
        dst = args.output_dir / f"{idx:02d}_{src.stem}_detail.jpg"
        results.append(render_detail(src, dst, logo_path=args.logo))
    report = args.output_dir / "gallery_whiteframe_report.json"
    report.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
    print(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
