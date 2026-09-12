#!/usr/bin/env python3
"""Gallery overlay: full-width gold top hairline + left 侨联 brand mark."""
from __future__ import annotations
import argparse, json, sys
from pathlib import Path
from typing import Any
import numpy as np
from PIL import Image, ImageDraw, ImageOps

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
from photo_formatter_v1_1 import CANVAS_PRESETS, JPEG_QUALITY, detect_orientation
from format_gallery_with_logo import contain_with_box, IMAGE_EXTS

DEFAULT_MARK = ROOT / "assets" / "qiaolian_logo_mark_gold.png"
GOLD_RGB = (212, 197, 160)


def draw_topline(canvas: Image.Image, y: int | None = None, thickness: float = 1.5) -> Image.Image:
    out_im = canvas.convert("RGBA")
    w, h = out_im.size
    if y is None:
        y = max(18, int(h * 0.028))
    layer = Image.new("RGBA", (w, h), (0, 0, 0, 0))
    d = ImageDraw.Draw(layer)
    th = max(1, int(thickness))
    d.rectangle([0, y, w, y + th], fill=(*GOLD_RGB, 235))
    cx = w // 2
    gap = max(22, int(w * 0.018))
    d.rectangle([cx - gap // 2, y - 2, cx + gap // 2, y + th + 2], fill=(0, 0, 0, 0))
    s = max(7, int(h * 0.012))
    cy = y + th / 2
    diamond = [(cx, cy - s), (cx + s, cy), (cx, cy + s), (cx - s, cy)]
    d.polygon(diamond, outline=(*GOLD_RGB, 255))
    d.line(
        [(cx - s * 0.9, cy + s * 0.55), (cx + s * 0.9, cy - s * 0.55)],
        fill=(255, 255, 255, 230),
        width=max(1, th),
    )
    left_x1 = cx - s - 1
    right_x0 = cx + s + 1
    if left_x1 >= cx - gap // 2:
        d.rectangle([cx - gap // 2, y, left_x1, y + th], fill=(*GOLD_RGB, 200))
    if cx + gap // 2 >= right_x0:
        d.rectangle([right_x0, y, cx + gap // 2, y + th], fill=(*GOLD_RGB, 200))
    return Image.alpha_composite(out_im, layer)


def format_gallery_topline(
    input_path: str | Path,
    output_path: str | Path,
    *,
    mark_path: str | Path = DEFAULT_MARK,
    logo_width_ratio: float = 0.30,
    quality: int = JPEG_QUALITY,
) -> dict[str, Any]:
    input_path, output_path = Path(input_path), Path(output_path)
    mark_path = Path(mark_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    mark = Image.open(mark_path).convert("RGBA")

    with Image.open(input_path) as src:
        src = ImageOps.exif_transpose(src).convert("RGB")
        orientation = detect_orientation(src.width, src.height)
        canvas_size = CANVAS_PRESETS[orientation]["size"]
        canvas, box = contain_with_box(src, canvas_size)
        canvas = draw_topline(canvas).convert("RGB")
        ix, iy, iw, ih = box
        target_w = int(iw * logo_width_ratio)
        L = mark.resize(
            (target_w, max(1, int(mark.height * target_w / mark.width))),
            Image.Resampling.LANCZOS,
        )
        line_y = max(18, int(canvas.height * 0.028))
        lx = ix + int(iw * 0.035)
        ly = max(line_y + int(canvas.height * 0.018), iy + int(ih * 0.04))
        patch = canvas.crop((lx, ly, min(canvas.width, lx + L.width), min(canvas.height, ly + L.height)))
        mean = float(np.asarray(patch.convert("L"), dtype=np.float32).mean())
        overlay = canvas.convert("RGBA")
        if mean > 90:
            pad_x, pad_y = 14, 10
            plate = Image.new("RGBA", (L.width + pad_x * 2, L.height + pad_y * 2), (0, 0, 0, 0))
            ImageDraw.Draw(plate).rounded_rectangle(
                [0, 0, plate.width - 1, plate.height - 1], radius=10, fill=(8, 10, 14, 185)
            )
            overlay.alpha_composite(plate, (max(0, lx - pad_x), max(0, ly - pad_y)))
        overlay.alpha_composite(L, (lx, ly))
        overlay.convert("RGB").save(output_path, "JPEG", quality=quality, optimize=True, progressive=True)

    return {
        "input": str(input_path),
        "output": str(output_path),
        "orientation": orientation,
        "brightness_mean": round(mean, 2),
        "logo_variant": "topline_gold",
        "logo_path": str(mark_path),
        "logo_xy": [lx, ly],
        "logo_size": [L.width, L.height],
    }


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("inputs", nargs="+", type=Path)
    ap.add_argument("-o", "--output-dir", type=Path, required=True)
    ap.add_argument("--mark", type=Path, default=DEFAULT_MARK)
    ap.add_argument("--report", type=Path, default=None)
    args = ap.parse_args(argv)
    files = []
    for p in args.inputs:
        p = p.expanduser().resolve()
        if p.is_dir():
            files.extend(sorted(x for x in p.iterdir() if x.is_file() and x.suffix.lower() in IMAGE_EXTS))
        elif p.is_file() and p.suffix.lower() in IMAGE_EXTS:
            files.append(p)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    results = []
    for i, src in enumerate(files, 1):
        dst = args.output_dir / f"{i:02d}_{src.stem}_logo.jpg"
        info = format_gallery_topline(src, dst, mark_path=args.mark)
        results.append(info)
        print(f"{src.name} -> {dst.name}")
    report = args.report or (args.output_dir / "gallery_topline_report.json")
    report.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
