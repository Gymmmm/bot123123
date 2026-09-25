#!/usr/bin/env python3
"""Neutralize source watermarks / contact overlays / competitor logos on listing photos.

v3 practical approach (no OCR dependency):
  1) Heuristic detection of bottom contact banners, corner logos, and slogan bands
  2) Prefer cropping the bottom contact/slogan strip off (“P off”) before inpaint
  3) OpenCV inpaint on remaining corner marks (TELEA)
  4) Never invent furniture — soft inpaint or crop only

Demo CLI writes before/after into an output folder.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import cv2
import numpy as np
from PIL import Image, ImageDraw, ImageOps

ROOT = Path(__file__).resolve().parent
IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp"}

# ---------------------------------------------------------------------------
# Detection
# ---------------------------------------------------------------------------


def _glyph_mask(gray: np.ndarray, min_area: int = 12, max_area: int = 1200) -> np.ndarray:
    """Binary mask of small high-contrast blobs that look like glyphs / logos."""
    kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (2, 2))
    grad = cv2.morphologyEx(gray, cv2.MORPH_GRADIENT, kernel)
    # Also catch bright text on dark / dark text on bright via adaptive bits
    thr1 = (grad > 40).astype(np.uint8) * 255
    # bright-on-dark
    bright = cv2.adaptiveThreshold(
        gray, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 31, -8
    )
    # dark-on-bright
    dark = cv2.adaptiveThreshold(
        gray, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY_INV, 31, 8
    )
    combo = cv2.bitwise_or(thr1, cv2.bitwise_and(bright, (gray < 200).astype(np.uint8) * 255))
    combo = cv2.bitwise_or(combo, cv2.bitwise_and(dark, (gray > 40).astype(np.uint8) * 255))

    nlab, labels, stats, _ = cv2.connectedComponentsWithStats(combo, 8)
    mask = np.zeros_like(gray, dtype=np.uint8)
    for i in range(1, nlab):
        area = stats[i, cv2.CC_STAT_AREA]
        ww = stats[i, cv2.CC_STAT_WIDTH]
        hh = stats[i, cv2.CC_STAT_HEIGHT]
        if min_area <= area <= max_area and 1 <= hh <= 60 and 1 <= ww <= 220:
            mask[labels == i] = 255
    # Expand slightly so inpaint covers anti-aliased edges
    mask = cv2.dilate(mask, cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (5, 5)), iterations=1)
    return mask


def _banner_rows(gray: np.ndarray, above_mean: float) -> np.ndarray:
    """Rows that look like a flat banner strip (low variance, shifted mean)."""
    row_std = gray.std(axis=1)
    row_mean = gray.mean(axis=1)
    low_std = row_std < 22
    shifted = np.abs(row_mean - above_mean) > 22
    return low_std & shifted


def detect_source_marks(img_bgr: np.ndarray) -> dict[str, Any]:
    """Build an inpaint mask + diagnostics for common source marks."""
    h, w = img_bgr.shape[:2]
    gray = cv2.cvtColor(img_bgr, cv2.COLOR_BGR2GRAY)
    mask = np.zeros((h, w), dtype=np.uint8)
    regions: list[dict[str, Any]] = []

    def add_region(kind: str, local_mask: np.ndarray, y0: int, x0: int, **extra):
        nonlocal mask
        if local_mask is None or local_mask.size == 0 or float(local_mask.mean()) == 0:
            return
        yh, xw = local_mask.shape[:2]
        mask[y0 : y0 + yh, x0 : x0 + xw] = cv2.bitwise_or(
            mask[y0 : y0 + yh, x0 : x0 + xw], local_mask
        )
        regions.append({"kind": kind, **extra, "box": [y0, y0 + yh, x0, x0 + xw]})

    # --- White (or dark) text line detector via horizontal morphology ---
    def text_line_mask(roi_gray: np.ndarray, bright: bool = True) -> np.ndarray:
        if bright:
            bw = (roi_gray > 175).astype(np.uint8) * 255
        else:
            bw = (roi_gray < 55).astype(np.uint8) * 255
        # Join characters into words/lines
        horiz = cv2.getStructuringElement(
            cv2.MORPH_RECT, (max(15, roi_gray.shape[1] // 40), 3)
        )
        joined = cv2.morphologyEx(bw, cv2.MORPH_CLOSE, horiz, iterations=2)
        joined = cv2.dilate(joined, cv2.getStructuringElement(cv2.MORPH_RECT, (5, 5)), 1)
        # Keep elongated components (text lines / logos)
        nlab, labels, stats, _ = cv2.connectedComponentsWithStats(joined, 8)
        out = np.zeros_like(roi_gray, dtype=np.uint8)
        for i in range(1, nlab):
            area = stats[i, cv2.CC_STAT_AREA]
            ww = stats[i, cv2.CC_STAT_WIDTH]
            hh = stats[i, cv2.CC_STAT_HEIGHT]
            if area < 80:
                continue
            aspect = ww / max(hh, 1)
            # slogan lines are wide; corner logos are moderately wide
            if (aspect >= 3.0 and hh <= 80 and ww >= roi_gray.shape[1] * 0.15) or (
                aspect >= 1.5 and 15 <= hh <= 90 and 40 <= ww <= roi_gray.shape[1] * 0.9
            ):
                out[labels == i] = 255
        return out

    # --- Bottom slogan / contact banner (prefer crop — “P off” the strip) ---
    y_bot = int(h * 0.78)
    bottom = gray[y_bot:, :]
    bright_lines = text_line_mask(bottom, bright=True)
    dark_lines = text_line_mask(bottom, bright=False)
    bottom_text = cv2.bitwise_or(bright_lines, dark_lines)
    # Also glyph density fallback
    bottom_glyphs = _glyph_mask(bottom)
    bottom_glyph_density = float(bottom_glyphs.mean() / 255.0)
    bottom_text_density = float(bottom_text.mean() / 255.0)

    # Row-peak: concentrated bright text band / flat contact bar
    row_bright = (bottom > 175).mean(axis=1)
    row_dark = (bottom < 40).mean(axis=1)
    peak_rows = (row_bright > 0.025) | (row_dark > 0.55)
    peak_frac = float(np.mean(peak_rows)) if peak_rows.size else 0.0
    # Flat dark/bright strip across most of the width (classic contact footer)
    row_std = bottom.std(axis=1)
    flat_banner_rows = (row_std < 28) & ((row_bright > 0.02) | (row_dark > 0.35) | (np.abs(bottom.mean(axis=1) - gray[: max(1, y_bot)].mean()) > 28))
    flat_banner_frac = float(np.mean(flat_banner_rows)) if flat_banner_rows.size else 0.0

    crop_bottom = False
    crop_frac = 0.0
    if (
        bottom_text_density > 0.006
        or peak_frac > 0.05
        or bottom_glyph_density > 0.04
        or flat_banner_frac > 0.12
    ):
        local = cv2.bitwise_or(bottom_text, bottom_glyphs)
        # Expand to full-width strip covering active text / banner rows
        row_active = (
            (local.mean(axis=1) > 0.8)
            | (row_bright > 0.02)
            | (row_dark > 0.45)
            | flat_banner_rows
        )
        if np.any(row_active) or peak_frac > 0.05 or flat_banner_frac > 0.12:
            if np.any(row_active):
                idxs = np.where(row_active)[0]
                first, last = int(idxs.min()), int(idxs.max())
            else:
                first, last = 0, bottom.shape[0] - 1
            first = max(0, first - 8)
            last = min(bottom.shape[0] - 1, last + 8)
            # Full-width band — safer than speckled inpaint for slogans/contacts
            band = np.zeros_like(bottom, dtype=np.uint8)
            band[first : last + 1, :] = 255
            # Contact / slogan bars are always cropped off when detected.
            crop_bottom = True
            crop_frac = min(0.22, max(0.08, (bottom.shape[0] - first) / float(h) + 0.02))
            add_region(
                "bottom_banner",
                band,
                y_bot,
                0,
                glyph_density=round(bottom_glyph_density, 4),
                text_density=round(bottom_text_density, 4),
                peak_frac=round(peak_frac, 4),
                flat_banner_frac=round(flat_banner_frac, 4),
            )

    # --- Corner logos ---
    corner_boxes = [
        ("top_left", 0, int(h * 0.18), 0, int(w * 0.36)),
        ("top_right", 0, int(h * 0.18), int(w * 0.64), w),
        ("bottom_left", int(h * 0.82), h, 0, int(w * 0.30)),
        ("bottom_right", int(h * 0.82), h, int(w * 0.70), w),
    ]
    for name, ya, yb, xa, xb in corner_boxes:
        roi = gray[ya:yb, xa:xb]
        if roi.size == 0:
            continue
        # Prefer bright text/logo on darker surround, and vice versa
        lines = cv2.bitwise_or(text_line_mask(roi, True), text_line_mask(roi, False))
        glyphs = _glyph_mask(roi, min_area=20, max_area=3500)
        local = cv2.bitwise_or(lines, glyphs)
        dens = float(local.mean() / 255.0)
        # Compact bright plaque (logo lockup)
        for thr, inv in ((180, False), (50, True)):
            bw = ((roi < thr) if inv else (roi > thr)).astype(np.uint8) * 255
            bw = cv2.morphologyEx(
                bw, cv2.MORPH_CLOSE, cv2.getStructuringElement(cv2.MORPH_RECT, (9, 5))
            )
            nlab, labels, stats, _ = cv2.connectedComponentsWithStats(bw, 8)
            for i in range(1, nlab):
                area = stats[i, cv2.CC_STAT_AREA]
                ww = stats[i, cv2.CC_STAT_WIDTH]
                hh = stats[i, cv2.CC_STAT_HEIGHT]
                aspect = ww / max(hh, 1)
                # Ignore huge bright windows / LED strips filling the corner
                if area > roi.size * 0.35:
                    continue
                if 120 < area < roi.size * 0.35 and 12 <= hh <= 100 and 40 <= ww and 1.4 <= aspect <= 12:
                    x, y = stats[i, cv2.CC_STAT_LEFT], stats[i, cv2.CC_STAT_TOP]
                    pad = 6
                    local[
                        max(0, y - pad) : min(local.shape[0], y + hh + pad),
                        max(0, x - pad) : min(local.shape[1], x + ww + pad),
                    ] = 255
        # Extra: bright logo text on dark ceiling (common developer corner mark)
        if float(roi.mean()) < 120:
            bright = (roi > 155).astype(np.uint8) * 255
            bright = cv2.morphologyEx(
                bright,
                cv2.MORPH_CLOSE,
                cv2.getStructuringElement(cv2.MORPH_RECT, (max(15, roi.shape[1] // 16), 5)),
                iterations=2,
            )
            nlab, labels, stats, _ = cv2.connectedComponentsWithStats(bright, 8)
            boxes = []
            for i in range(1, nlab):
                area = stats[i, cv2.CC_STAT_AREA]
                ww = stats[i, cv2.CC_STAT_WIDTH]
                hh = stats[i, cv2.CC_STAT_HEIGHT]
                if area > roi.size * 0.45:
                    continue
                if 40 < area and 6 <= hh <= 90 and ww >= 25:
                    x, y = stats[i, cv2.CC_STAT_LEFT], stats[i, cv2.CC_STAT_TOP]
                    boxes.append((x, y, ww, hh))
            if boxes:
                # Union bbox of bright logo pieces → solid fill (better inpaint)
                x0 = min(b[0] for b in boxes)
                y0 = min(b[1] for b in boxes)
                x1 = max(b[0] + b[2] for b in boxes)
                y1 = max(b[1] + b[3] for b in boxes)
                pad = 10
                local[
                    max(0, y0 - pad) : min(local.shape[0], y1 + pad),
                    max(0, x0 - pad) : min(local.shape[1], x1 + pad),
                ] = 255

        dens = float(local.mean() / 255.0)
        if dens > 0.012:
            local = cv2.dilate(
                local, cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (5, 5)), iterations=1
            )
            # Cap corner mask size
            if dens > 0.35:
                # keep only largest few components
                nlab, labels, stats, _ = cv2.connectedComponentsWithStats(local, 8)
                keep = np.zeros_like(local)
                comps = sorted(
                    ((stats[i, cv2.CC_STAT_AREA], i) for i in range(1, nlab)), reverse=True
                )
                budget = int(local.size * 0.20)
                used = 0
                for area, i in comps:
                    if used and used + area > budget:
                        break
                    keep[labels == i] = 255
                    used += area
                local = keep
            add_region(
                f"corner_{name}",
                local,
                ya,
                xa,
                glyph_density=round(dens, 4),
            )

    # --- Mid-frame bottom-centered slogan (developer watermark) ---
    cy0, cy1 = int(h * 0.72), int(h * 0.97)
    cx0, cx1 = int(w * 0.08), int(w * 0.92)
    mid = gray[cy0:cy1, cx0:cx1]
    mid_lines = text_line_mask(mid, bright=True)
    mid_d = float(mid_lines.mean() / 255.0)
    if mid_d > 0.004:
        # Expand vertically a bit around text rows
        row_a = mid_lines.mean(axis=1) > 1
        local = mid_lines.copy()
        if np.any(row_a):
            idxs = np.where(row_a)[0]
            first, last = max(0, int(idxs.min()) - 8), min(mid.shape[0] - 1, int(idxs.max()) + 8)
            # full width of mid box for clean inpaint/crop of slogan
            strip = np.zeros_like(mid, dtype=np.uint8)
            strip[first : last + 1, :] = 255
            local = strip
        add_region(
            "center_slogan",
            local,
            cy0,
            cx0,
            glyph_density=round(mid_d, 4),
        )
        # If slogan sits in lower portion, prefer crop
        slogan_bottom = (cy0 + (int(np.where(row_a)[0].max()) if np.any(row_a) else mid.shape[0])) / h
        if np.any(row_a):
            slogan_top = cy0 + int(np.where(row_a)[0].min())
            # Slogan in lower third → crop above it
            if slogan_top >= int(h * 0.78):
                crop_bottom = True
                crop_frac = min(0.18, max(0.08, 1.0 - (slogan_top - 12) / float(h)))

    coverage = float(mask.mean() / 255.0)
    return {
        "mask": mask,
        "regions": regions,
        "coverage": round(coverage, 5),
        "crop_bottom": crop_bottom,
        "crop_frac": round(crop_frac, 4),
        "bottom_glyph_density": round(bottom_glyph_density, 4),
    }


def scrub_image(
    img_bgr: np.ndarray,
    *,
    prefer_crop: bool = True,
    inpaint_radius: int = 3,
) -> tuple[np.ndarray, dict[str, Any]]:
    """Return scrubbed BGR image + metadata."""
    det = detect_source_marks(img_bgr)
    mask = det["mask"]
    meta: dict[str, Any] = {
        "regions": det["regions"],
        "coverage": det["coverage"],
        "method": [],
        "crop_frac": 0.0,
    }

    out = img_bgr.copy()
    h, w = out.shape[:2]

    # Prefer crop when bottom slogan/contact banner detected — surgically “P off”
    if prefer_crop and det["crop_bottom"] and det["crop_frac"] >= 0.06:
        cut = int(h * (1.0 - det["crop_frac"]))
        cut = max(int(h * 0.75), min(cut, int(h * 0.94)))
        out = out[:cut, :, :].copy()
        meta["method"].append(f"crop_bottom_{det['crop_frac']:.2f}")
        meta["crop_frac"] = det["crop_frac"]
        det2 = detect_source_marks(out)
        # Drop bottom_* regions already handled by crop; keep corner logos
        keep_regions = [
            r
            for r in det2["regions"]
            if str(r.get("kind", "")).startswith("corner_top")
        ]
        mask = np.zeros(out.shape[:2], dtype=np.uint8)
        for r in keep_regions:
            ya, yb, xa, xb = r["box"]
            # rebuild from fresh det2 mask in that box
            mask[ya:yb, xa:xb] = cv2.bitwise_or(mask[ya:yb, xa:xb], det2["mask"][ya:yb, xa:xb])
        meta["regions"] = det["regions"] + [{**r, "after_crop": True} for r in keep_regions]
        meta["coverage"] = round(float(mask.mean() / 255.0), 5)
    else:
        # If center slogan / bottom banner present but not cropped, force crop
        has_slogan = any(r.get("kind") == "center_slogan" for r in det["regions"])
        has_bottom = any(r.get("kind") == "bottom_banner" for r in det["regions"])
        if prefer_crop and (has_slogan or has_bottom):
            # Find lowest y0 among slogan/bottom regions
            ys = [
                r["box"][0]
                for r in det["regions"]
                if r.get("kind") in ("center_slogan", "bottom_banner")
            ]
            if ys:
                y0 = min(ys)
                # only crop if mark is in lower 25%
                if y0 >= int(h * 0.75):
                    cut = max(int(h * 0.75), y0 - 16)
                    out = out[:cut, :, :].copy()
                    meta["method"].append("crop_bottom_auto")
                    meta["crop_frac"] = round(1.0 - cut / h, 4)
                    det2 = detect_source_marks(out)
                    keep_regions = [
                        r
                        for r in det2["regions"]
                        if str(r.get("kind", "")).startswith("corner_top")
                    ]
                    mask = np.zeros(out.shape[:2], dtype=np.uint8)
                    for r in keep_regions:
                        ya, yb, xa, xb = r["box"]
                        mask[ya:yb, xa:xb] = cv2.bitwise_or(
                            mask[ya:yb, xa:xb], det2["mask"][ya:yb, xa:xb]
                        )
                    meta["regions"] = det["regions"] + [
                        {**r, "after_crop": True} for r in keep_regions
                    ]

    if mask is not None and float(mask.mean()) > 0:
        cov = float(mask.mean() / 255.0)
        if cov > 0.15:
            nlab, labels, stats, _ = cv2.connectedComponentsWithStats(mask, 8)
            keep = np.zeros_like(mask)
            areas = [(stats[i, cv2.CC_STAT_AREA], i) for i in range(1, nlab)]
            areas.sort(reverse=True)
            budget = int(mask.size * 0.08)
            used = 0
            for area, i in areas:
                if used + area > budget and used > 0:
                    break
                keep[labels == i] = 255
                used += area
            mask = keep
            meta["method"].append("mask_budget_trimmed")
            cov = float(mask.mean() / 255.0)
        if cov > 0.0005:
            # Slight dilate for coverage of anti-alias, but keep small
            mask = cv2.dilate(mask, cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (3, 3)), 1)
            out = cv2.inpaint(out, mask, inpaint_radius, cv2.INPAINT_TELEA)
            meta["method"].append("inpaint_telea")
            meta["inpaint_coverage"] = round(cov, 5)

    if not meta["method"]:
        meta["method"].append("noop")

    meta["mask_preview"] = mask
    return out, meta


def scrub_file(
    input_path: str | Path,
    output_path: str | Path,
    *,
    prefer_crop: bool = True,
    save_mask: bool = False,
) -> dict[str, Any]:
    input_path = Path(input_path)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Preserve orientation via Pillow then hand to OpenCV
    with Image.open(input_path) as im:
        im = ImageOps.exif_transpose(im).convert("RGB")
        rgb = np.asarray(im)
    img_bgr = cv2.cvtColor(rgb, cv2.COLOR_RGB2BGR)

    scrubbed, meta = scrub_image(img_bgr, prefer_crop=prefer_crop)
    mask = meta.pop("mask_preview", None)

    out_rgb = cv2.cvtColor(scrubbed, cv2.COLOR_BGR2RGB)
    Image.fromarray(out_rgb).save(output_path, quality=92, optimize=True)

    result = {
        "input": str(input_path),
        "output": str(output_path),
        "methods": meta.get("method"),
        "regions": meta.get("regions"),
        "coverage": meta.get("coverage"),
        "inpaint_coverage": meta.get("inpaint_coverage"),
        "crop_frac": meta.get("crop_frac"),
        "size_in": list(img_bgr.shape[:2][::-1]),
        "size_out": list(scrubbed.shape[:2][::-1]),
    }

    if save_mask and mask is not None:
        mask_path = output_path.with_name(output_path.stem + "_mask.png")
        # resize mask if crop changed shape
        if mask.shape[:2] != scrubbed.shape[:2]:
            # mask from last stage should match; if not, skip
            if mask.shape[0] >= scrubbed.shape[0]:
                mask = mask[: scrubbed.shape[0], : scrubbed.shape[1]]
        cv2.imwrite(str(mask_path), mask)
        result["mask"] = str(mask_path)

    return result


def make_before_after(
    before_path: Path,
    after_path: Path,
    out_path: Path,
    label_before: str = "BEFORE",
    label_after: str = "AFTER",
) -> Path:
    a = Image.open(before_path).convert("RGB")
    b = Image.open(after_path).convert("RGB")
    # Match heights
    h = max(a.height, b.height)
    def fit(im):
        scale = h / im.height
        return im.resize((max(1, int(im.width * scale)), h), Image.Resampling.LANCZOS)
    a, b = fit(a), fit(b)
    gap = 12
    canvas = Image.new("RGB", (a.width + b.width + gap, h + 36), (245, 245, 245))
    canvas.paste(a, (0, 36))
    canvas.paste(b, (a.width + gap, 36))
    draw = ImageDraw.Draw(canvas)
    draw.text((8, 8), label_before, fill=(20, 20, 20))
    draw.text((a.width + gap + 8, 8), label_after, fill=(20, 20, 20))
    out_path.parent.mkdir(parents=True, exist_ok=True)
    canvas.save(out_path, quality=90)
    return out_path


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Scrub source watermarks / contacts from photos")
    ap.add_argument("inputs", nargs="+", type=Path, help="Image path(s) or directories")
    ap.add_argument("-o", "--output-dir", type=Path, required=True)
    ap.add_argument("--no-crop", action="store_true", help="Disable bottom crop fallback")
    ap.add_argument("--save-mask", action="store_true")
    ap.add_argument("--compare", action="store_true", help="Also write before/after strips")
    args = ap.parse_args(argv)

    files: list[Path] = []
    for p in args.inputs:
        p = p.expanduser().resolve()
        if p.is_dir():
            files.extend(
                sorted(x for x in p.iterdir() if x.suffix.lower() in IMAGE_EXTS)
            )
        elif p.is_file():
            files.append(p)
    if not files:
        print("No inputs", file=sys.stderr)
        return 1

    args.output_dir.mkdir(parents=True, exist_ok=True)
    results = []
    for src in files:
        dst = args.output_dir / f"{src.stem}_scrubbed.jpg"
        info = scrub_file(
            src,
            dst,
            prefer_crop=not args.no_crop,
            save_mask=args.save_mask,
        )
        # Also keep a copy of the original for demos
        before_copy = args.output_dir / f"{src.stem}_before.jpg"
        if not before_copy.exists():
            with Image.open(src) as im:
                ImageOps.exif_transpose(im).convert("RGB").save(
                    before_copy, quality=92
                )
        info["before"] = str(before_copy)
        if args.compare:
            cmp_path = args.output_dir / f"{src.stem}_compare.jpg"
            make_before_after(before_copy, dst, cmp_path)
            info["compare"] = str(cmp_path)
        results.append(info)
        print(
            f"{src.name}: methods={info['methods']} "
            f"regions={len(info.get('regions') or [])} → {dst.name}"
        )

    report = args.output_dir / "scrub_report.json"
    with open(report, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"report: {report}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
