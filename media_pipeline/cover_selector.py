#!/usr/bin/env python3
"""Cover photo selection for 侨联 listing media.

Scores listing photos and returns the best cover candidate plus a score dict.
See COVER_RULES.md for the locked policy.
"""
from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path
from typing import Any

import cv2
import numpy as np
from PIL import Image, ImageOps

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from photo_ranker import (  # noqa: E402
    IMAGE_EXTS,
    analyze_photo,
    calc_phash,
    deduplicate,
    hamming,
)

# ---------------------------------------------------------------------------
# Heuristic feature helpers (no ML room classifier in v1)
# ---------------------------------------------------------------------------


def _load_rgb(path: Path) -> np.ndarray | None:
    with Image.open(path) as im:
        im = ImageOps.exif_transpose(im).convert("RGB")
        return np.asarray(im)


def _edge_density(gray: np.ndarray) -> float:
    edges = cv2.Canny(gray, 60, 140)
    return float(edges.mean() / 255.0)


def text_band_score(img_bgr: np.ndarray) -> dict[str, float]:
    """Detect heavy text / contact banners in bottom & corners."""
    h, w = img_bgr.shape[:2]
    gray = cv2.cvtColor(img_bgr, cv2.COLOR_BGR2GRAY)

    def band_textiness(y0: int, y1: int, x0: int, x1: int) -> float:
        roi = gray[y0:y1, x0:x1]
        if roi.size == 0:
            return 0.0
        # Morphological gradient → small high-contrast blobs ≈ glyphs
        kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (3, 3))
        grad = cv2.morphologyEx(roi, cv2.MORPH_GRADIENT, kernel)
        thr = (grad > 45).astype(np.uint8) * 255
        nlab, _, stats, _ = cv2.connectedComponentsWithStats(thr, 8)
        glyphs = 0
        for i in range(1, nlab):
            area = stats[i, cv2.CC_STAT_AREA]
            ww = stats[i, cv2.CC_STAT_WIDTH]
            hh = stats[i, cv2.CC_STAT_HEIGHT]
            if 15 < area < 900 and 2 <= hh <= 42 and 2 <= ww <= 140:
                glyphs += 1
        area_norm = max(1.0, roi.size / 4000.0)
        return min(1.0, glyphs / area_norm / 3.0)

    bottom = band_textiness(int(h * 0.88), h, 0, w)
    top = band_textiness(0, int(h * 0.12), 0, w)
    corners = [
        band_textiness(0, int(h * 0.14), 0, int(w * 0.28)),
        band_textiness(0, int(h * 0.14), int(w * 0.72), w),
        band_textiness(int(h * 0.86), h, 0, int(w * 0.28)),
        band_textiness(int(h * 0.86), h, int(w * 0.72), w),
    ]
    corner_max = max(corners) if corners else 0.0
    heavy = max(bottom, corner_max, top * 0.8)
    return {
        "bottom_text": round(bottom, 3),
        "top_text": round(top, 3),
        "corner_text_max": round(corner_max, 3),
        "text_heavy": round(heavy, 3),
    }


def room_heuristic(img_bgr: np.ndarray) -> dict[str, Any]:
    """Cheap room-type hints without a classifier.

    Signals used:
    - toilet: large bright near-white blob + high local contrast in lower half
    - kitchen: warm cabinet-ish hues + many horizontal edges mid-frame
    - exterior: sky-blue top band + green/gray ground, higher outdoor brightness
    - living/bedroom: mid interior colors, furniture-scale edge structure
    """
    h, w = img_bgr.shape[:2]
    hsv = cv2.cvtColor(img_bgr, cv2.COLOR_BGR2HSV)
    gray = cv2.cvtColor(img_bgr, cv2.COLOR_BGR2GRAY)
    rgb = cv2.cvtColor(img_bgr, cv2.COLOR_BGR2RGB)

    # Toilet / bathroom: very bright porcelain dominance in lower/central region
    bright = np.mean(gray > 210)
    very_white = np.mean((rgb[:, :, 0] > 200) & (rgb[:, :, 1] > 200) & (rgb[:, :, 2] > 200))
    lower = gray[int(h * 0.45) :, :]
    lower_white = float(np.mean(lower > 205))
    toilet_score = min(1.0, (bright * 0.4 + very_white * 0.4 + lower_white * 0.6))

    # Kitchen: warm browns/wood + dense mid horizontal edges
    warm = (
        (hsv[:, :, 0] < 35)
        & (hsv[:, :, 1] > 40)
        & (hsv[:, :, 2] > 60)
        & (hsv[:, :, 2] < 220)
    )
    warm_frac = float(np.mean(warm))
    edges = cv2.Canny(gray, 50, 120)
    mid = edges[int(h * 0.25) : int(h * 0.75), :]
    # horizontal edge preference via Sobel-Y
    sobely = cv2.Sobel(gray, cv2.CV_32F, 0, 1, ksize=3)
    horiz = float(np.mean(np.abs(sobely) > 40))
    kitchen_score = min(1.0, warm_frac * 1.5 + horiz * 0.8 + float(mid.mean() / 255) * 0.5)

    # Exterior: blue-ish top + outdoor brightness / saturation mix
    top = hsv[: int(h * 0.28), :, :]
    blue = (top[:, :, 0] > 90) & (top[:, :, 0] < 135) & (top[:, :, 1] > 40) & (top[:, :, 2] > 80)
    blue_frac = float(np.mean(blue))
    sat_mean = float(hsv[:, :, 1].mean())
    exterior_score = min(1.0, blue_frac * 2.2 + (sat_mean / 120.0) * 0.25)

    # Living / bedroom: moderate brightness, furniture-scale edges, not toilet-white
    edge_d = _edge_density(gray)
    mean_b = float(gray.mean())
    living_score = 0.0
    if 55 < mean_b < 190 and 0.02 < edge_d < 0.18 and toilet_score < 0.55:
        living_score = min(
            1.0,
            0.35
            + (1.0 - abs(mean_b - 125) / 125.0) * 0.35
            + min(edge_d / 0.08, 1.0) * 0.3
            - toilet_score * 0.4
            - exterior_score * 0.15,
        )
        living_score = max(0.0, living_score)

    bedroom_score = living_score * 0.85  # without bed detector, share living pool
    # Soft boost if soft textile-ish mid tones (lower sat)
    if sat_mean < 55 and living_score > 0.25:
        bedroom_score = min(1.0, bedroom_score + 0.1)

    scores = {
        "toilet": round(float(toilet_score), 3),
        "kitchen": round(float(kitchen_score), 3),
        "exterior": round(float(exterior_score), 3),
        "living": round(float(max(0.0, living_score)), 3),
        "bedroom": round(float(max(0.0, bedroom_score)), 3),
    }
    preferred = max(
        [("living", scores["living"]), ("exterior", scores["exterior"]), ("bedroom", scores["bedroom"])],
        key=lambda x: x[1],
    )
    avoided = max(
        [("toilet", scores["toilet"]), ("kitchen", scores["kitchen"])],
        key=lambda x: x[1],
    )
    label = preferred[0]
    if avoided[1] > preferred[1] + 0.08 and avoided[1] > 0.45:
        label = avoided[0]
    scores["label"] = label
    return scores


def score_cover_candidate(path: Path, base: dict[str, Any] | None = None) -> dict[str, Any]:
    """Full cover score for one photo."""
    path = Path(path)
    base = base or analyze_photo(path)
    if not base.get("valid"):
        return {
            **base,
            "cover_score": -999.0,
            "cover_reject": True,
            "cover_reject_reasons": ["unreadable"],
        }

    img = cv2.imread(str(path))
    if img is None:
        return {
            **base,
            "cover_score": -999.0,
            "cover_reject": True,
            "cover_reject_reasons": ["opencv_read_failed"],
        }

    h, w = img.shape[:2]
    ratio = w / max(h, 1)
    text = text_band_score(img)
    room = room_heuristic(img)

    reject_reasons: list[str] = list(base.get("reject_reasons") or [])
    # Extra cover-specific rejects / downranks
    if ratio < 0.62:
        reject_reasons.append("extreme_portrait")
    if text["text_heavy"] >= 0.55:
        reject_reasons.append("heavy_text_watermark")
    if base.get("sharpness", 0) < 35:
        if "too_blurry" not in reject_reasons:
            reject_reasons.append("too_blurry")
    if base.get("brightness_score", 1) < 0.2:
        if "too_dark" not in reject_reasons:
            reject_reasons.append("too_dark")

    # Preference bonuses
    room_bonus = 0.0
    label = room["label"]
    if label in ("living", "exterior"):
        room_bonus = 0.22 + 0.10 * room[label]
    elif label == "bedroom":
        room_bonus = 0.14 + 0.08 * room["bedroom"]
    elif label == "kitchen":
        room_bonus = -0.12 - 0.10 * room["kitchen"]
    elif label == "toilet":
        room_bonus = -0.35 - 0.20 * room["toilet"]

    # Landscape-ish
    if ratio >= 1.25:
        orient_bonus = 0.18
    elif ratio >= 1.05:
        orient_bonus = 0.12
    elif ratio >= 0.85:
        orient_bonus = 0.02
    elif ratio >= 0.70:
        orient_bonus = -0.08
    else:
        orient_bonus = -0.22

    sharpness_norm = min(float(base.get("sharpness", 0)) / 500.0, 1.0)
    text_penalty = text["text_heavy"] * 0.45
    quality = float(base.get("score") or 0)

    cover_score = (
        quality * 0.55
        + sharpness_norm * 0.15
        + room_bonus
        + orient_bonus
        - text_penalty
        - (0.25 if "extreme_portrait" in reject_reasons else 0.0)
    )

    hard_reject = any(
        r in reject_reasons
        for r in ("too_blurry", "opencv_read_failed", "unreadable", "too_small")
    )
    # toilet-only hard-downrank flag (not absolute reject if sole photo)
    soft_reject = ("heavy_text_watermark" in reject_reasons) or (
        label == "toilet" and room["toilet"] > 0.6
    )

    return {
        **base,
        "ratio": round(ratio, 3),
        "room": room,
        "text": text,
        "room_bonus": round(room_bonus, 3),
        "orient_bonus": round(orient_bonus, 3),
        "text_penalty": round(text_penalty, 3),
        "cover_score": round(cover_score, 4),
        "cover_reject": bool(hard_reject),
        "cover_soft_reject": bool(soft_reject),
        "cover_reject_reasons": reject_reasons,
    }


def select_cover(paths: list[str | Path]) -> tuple[str | None, dict[str, Any]]:
    """Pick best cover path and return (best_path, scores_dict).

    scores_dict keys:
      - ranked: list of per-photo score dicts (best first)
      - best_path / best
      - duplicates
      - policy
    """
    path_list = [Path(p) for p in paths]
    path_list = [p for p in path_list if p.is_file() and p.suffix.lower() in IMAGE_EXTS]
    if not path_list:
        return None, {
            "ranked": [],
            "best_path": None,
            "best": None,
            "duplicates": [],
            "policy": "cover_selector_v1",
            "error": "no_images",
        }

    analyzed = [score_cover_candidate(p) for p in path_list]

    # Dedup using photo_ranker (exact + perceptual); keep higher cover_score
    # First run standard dedup on base fields, then re-score keep list.
    kept, duplicates = deduplicate(analyzed)
    # Prefer non soft-reject / non hard-reject
    usable = [x for x in kept if not x.get("cover_reject")]
    pool = usable if usable else kept

    preferred = [
        x
        for x in pool
        if not x.get("cover_soft_reject")
        and (x.get("room") or {}).get("label") in ("living", "exterior", "bedroom")
    ]
    if not preferred:
        preferred = [x for x in pool if not x.get("cover_soft_reject")]
    if not preferred:
        preferred = pool

    preferred.sort(key=lambda x: float(x.get("cover_score") or -999), reverse=True)
    # Full ranked list for report
    ranked = sorted(kept, key=lambda x: float(x.get("cover_score") or -999), reverse=True)

    best = preferred[0] if preferred else (ranked[0] if ranked else None)
    best_path = best["file"] if best else None

    scores_dict = {
        "ranked": ranked,
        "best_path": best_path,
        "best": best,
        "duplicates": duplicates,
        "policy": "cover_selector_v1",
        "prefer": ["living", "exterior", "bedroom"],
        "downrank": ["toilet", "kitchen_only", "blurry", "dark", "extreme_portrait", "heavy_text"],
    }
    return best_path, scores_dict


def select_cover_from_folder(folder: str | Path) -> tuple[str | None, dict[str, Any]]:
    folder = Path(folder)
    files = sorted(
        p for p in folder.rglob("*") if p.is_file() and p.suffix.lower() in IMAGE_EXTS
    )
    return select_cover(files)


def _json_default(obj):
    if isinstance(obj, (np.floating,)):
        return float(obj)
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    raise TypeError(type(obj))


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Select best cover photo from a folder")
    ap.add_argument("folder", type=Path, help="Folder of listing photos")
    ap.add_argument(
        "-o",
        "--output-dir",
        type=Path,
        default=None,
        help="Where to write cover_pick_report.json + winner copy",
    )
    args = ap.parse_args(argv)

    folder = args.folder.expanduser().resolve()
    out_dir = (args.output_dir or (ROOT / "output" / "media_pipeline_brand_v2" / "cover_pick"))
    out_dir = out_dir.expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    best_path, scores = select_cover_from_folder(folder)
    report_path = out_dir / "cover_pick_report.json"
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(scores, f, ensure_ascii=False, indent=2, default=_json_default)

    winner_dst = None
    if best_path:
        src = Path(best_path)
        winner_dst = out_dir / f"cover_winner{src.suffix.lower() or '.jpg'}"
        shutil.copy2(src, winner_dst)
        print(f"winner: {src}")
        print(f"copied: {winner_dst}")
        print(f"cover_score: {(scores.get('best') or {}).get('cover_score')}")
        print(f"room: {(scores.get('best') or {}).get('room')}")
    else:
        print("No usable cover candidate.", file=sys.stderr)

    print(f"report: {report_path}")
    print(f"candidates: {len(scores.get('ranked') or [])}")
    return 0 if best_path else 1


if __name__ == "__main__":
    raise SystemExit(main())
