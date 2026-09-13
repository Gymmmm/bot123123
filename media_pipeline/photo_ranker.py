#!/usr/bin/env python3
"""Gym media_pipeline_v1 — photo quality scoring + dedup (logic unchanged)."""
from pathlib import Path
import hashlib
import cv2
import numpy as np
from PIL import Image, ImageOps
IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp"}
def file_md5(path: Path) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()
def load_cv(path: Path):
    return cv2.imread(str(path))
def calc_sharpness(img):
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    return float(cv2.Laplacian(gray, cv2.CV_64F).var())
def calc_brightness(img):
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    mean = float(np.mean(gray))
    score = 1.0 - abs(mean - 135.0) / 135.0
    return max(0.0, min(1.0, score))
def calc_contrast(img):
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    std = float(np.std(gray))
    return max(0.0, min(1.0, std / 60.0))
def calc_exposure_penalty(img):
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    dark = float(np.mean(gray < 20))
    bright = float(np.mean(gray > 245))
    return min(1.0, (dark + bright) * 2.0)
def calc_resolution_score(w, h):
    px = w * h
    if px >= 2_000_000:
        return 1.0
    if px >= 1_000_000:
        return 0.85
    if px >= 500_000:
        return 0.65
    return max(0.2, px / 500_000 * 0.65)
def calc_orientation_score(w, h):
    ratio = w / max(h, 1)
    if ratio >= 1.25:
        return 1.0
    if ratio >= 1.0:
        return 0.8
    if ratio >= 0.75:
        return 0.6
    return 0.4
def calc_phash(path: Path):
    with Image.open(path) as im:
        im = ImageOps.exif_transpose(im).convert("L").resize((32, 32))
        arr = np.asarray(im, dtype=np.float32)
        dct = cv2.dct(arr)
        block = dct[:8, :8]
        median = np.median(block[1:])
        return (block > median).flatten()
def hamming(a, b):
    return int(np.count_nonzero(a != b))
def analyze_photo(path: Path):
    img = load_cv(path)
    if img is None:
        return {
            "file": str(path),
            "valid": False,
            "reject": True,
            "reject_reasons": ["opencv_read_failed"]
        }
    h, w = img.shape[:2]
    sharpness = calc_sharpness(img)
    brightness = calc_brightness(img)
    contrast = calc_contrast(img)
    exposure = calc_exposure_penalty(img)
    resolution = calc_resolution_score(w, h)
    orientation = calc_orientation_score(w, h)
    sharpness_norm = min(sharpness / 500.0, 1.0)
    score = (
        sharpness_norm * 0.32
        + brightness * 0.18
        + contrast * 0.15
        + resolution * 0.20
        + orientation * 0.15
        - exposure * 0.35
    )
    reject_reasons = []
    if sharpness < 40:
        reject_reasons.append("too_blurry")
    if brightness < 0.25:
        reject_reasons.append("bad_brightness")
    if exposure > 0.45:
        reject_reasons.append("bad_exposure")
    if w < 500 or h < 500:
        reject_reasons.append("too_small")
    return {
        "file": str(path),
        "valid": True,
        "width": w,
        "height": h,
        "ratio": round(w / max(h, 1), 3),
        "sharpness": round(sharpness, 2),
        "brightness_score": round(brightness, 3),
        "contrast_score": round(contrast, 3),
        "resolution_score": round(resolution, 3),
        "orientation_score": round(orientation, 3),
        "exposure_penalty": round(exposure, 3),
        "score": round(score, 4),
        "reject": bool(reject_reasons),
        "reject_reasons": reject_reasons,
    }
def deduplicate(items, phash_threshold=6):
    kept = []
    duplicates = []
    md5_seen = {}
    phash_seen = []
    for item in items:
        path = Path(item["file"])
        try:
            md5 = file_md5(path)
        except Exception:
            md5 = None
        if md5 and md5 in md5_seen:
            duplicates.append({
                "file": str(path),
                "duplicate_of": md5_seen[md5],
                "type": "exact"
            })
            continue
        try:
            ph = calc_phash(path)
        except Exception:
            ph = None
        duplicate_of = None
        if ph is not None:
            for old_ph, old_file in phash_seen:
                if hamming(ph, old_ph) <= phash_threshold:
                    duplicate_of = old_file
                    break
        if duplicate_of:
            duplicates.append({
                "file": str(path),
                "duplicate_of": duplicate_of,
                "type": "perceptual"
            })
            continue
        kept.append(item)
        if md5:
            md5_seen[md5] = str(path)
        if ph is not None:
            phash_seen.append((ph, str(path)))
    return kept, duplicates
def rank_photos(folder):
    folder = Path(folder)
    files = sorted([
        p for p in folder.rglob("*")
        if p.suffix.lower() in IMAGE_EXTS
    ])
    analyzed = []
    for path in files:
        result = analyze_photo(path)
        if result.get("valid"):
            analyzed.append(result)
    clean, duplicates = deduplicate(analyzed)
    clean.sort(
        key=lambda x: (
            x["reject"],
            -x["score"]
        )
    )
    return {
        "ranked": clean,
        "duplicates": duplicates
    }

def pick_cover_candidate(ranked_result):
    """
    Cover candidate rule (no room ML):
    1. Prefer landscape (ratio >= 1.10) among non-reject
    2. Skip reject (sharpness/exposure/resolution already in reject flags)
    3. Else highest score among non-reject
    4. Fallback first ranked

    Note: bathroom/detail still possible until a semantic classifier exists.
    """
    ranked = (ranked_result or {}).get("ranked") or []
    if not ranked:
        return None

    non_reject = [x for x in ranked if not x.get("reject")]
    pool = non_reject if non_reject else ranked

    landscape = [
        x for x in pool
        if float(x.get("ratio") or 0) >= 1.10
    ]
    if landscape:
        return max(landscape, key=lambda x: float(x.get("score") or 0))

    if non_reject:
        return max(non_reject, key=lambda x: float(x.get("score") or 0))

    return ranked[0]

def get_best_cover(folder):
    result = rank_photos(folder)
    for item in result["ranked"]:
        if not item["reject"]:
            return item
    if result["ranked"]:
        return result["ranked"][0]
    return None
