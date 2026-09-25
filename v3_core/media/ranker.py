"""V3 media ranking core extracted from production photo_ranker.

The clean runtime path needs only path ranking plus perceptual-hash helpers.
Cover pick folds prior ``cover_selector_v1`` heuristics: prefer living / kitchen /
exterior; soft-penalize watermark/contact-heavy and toilet frames; keep blur /
overexposure hard rejects. Legacy folder helpers remain in ``photo_ranker.py``.
"""
from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, Iterable

import numpy as np
from PIL import Image, ImageOps

try:
    import cv2
except ImportError:
    cv2 = None

from .legacy_score import score_image
from .photo_formatter import IMAGE_EXTS

NEAR_DUPLICATE_HAMMING = 2
SEVERE_REJECT_SCORE = -45.0


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _dhash(path: Path, hash_size: int = 8) -> int | None:
    try:
        with Image.open(path) as source:
            image = ImageOps.exif_transpose(source).convert("L")
            image = image.resize((hash_size + 1, hash_size), Image.Resampling.LANCZOS)
            pixels = list(image.getdata())
    except Exception:
        return None
    result = 0
    bit = 0
    width = hash_size + 1
    for y in range(hash_size):
        row = y * width
        for x in range(hash_size):
            if pixels[row + x] > pixels[row + x + 1]:
                result |= 1 << bit
            bit += 1
    return result


def _hamming(a: int | None, b: int | None) -> int:
    if a is None or b is None:
        return 10_000
    return (a ^ b).bit_count()


def _read_cv(path: Path):
    if cv2 is None:
        return None
    return cv2.imread(str(path))


def _sharpness(img) -> float:
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    return min(float(cv2.Laplacian(gray, cv2.CV_64F).var()) / 4.0, 100.0)


def _brightness(img) -> float:
    value = float(cv2.cvtColor(img, cv2.COLOR_BGR2HSV)[:, :, 2].mean())
    return max(0.0, 100.0 - abs(value - 165.0) * 1.1)


def _contrast(img) -> float:
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    return min(float(gray.std()) * 2.0, 100.0)


def _resolution(img) -> float:
    h, w = img.shape[:2]
    return min((w * h) / 2_000_000 * 100.0, 100.0)


def _exposure(img) -> float:
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    total = max(1, gray.size)
    bad = (np.sum(gray < 25) + np.sum(gray > 245)) / total
    return max(0.0, 100.0 - bad * 300.0)


def _orientation(img) -> tuple[float, float]:
    h, w = img.shape[:2]
    ratio = w / max(h, 1)
    if 1.20 <= ratio <= 1.55:
        score = 100.0
    elif 1.55 < ratio <= 1.85:
        score = 94.0
    elif 1.05 <= ratio < 1.20:
        score = 88.0
    elif ratio > 1.85:
        score = 80.0
    elif 0.90 <= ratio < 1.05:
        score = 75.0
    else:
        score = 45.0
    return score, ratio


def _space(img) -> float:
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    gray = cv2.GaussianBlur(gray, (5, 5), 0)
    edges = cv2.Canny(gray, 60, 160)
    edge_ratio = np.count_nonzero(edges) / max(1, edges.size)
    return max(0.0, min(100.0, 100.0 - abs(edge_ratio - 0.10) * 600.0))


def _color(img) -> float:
    saturation = float(cv2.cvtColor(img, cv2.COLOR_BGR2HSV)[:, :, 1].mean())
    return max(0.0, 100.0 - abs(saturation - 75.0))


def _text_band_score(img) -> dict[str, float]:
    """Glyph-blob text/contact density in bottom & corners (no OCR)."""
    h, w = img.shape[:2]
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)

    def band_textiness(y0: int, y1: int, x0: int, x1: int) -> float:
        roi = gray[y0:y1, x0:x1]
        if roi.size == 0:
            return 0.0
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


def _room_cover_tier(label: object) -> int:
    """Cover preference tiers: living > exterior > kitchen > bedroom > other > toilet."""
    key = str(label or "").strip().lower()
    return {
        "living": 5,
        "exterior": 4,
        "kitchen": 3,
        "bedroom": 2,
        "toilet": -2,
    }.get(key, 1)


def _room_heuristic(img) -> dict[str, Any]:
    """Cheap room-type hints: prefer living / kitchen / exterior over toilet/bedroom.

    Color/structure heuristics only — not a trained classifier. Beds are explicitly
    demoted so bedroom shots stop winning the "living" cover slot.
    """
    h, w = img.shape[:2]
    hsv = cv2.cvtColor(img, cv2.COLOR_BGR2HSV)
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    rgb = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)

    bright = float(np.mean(gray > 210))
    very_white = float(np.mean((rgb[:, :, 0] > 200) & (rgb[:, :, 1] > 200) & (rgb[:, :, 2] > 200)))
    lower_white = float(np.mean(gray[int(h * 0.45) :, :] > 205))
    toilet_score = min(1.0, bright * 0.4 + very_white * 0.4 + lower_white * 0.6)

    warm = (
        (hsv[:, :, 0] < 35)
        & (hsv[:, :, 1] > 40)
        & (hsv[:, :, 2] > 60)
        & (hsv[:, :, 2] < 220)
    )
    warm_frac = float(np.mean(warm))
    mid_warm = float(np.mean(warm[int(h * 0.28) : int(h * 0.88), :]))
    edges = cv2.Canny(gray, 50, 120)
    mid = edges[int(h * 0.25) : int(h * 0.75), :]
    sobely = cv2.Sobel(gray, cv2.CV_32F, 0, 1, ksize=3)
    sobelx = cv2.Sobel(gray, cv2.CV_32F, 1, 0, ksize=3)
    horiz = float(np.mean(np.abs(sobely) > 40))
    kitchen_score = min(1.0, warm_frac * 1.5 + horiz * 0.8 + float(mid.mean() / 255) * 0.5)

    top = hsv[: int(h * 0.28), :, :]
    blue = (top[:, :, 0] > 90) & (top[:, :, 0] < 135) & (top[:, :, 1] > 40) & (top[:, :, 2] > 80)
    blue_frac = float(np.mean(blue))
    sat_mean = float(hsv[:, :, 1].mean())
    exterior_score = min(1.0, blue_frac * 2.2 + (sat_mean / 120.0) * 0.25)

    # Bed cue: soft mid-lower bedding + strong horizontal headboard edge.
    bed_band = gray[int(h * 0.38) : int(h * 0.82), int(w * 0.12) : int(w * 0.88)]
    bed_horiz = float(np.mean(np.abs(sobely[int(h * 0.28) : int(h * 0.72), :]) > 35))
    bed_soft = float(np.mean((bed_band > 70) & (bed_band < 210))) if bed_band.size else 0.0
    bed_flat = 1.0 - min(1.0, float(bed_band.std() / 55.0)) if bed_band.size else 0.0
    side_vert = float(np.mean(np.abs(sobelx[:, : int(w * 0.22)]) > 40)) + float(
        np.mean(np.abs(sobelx[:, int(w * 0.78) :]) > 40)
    )
    bed_score = min(
        1.0,
        bed_soft * 0.45 + bed_horiz * 0.85 + bed_flat * 0.35 + min(side_vert, 0.8) * 0.25,
    )

    edge_d = float(edges.mean() / 255.0)
    mean_b = float(gray.mean())
    living_score = 0.0
    if 55 < mean_b < 190 and 0.02 < edge_d < 0.18 and toilet_score < 0.55:
        living_score = max(
            0.0,
            min(
                1.0,
                0.35
                + (1.0 - abs(mean_b - 125) / 125.0) * 0.35
                + min(edge_d / 0.08, 1.0) * 0.3
                - toilet_score * 0.4
                - exterior_score * 0.15,
            ),
        )
    if mid_warm > 0.07 and toilet_score < 0.48 and exterior_score < 0.55:
        living_score = max(
            living_score,
            min(1.0, 0.42 + mid_warm * 1.35 - toilet_score * 0.55),
        )

    bedroom_score = living_score * 0.55
    if sat_mean < 55 and living_score > 0.25:
        bedroom_score = min(1.0, bedroom_score + 0.1)
    bedroom_score = max(bedroom_score, bed_score)
    if bed_score >= 0.42:
        living_score *= 0.25
        bedroom_score = max(bedroom_score, min(1.0, 0.55 + bed_score * 0.45))

    scores: dict[str, Any] = {
        "toilet": round(float(toilet_score), 3),
        "kitchen": round(float(kitchen_score), 3),
        "exterior": round(float(exterior_score), 3),
        "living": round(float(living_score), 3),
        "bedroom": round(float(max(0.0, bedroom_score)), 3),
        "bed": round(float(bed_score), 3),
    }
    preferred = max(
        [
            ("living", scores["living"]),
            ("kitchen", scores["kitchen"]),
            ("exterior", scores["exterior"]),
            ("bedroom", scores["bedroom"]),
        ],
        key=lambda x: x[1],
    )
    label = preferred[0]
    if scores["toilet"] > preferred[1] + 0.08 and scores["toilet"] > 0.45:
        label = "toilet"
    if bed_score >= 0.48 and scores["bedroom"] >= scores["living"]:
        label = "bedroom"
    scores["label"] = label
    return scores


def _cover_room_bonus(room: dict[str, Any]) -> float:
    label = str(room.get("label") or "")
    if label == "living":
        return 14.0 + 6.0 * float(room.get("living") or 0)
    if label == "exterior":
        return 10.0 + 5.0 * float(room.get("exterior") or 0)
    if label == "kitchen":
        return 7.0 + 4.0 * float(room.get("kitchen") or 0)
    if label == "bedroom":
        # Bedroom is usable gallery material but a weak channel-cover lead.
        return -2.0 + 1.5 * float(room.get("bedroom") or 0) - 8.0 * float(room.get("bed") or 0)
    if label == "toilet":
        return -24.0 - 14.0 * float(room.get("toilet") or 0)
    return 0.0


def _cover_text_penalty(text: dict[str, float]) -> tuple[float, bool]:
    """Bottom/contact bands are especially toxic for channel covers."""
    bottom = float(text.get("bottom_text") or 0)
    heavy = float(text.get("text_heavy") or 0)
    penalty = heavy * 24.0 + bottom * 12.0
    soft = heavy >= 0.48 or bottom >= 0.52
    return penalty, soft



def _cv_metrics(path: Path) -> dict[str, Any] | None:
    img = _read_cv(path)
    if img is None:
        return None
    h, w = img.shape[:2]
    sharpness = _sharpness(img)
    brightness = _brightness(img)
    contrast = _contrast(img)
    resolution = _resolution(img)
    exposure = _exposure(img)
    orientation, ratio = _orientation(img)
    space = _space(img)
    color = _color(img)

    rejected = False
    reason = ""
    if w < 700 or h < 500:
        rejected, reason = True, "low_resolution"
    elif sharpness < 18:
        rejected, reason = True, "blur"
    elif brightness < 25:
        rejected, reason = True, "bad_brightness"
    elif exposure < 45:
        rejected, reason = True, "bad_exposure"

    text = _text_band_score(img)
    room = _room_heuristic(img)
    label = str(room.get("label") or "")
    room_bonus = _cover_room_bonus(room)
    text_penalty, text_soft = _cover_text_penalty(text)
    soft_reject = bool(
        text_soft
        or (label == "toilet" and float(room.get("toilet") or 0) > 0.48)
        or (label == "bedroom" and float(room.get("bed") or 0) >= 0.55)
    )

    total = (
        sharpness * 0.18
        + brightness * 0.10
        + contrast * 0.07
        + resolution * 0.07
        + exposure * 0.12
        + orientation * 0.18
        + space * 0.10
        + color * 0.05
        + room_bonus
        - text_penalty
    )
    if ratio < 0.90:
        total -= 22
    if ratio > 2.0:
        total -= 8
    if soft_reject:
        total -= 16
    if label == "toilet":
        total -= 10
    if rejected:
        total -= 50

    return {
        "score": round(max(total, 0.0), 2),
        "reject": rejected,
        "reason": reason,
        "soft_reject": soft_reject,
        "room_label": label,
        "room": room,
        "text": text,
        "room_bonus": round(room_bonus, 2),
        "text_penalty": round(text_penalty, 2),
        "room_tier": _room_cover_tier(label),
        "sharpness": round(sharpness, 2),
        "brightness": round(brightness, 2),
        "contrast": round(contrast, 2),
        "resolution": round(resolution, 2),
        "exposure": round(exposure, 2),
        "orientation": round(orientation, 2),
        "space": round(space, 2),
        "color": round(color, 2),
        "ratio": round(ratio, 3),
    }


def _score_one(path: Path, source_order: int) -> dict[str, Any]:
    metrics = _cv_metrics(path)
    if metrics is None:
        legacy_score, legacy_reason = score_image(str(path))
        reject = legacy_score <= -900 or legacy_score < SEVERE_REJECT_SCORE
        try:
            with Image.open(path) as source:
                w, h = ImageOps.exif_transpose(source).size
        except Exception:
            w, h = 0, 1
        ratio = w / max(h, 1)
        metrics = {
            "score": float(legacy_score),
            "reject": bool(reject),
            "reason": legacy_reason,
            "soft_reject": False,
            "room_label": "",
            "room_tier": _room_cover_tier(""),
            "sharpness": 0.0,
            "brightness": 0.0,
            "contrast": 0.0,
            "resolution": 0.0,
            "exposure": 0.0,
            "orientation": 100.0 if 1.20 <= ratio <= 1.55 else (88.0 if ratio >= 1.05 else 45.0),
            "space": 0.0,
            "color": 0.0,
            "ratio": round(ratio, 3),
        }
    return {
        "file": str(path),
        "path": str(path),
        "filename": path.name,
        "source_order": source_order,
        **metrics,
    }


def rank_photo_paths(paths: Iterable[str | Path]) -> list[dict[str, Any]]:
    ranked: list[dict[str, Any]] = []
    for index, raw in enumerate(paths, start=1):
        path = Path(raw).resolve()
        if not path.is_file() or path.suffix.lower() not in IMAGE_EXTS:
            continue
        ranked.append(_score_one(path, index))

    def sort_key(item: dict[str, Any]) -> tuple:
        ratio = float(item.get("ratio") or 0)
        landscape = ratio >= 1.05
        tier = int(item.get("room_tier") or _room_cover_tier(item.get("room_label")))
        return (
            0 if item.get("reject") else 1,
            0 if item.get("soft_reject") else 1,
            tier,
            1 if landscape else 0,
            float(item.get("score") or 0),
            float(item.get("orientation") or 0),
            float(item.get("sharpness") or 0),
            float(item.get("exposure") or 0),
            float(item.get("resolution") or 0),
            -int(item.get("source_order") or 0),
        )

    ranked.sort(key=sort_key, reverse=True)
    return ranked


__all__ = [
    "NEAR_DUPLICATE_HAMMING",
    "SEVERE_REJECT_SCORE",
    "_dhash",
    "_hamming",
    "_room_cover_tier",
    "rank_photo_paths",
]
