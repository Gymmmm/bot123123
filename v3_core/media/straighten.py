"""Auto-straighten exterior hero photos (roll + vertical keystone).

Detect near-vertical building edges (OpenCV LSD), fit their lean as a linear
function of x (lean = p + q*x), then warp so walls stand vertical:

* roll (p) is corrected fully, clamped to ±8°;
* vertical keystone (q) is corrected at 85% — a full correction stretches the
  top of the frame and looks unnatural;
* the result is cropped inward so no empty (black) corners remain.

OpenCV is optional. Any failure (no cv2, too few lines, unreadable image,
excessive crop) returns ``None`` and the caller keeps the original photo.
"""
from __future__ import annotations

import math
from pathlib import Path
from typing import Any

import numpy as np

try:  # pragma: no cover - exercised implicitly when OpenCV is installed
    import cv2
except Exception:  # pragma: no cover
    cv2 = None

MAX_ROLL_DEG = 8.0
KEYSTONE_STRENGTH = 0.85
MAX_LEAN_DEG = 20.0
MIN_SEGMENTS = 6
MIN_CROP_KEPT = 0.6


def _vertical_segments(gray: "np.ndarray") -> list[tuple[float, float, float, float]]:
    H, W = gray.shape
    scale = 1200.0 / max(H, W)
    small = cv2.resize(gray, None, fx=scale, fy=scale, interpolation=cv2.INTER_AREA)
    detected = cv2.createLineSegmentDetector(cv2.LSD_REFINE_STD).detect(small)[0]
    out: list[tuple[float, float, float, float]] = []
    if detected is None:
        return out
    for x1, y1, x2, y2 in detected.reshape(-1, 4) / scale:
        dx, dy = float(x2 - x1), float(y2 - y1)
        length = math.hypot(dx, dy)
        if length < 0.05 * H or abs(dy) < 1e-6:
            continue
        lean = dx / dy
        if abs(math.degrees(math.atan(lean))) > MAX_LEAN_DEG:
            continue
        out.append(((x1 + x2) / 2.0, (y1 + y2) / 2.0, lean, length))
    return out


def _fit_lean(segments, width: int) -> tuple[float, float, int]:
    xs = np.array([(s[0] - width / 2.0) / width for s in segments])
    leans = np.array([s[2] for s in segments])
    weights = np.array([s[3] for s in segments])
    keep = np.ones(len(segments), bool)
    p = q = 0.0
    for _ in range(4):
        if keep.sum() < MIN_SEGMENTS:
            break
        A = np.stack([np.ones(keep.sum()), xs[keep]], 1) * weights[keep, None]
        p, q = np.linalg.lstsq(A, leans[keep] * weights[keep], rcond=None)[0]
        residual = np.abs(leans - (p + q * xs))
        keep = residual <= max(0.02, float(np.percentile(residual[keep], 75)) * 1.5)
    return float(p), float(q), int(keep.sum())


def mean_abs_lean_deg(gray: "np.ndarray") -> float | None:
    """Length-weighted mean |lean| of near-vertical lines, in degrees."""
    if cv2 is None:
        return None
    segments = _vertical_segments(gray)
    if not segments:
        return None
    lean = float(np.average([abs(s[2]) for s in segments], weights=[s[3] for s in segments]))
    return math.degrees(math.atan(lean))


def straighten_array(img: "np.ndarray") -> tuple["np.ndarray", dict[str, Any]] | None:
    if cv2 is None or img is None or img.ndim != 3:
        return None
    H, W = img.shape[:2]
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    segments = _vertical_segments(gray)
    if len(segments) < MIN_SEGMENTS:
        return None
    p, q, inliers = _fit_lean(segments, W)
    if inliers < MIN_SEGMENTS:
        return None
    roll = math.degrees(math.atan(p))
    if abs(roll) > MAX_ROLL_DEG:
        p = math.tan(math.radians(math.copysign(MAX_ROLL_DEG, roll)))
    qk = q * KEYSTONE_STRENGTH
    src, dst = [], []
    for fx in (0.2, 0.8):
        x = fx * W
        lean = p + qk * (fx - 0.5)
        for y in (0.0, float(H)):
            src.append((x + lean * (y - H / 2.0), y))
            dst.append((x, y))
    M = cv2.getPerspectiveTransform(np.float32(src), np.float32(dst))
    warped = cv2.warpPerspective(img, M, (W, H), flags=cv2.INTER_LANCZOS4, borderMode=cv2.BORDER_CONSTANT)
    mask = cv2.warpPerspective(np.full((H, W), 255, np.uint8), M, (W, H), flags=cv2.INTER_NEAREST)
    c = cv2.perspectiveTransform(np.float32([[[0, 0], [W, 0], [W, H], [0, H]]]), M)[0]
    left = int(math.ceil(max(0.0, c[0][0], c[3][0]))) + 2
    top = int(math.ceil(max(0.0, c[0][1], c[1][1]))) + 2
    right = int(min(float(W), c[1][0], c[2][0])) - 2
    bottom = int(min(float(H), c[2][1], c[3][1])) - 2
    for _ in range(80):
        if right - left < W * 0.5 or bottom - top < H * 0.5:
            return None
        if mask[top:bottom, left:right].min() == 255:
            break
        left, top, right, bottom = left + 3, top + 3, right - 3, bottom - 3
    else:
        return None
    kept = (right - left) * (bottom - top) / float(W * H)
    if kept < MIN_CROP_KEPT:
        return None
    out = warped[top:bottom, left:right]
    before = math.degrees(math.atan(float(np.average([abs(s[2]) for s in segments], weights=[s[3] for s in segments]))))
    return out, {
        "roll_deg": round(math.degrees(math.atan(p)), 2),
        "keystone": round(qk, 4),
        "lean_before_deg": round(before, 2),
        "crop_kept": round(kept, 3),
    }


def straighten_file(source: str | Path, output: str | Path) -> dict[str, Any] | None:
    """Write a straightened copy of ``source``; ``None`` means keep the original."""
    try:
        if cv2 is None:
            return None
        img = cv2.imread(str(source), cv2.IMREAD_COLOR)
        result = straighten_array(img)
        if result is None:
            return None
        out, info = result
        output = Path(output)
        output.parent.mkdir(parents=True, exist_ok=True)
        if not cv2.imwrite(str(output), out, [cv2.IMWRITE_JPEG_QUALITY, 95]):
            return None
        info["path"] = str(output)
        return info
    except Exception:
        return None


__all__ = ["mean_abs_lean_deg", "straighten_array", "straighten_file"]
