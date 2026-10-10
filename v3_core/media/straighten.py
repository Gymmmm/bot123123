"""Bounded geometric correction for cover heroes (roll + vertical keystone).

Detect near-vertical building edges (OpenCV LSD), fit their lean as a linear
function of x (lean = p + q*x), then warp so walls stand vertical:

* roll (p) is corrected fully, clamped to ±8°;
* vertical keystone (q) is corrected at 85% — a full correction stretches the
  top of the frame and looks unnatural;
* the result is cropped inward so no empty (black) corners remain.

Bounds (Gym spec §四): interior shots get a light roll-only fix (±3°, no
keystone); photos that are already level are left untouched; low resolution,
severe up-tilt or a crop that would cut the subject are refused; and every
accepted warp is measured before/after (local aspect change of the transform,
crop kept, residual lean). Exceeding a limit reverts to the original photo.

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
MIN_CROP_KEPT = 0.7
INTERIOR_MAX_ROLL_DEG = 3.0
LEVEL_LEAN_DEG = 0.8          # already upright: never warp
SEVERE_LEAN_DEG = 15.0        # strong up-shot / fisheye: refuse, pick another photo
MIN_SHORT_SIDE = 600
MAX_ASPECT_CHANGE = 0.10      # max local |sx/sy - 1| over the central subject (20–80%)
MAX_EDGE_ASPECT_CHANGE = 0.25 # same, over the whole kept crop (corners)
KEYSTONE_STEPS = (KEYSTONE_STRENGTH, 0.7, 0.55, 0.4)  # back off until within bounds


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


def _aspect_change(M: "np.ndarray", box: tuple[int, int, int, int], H: int, W: int) -> float:
    """Max local anisotropy |sx/sy - 1| of the warp over the kept region (output space)."""
    Minv = np.linalg.inv(M)
    left, top, right, bottom = box
    worst = 0.0
    for fy in np.linspace(0.0, 1.0, 5):
        for fx in np.linspace(0.0, 1.0, 5):
            x, y = left + fx * (right - left), top + fy * (bottom - top)
            pts = np.float32([[[x, y], [x + 1.0, y], [x, y + 1.0]]])
            src = cv2.perspectiveTransform(pts, Minv)[0]
            sx = 1.0 / max(1e-6, float(np.hypot(*(src[1] - src[0]))))
            sy = 1.0 / max(1e-6, float(np.hypot(*(src[2] - src[0]))))
            worst = max(worst, abs(sx / sy - 1.0))
    return worst


def assess(img: "np.ndarray") -> dict[str, Any] | None:
    """Tilt of the photo without changing it (None when there is no usable line evidence)."""
    if cv2 is None or img is None or img.ndim != 3:
        return None
    H, W = img.shape[:2]
    segments = _vertical_segments(cv2.cvtColor(img, cv2.COLOR_BGR2GRAY))
    if len(segments) < MIN_SEGMENTS:
        return None
    p, q, inliers = _fit_lean(segments, W)
    if inliers < MIN_SEGMENTS:
        return None
    before = math.degrees(math.atan(float(np.average([abs(s[2]) for s in segments], weights=[s[3] for s in segments]))))
    return {"roll_deg": math.degrees(math.atan(p)), "p": p, "q": q, "lean_before_deg": before,
            "segments": len(segments), "short_side": min(H, W)}


def _warp(img: "np.ndarray", p: float, qk: float):
    """Perspective warp for lean p + qk*x, cropped inward until no empty pixels remain."""
    H, W = img.shape[:2]
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
            kept = (right - left) * (bottom - top) / float(W * H)
            return warped, M, (left, top, right, bottom), kept
        left, top, right, bottom = left + 3, top + 3, right - 3, bottom - 3
    return None


def correct_array(img: "np.ndarray", *, mode: str = "exterior") -> tuple[str, "np.ndarray | None", dict[str, Any]]:
    """Return (status, corrected | None, metrics).

    status: ``corrected`` | ``level`` (already upright, untouched) | ``no_lines`` |
    ``refused:<reason>`` (low_resolution / severe_tilt / would_crop_subject) |
    ``reverted:<reason>`` (aspect_change / no_improvement). Only ``corrected``
    carries an image; every other status means "use the original photo".
    """
    if cv2 is None or img is None or img.ndim != 3:
        return "no_lines", None, {}
    H, W = img.shape[:2]
    if min(H, W) < MIN_SHORT_SIDE:
        return "refused:low_resolution", None, {"short_side": min(H, W)}
    info = assess(img)
    if info is None:
        return "no_lines", None, {}
    metrics: dict[str, Any] = {"lean_before_deg": round(info["lean_before_deg"], 2), "roll_before_deg": round(info["roll_deg"], 2)}
    if info["lean_before_deg"] > SEVERE_LEAN_DEG:
        return "refused:severe_tilt", None, metrics
    interior = mode != "exterior"
    max_roll = INTERIOR_MAX_ROLL_DEG if interior else MAX_ROLL_DEG
    p, q = info["p"], (0.0 if interior else info["q"])
    if info["lean_before_deg"] < LEVEL_LEAN_DEG and abs(info["roll_deg"]) < LEVEL_LEAN_DEG / 2:
        return "level", None, metrics
    roll = math.degrees(math.atan(p))
    if abs(roll) > max_roll:
        p = math.tan(math.radians(math.copysign(max_roll, roll)))
    last_reason = "reverted:aspect_change"
    for strength in (KEYSTONE_STEPS if q else (0.0,)):
        result = _warp(img, p, q * strength)
        if result is None:
            return "refused:would_crop_subject", None, metrics
        warped, M, (left, top, right, bottom), kept = result
        width, height = right - left, bottom - top
        centre = (left + int(0.2 * width), top + int(0.2 * height), left + int(0.8 * width), top + int(0.8 * height))
        metrics.update({
            "roll_deg": round(math.degrees(math.atan(p)), 2),
            "keystone": round(q * strength, 4),
            "keystone_strength": strength,
            "crop_kept": round(kept, 3),
            "aspect_change": round(_aspect_change(M, centre, H, W), 4),
            "edge_aspect_change": round(_aspect_change(M, (left, top, right, bottom), H, W), 4),
        })
        if kept < MIN_CROP_KEPT:
            return "refused:would_crop_subject", None, metrics
        if metrics["aspect_change"] <= MAX_ASPECT_CHANGE and metrics["edge_aspect_change"] <= MAX_EDGE_ASPECT_CHANGE:
            break
    else:
        return last_reason, None, metrics
    out = warped[top:bottom, left:right]
    after = mean_abs_lean_deg(cv2.cvtColor(out, cv2.COLOR_BGR2GRAY))
    metrics["lean_after_deg"] = None if after is None else round(after, 2)
    if after is not None and after > info["lean_before_deg"] - 0.2:
        return "reverted:no_improvement", None, metrics
    return "corrected", out, metrics


def straighten_array(img: "np.ndarray", *, mode: str = "exterior") -> tuple["np.ndarray", dict[str, Any]] | None:
    status, out, metrics = correct_array(img, mode=mode)
    if status != "corrected" or out is None:
        return None
    return out, metrics


def correct_file(source: str | Path, output: str | Path, *, mode: str = "exterior") -> dict[str, Any]:
    """Always returns a report; ``report["path"]`` is set only when a corrected copy was written."""
    report: dict[str, Any] = {"status": "no_lines", "mode": mode}
    try:
        if cv2 is None:
            return {**report, "status": "unavailable"}
        img = cv2.imread(str(source), cv2.IMREAD_COLOR)
        if img is None:
            return {**report, "status": "unreadable"}
        status, out, metrics = correct_array(img, mode=mode)
        report.update(metrics)
        report["status"] = status
        if status == "corrected" and out is not None:
            output = Path(output)
            output.parent.mkdir(parents=True, exist_ok=True)
            if cv2.imwrite(str(output), out, [cv2.IMWRITE_JPEG_QUALITY, 95]):
                report["path"] = str(output)
            else:
                report["status"] = "write_failed"
        return report
    except Exception as exc:  # pragma: no cover - defensive
        return {**report, "status": f"error:{type(exc).__name__}"}


def straighten_file(source: str | Path, output: str | Path, *, mode: str = "exterior") -> dict[str, Any] | None:
    """Write a corrected copy of ``source``; ``None`` means keep the original."""
    report = correct_file(source, output, mode=mode)
    return report if report.get("path") else None


__all__ = ["assess", "correct_array", "correct_file", "mean_abs_lean_deg", "straighten_array", "straighten_file"]
