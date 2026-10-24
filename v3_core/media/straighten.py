"""Bounded geometric correction for cover heroes (roll + vertical keystone).

Detect near-vertical building edges (OpenCV LSD), fit their lean as a linear
function of x (lean = p + q*x), then warp so walls stand vertical:

* roll (p) is corrected fully, clamped to ±8°;
* vertical keystone (q) starts at 85%; exterior heroes that remain visibly
  tilted get an adaptive high-correction pass, capped at 110%;
* the result is cropped inward so no empty (black) corners remain.

Exterior heroes keep the gentle result whenever it is already straight. A close
upward shot is retried at 100% and 110% strength and the safest result with the
lowest residual lean wins. Distortion, retained crop and residual lean remain hard
guards; only a still-unusable facade falls back to the best living room.

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
EXTERIOR_HIGH_STRENGTHS = (1.0, 1.1)
MAX_LEAN_DEG = 20.0
MIN_SEGMENTS = 6
MIN_CROP_KEPT = 0.7
INTERIOR_MAX_ROLL_DEG = 3.0
LEVEL_LEAN_DEG = 0.8          # already upright: never warp
SEVERE_LEAN_DEG = 15.0        # strong up-shot / fisheye: refuse, pick another photo
MIN_SHORT_SIDE = 600
MAX_ASPECT_CHANGE = 0.10      # max local |sx/sy - 1| over the central subject (20–80%)
MAX_EDGE_ASPECT_CHANGE = 0.25 # same, over the whole kept crop (corners)
KEYSTONE_STEPS = (KEYSTONE_STRENGTH, 0.7, 0.55, 0.4)  # interior / legacy back-off

# Exterior heroes (Gym 2026-10-11, second feedback): the gentle 03:41 algorithm is the
# approved look (full tilt ±8°, 85 % keystone, crop to no black corners). Keep that
# as the default, but retry a facade at 100/110 % when the gentle pass still leans.
# The same crop and anisotropy guards prevent an obviously stretched building.
GENTLE_MIN_SEGMENTS = 4
EXTERIOR_SEGMENT_MAX_LEAN_DEG = 35.0   # residual measurement only: converging columns count as evidence
GUARD_ASPECT_CHANGE = 0.25             # guard against clearly broken warps (centre of the building)
GUARD_EDGE_ASPECT_CHANGE = 0.40        # same, worst corner of the kept crop
HERO_MAX_RESIDUAL_DEG = 3.5            # still leaning after the gentle fix → not the hero
SEVERE_UPSHOT_EDGE_DEG = 20.0          # verticals at the frame edge lean > 20° = close upward shot


def _vertical_segments(gray: "np.ndarray", max_lean_deg: float | None = None) -> list[tuple[float, float, float, float]]:
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
        if abs(math.degrees(math.atan(lean))) > (MAX_LEAN_DEG if max_lean_deg is None else max_lean_deg):
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


def _keystone_matrix(W: int, H: int, p: float, qk: float) -> "np.ndarray":
    src, dst = [], []
    for fx in (0.2, 0.8):
        x = fx * W
        lean = p + qk * (fx - 0.5)
        for y in (0.0, float(H)):
            src.append((x + lean * (y - H / 2.0), y))
            dst.append((x, y))
    return cv2.getPerspectiveTransform(np.float32(src), np.float32(dst))


def _warp(img: "np.ndarray", p: float, qk: float, vscale: float = 1.0):
    """Perspective warp for lean p + qk*x (optionally stretched vertically by ``vscale``),
    cropped inward until no empty pixels remain."""
    H, W = img.shape[:2]
    M = _keystone_matrix(W, H, p, qk)
    if vscale != 1.0:
        M = np.array([[1.0, 0.0, 0.0], [0.0, vscale, 0.0], [0.0, 0.0, 1.0]]) @ M
    Hc = int(round(H * vscale))
    warped = cv2.warpPerspective(img, M, (W, Hc), flags=cv2.INTER_LANCZOS4, borderMode=cv2.BORDER_CONSTANT)
    mask = cv2.warpPerspective(np.full((H, W), 255, np.uint8), M, (W, Hc), flags=cv2.INTER_NEAREST)
    c = cv2.perspectiveTransform(np.float32([[[0, 0], [W, 0], [W, H], [0, H]]]), M)[0]
    left = int(math.ceil(max(0.0, c[0][0], c[3][0]))) + 2
    top = int(math.ceil(max(0.0, c[0][1], c[1][1]))) + 2
    right = int(min(float(W), c[1][0], c[2][0])) - 2
    bottom = int(min(float(Hc), c[2][1], c[3][1])) - 2
    step = max(3, int(min(W, Hc) * 0.004))
    for _ in range(200):
        if right - left < W * 0.5 or bottom - top < Hc * 0.4:
            return None
        if mask[top:bottom, left:right].min() == 255:
            kept = (right - left) * (bottom - top) / float(W * Hc)
            return warped, M, (left, top, right, bottom), kept
        # shrink only the sides that still touch empty pixels
        moved = False
        if mask[top:bottom, left].min() < 255:
            left += step; moved = True
        if mask[top:bottom, right - 1].min() < 255:
            right -= step; moved = True
        if mask[top, left:right].min() < 255:
            top += step; moved = True
        if mask[bottom - 1, left:right].min() < 255:
            bottom -= step; moved = True
        if not moved:
            left, top, right, bottom = left + step, top + step, right - step, bottom - step
    return None


def residual_lean_deg(img: "np.ndarray") -> float | None:
    """Fitted vertical lean left at the building edges (max |lean| at x = ±40 %), degrees."""
    if cv2 is None:
        return None
    H, W = img.shape[:2]
    segments = _vertical_segments(cv2.cvtColor(img, cv2.COLOR_BGR2GRAY), EXTERIOR_SEGMENT_MAX_LEAN_DEG)
    if len(segments) < MIN_SEGMENTS:
        return None
    p, q, inliers = _fit_lean(segments, W)
    if inliers < MIN_SEGMENTS:
        return None
    return max(abs(math.degrees(math.atan(p + q * x))) for x in (-0.4, 0.0, 0.4))


def _gentle_fit(segments, W: int) -> tuple[float, float, int]:
    """Robust weighted fit lean(x) = p + q*(x - W/2)/W (Gym-approved 03:41 version, verbatim)."""
    if len(segments) < GENTLE_MIN_SEGMENTS:
        return 0.0, 0.0, len(segments)
    xs = np.array([(s[0] - W / 2) / W for s in segments])
    ls = np.array([s[2] for s in segments])
    w = np.array([s[3] for s in segments])
    keep = np.ones(len(segments), bool)
    p = q = 0.0
    for _ in range(4):
        A = np.stack([np.ones(keep.sum()), xs[keep]], 1) * w[keep, None]
        p, q = np.linalg.lstsq(A, ls[keep] * w[keep], rcond=None)[0]
        res = np.abs(ls - (p + q * xs))
        thr = max(0.02, np.percentile(res[keep], 75) * 1.5)
        keep = res <= thr
        if keep.sum() < GENTLE_MIN_SEGMENTS:
            break
    return float(p), float(q), int(keep.sum())


def _gentle_warp(img: "np.ndarray", p: float, qk: float):
    """Trapezoid keystone through x = 20 % / 80 %, then crop inward (3 px steps, ≤ 60) until
    no empty pixel remains. Returns (crop, M, box, kept) or None when corners stay empty."""
    H, W = img.shape[:2]
    M = _keystone_matrix(W, H, p, qk)
    warped = cv2.warpPerspective(img, M, (W, H), flags=cv2.INTER_LANCZOS4, borderMode=cv2.BORDER_CONSTANT)
    mask = cv2.warpPerspective(np.full((H, W), 255, np.uint8), M, (W, H), flags=cv2.INTER_NEAREST)
    c = cv2.perspectiveTransform(np.float32([[[0, 0], [W, 0], [W, H], [0, H]]]), M)[0]
    left, right = max(c[0][0], c[3][0]), min(c[1][0], c[2][0])
    top, bottom = max(c[0][1], c[1][1]), min(c[2][1], c[3][1])
    left, top = int(math.ceil(max(0.0, left))) + 2, int(math.ceil(max(0.0, top))) + 2
    right, bottom = int(min(W, right)) - 2, int(min(H, bottom)) - 2
    for _ in range(60):
        if right - left < 8 or bottom - top < 8:
            return None
        if mask[top:bottom, left:right].min() == 255:
            break
        left += 3; right -= 3; top += 3; bottom -= 3
    else:
        return None
    if mask[top:bottom, left:right].min() < 255:
        return None
    kept = (right - left) * (bottom - top) / float(W * H)
    return warped[top:bottom, left:right], M, (left, top, right, bottom), kept


def _correct_exterior(img: "np.ndarray") -> tuple[str, "np.ndarray | None", dict[str, Any]]:
    """Adaptive exterior correction with a gentle-first, strong-when-needed policy.

    The 85 % pass remains the default. If it still exceeds the hero residual limit,
    try 100 % and 110 % and retain the safe candidate with the lowest residual lean.
    """
    H, W = img.shape[:2]
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    segments = _vertical_segments(gray)                       # |lean| ≤ 20°, as in the approved script
    p, q, inliers = _gentle_fit(segments, W)
    if len(segments) < GENTLE_MIN_SEGMENTS or inliers < GENTLE_MIN_SEGMENTS:
        return "no_lines", None, {}
    raw = float(np.average([abs(s[2]) for s in segments], weights=[s[3] for s in segments]))
    edge_lean = max(abs(math.degrees(math.atan(p + q * x))) for x in (-0.5, 0.5))
    before = residual_lean_deg(img)
    metrics: dict[str, Any] = {
        "method": "gentle", "lean_before_deg": round(math.degrees(math.atan(raw)), 2),
        "roll_before_deg": round(math.degrees(math.atan(p)), 2),
        "edge_lean_before_deg": round(edge_lean, 2),
        "residual_before_deg": None if before is None else round(before, 2),
    }
    severe = edge_lean > SEVERE_UPSHOT_EDGE_DEG
    metrics["severe_upshot"] = severe
    if (before is not None and before < LEVEL_LEAN_DEG) and abs(metrics["roll_before_deg"]) < LEVEL_LEAN_DEG / 2:
        metrics.update(residual_after_deg=metrics["residual_before_deg"], hero_ok=not severe)
        return "level", None, metrics
    roll = math.degrees(math.atan(p))
    if abs(roll) > MAX_ROLL_DEG:
        p = math.tan(math.radians(math.copysign(MAX_ROLL_DEG, roll)))
    strengths = (KEYSTONE_STRENGTH, *EXTERIOR_HIGH_STRENGTHS)
    candidates: list[tuple[float, "np.ndarray", dict[str, Any]]] = []
    last_metrics = dict(metrics)
    rejected_crop = rejected_aspect = rejected_improvement = False
    for strength in strengths:
        qk = q * strength
        result = _gentle_warp(img, p, qk)
        if result is None:
            continue
        out, M, (left, top, right, bottom), kept = result
        width, height = right - left, bottom - top
        centre = (left + int(0.2 * width), top + int(0.2 * height),
                  left + int(0.8 * width), top + int(0.8 * height))
        after = residual_lean_deg(out)
        lean_after = mean_abs_lean_deg(cv2.cvtColor(out, cv2.COLOR_BGR2GRAY))
        trial = {
            **metrics,
            "roll_deg": round(math.degrees(math.atan(p)), 2), "keystone_q": round(q, 4),
            "keystone": round(qk, 4), "keystone_strength": strength,
            "correction_level": "gentle" if strength == KEYSTONE_STRENGTH else "high",
            "crop_kept": round(kept, 3), "output_size": [width, height],
            "aspect_change": round(_aspect_change(M, centre, H, W), 4),
            "edge_aspect_change": round(_aspect_change(M, (left, top, right, bottom), H, W), 4),
            "residual_after_deg": None if after is None else round(after, 2),
            "lean_after_deg": None if lean_after is None else round(lean_after, 2),
        }
        last_metrics = trial
        rejected_crop |= kept < MIN_CROP_KEPT
        rejected_aspect |= (trial["aspect_change"] > GUARD_ASPECT_CHANGE
                            or trial["edge_aspect_change"] > GUARD_EDGE_ASPECT_CHANGE)
        rejected_improvement |= lean_after is not None and lean_after > metrics["lean_before_deg"] - 0.2
        safe = (kept >= MIN_CROP_KEPT
                and trial["aspect_change"] <= GUARD_ASPECT_CHANGE
                and trial["edge_aspect_change"] <= GUARD_EDGE_ASPECT_CHANGE
                and (lean_after is None or lean_after <= metrics["lean_before_deg"] - 0.2))
        if safe:
            residual = after if after is not None else before
            candidates.append((float("inf") if residual is None else residual, out, trial))
            if strength == KEYSTONE_STRENGTH and (residual is None or residual <= HERO_MAX_RESIDUAL_DEG) and not severe:
                break
    if not candidates:
        last_metrics["hero_ok"] = False
        if rejected_aspect:
            return "reverted:aspect_change", None, last_metrics
        if rejected_crop:
            return "reverted:crop", None, last_metrics
        if rejected_improvement:
            return "reverted:no_improvement", None, last_metrics
        return "refused:would_crop_subject", None, last_metrics
    residual, out, metrics = min(candidates, key=lambda item: item[0])
    metrics["hero_ok"] = residual <= HERO_MAX_RESIDUAL_DEG
    return "corrected", out, metrics


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
    if mode == "exterior":
        return _correct_exterior(img)
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


__all__ = ["assess", "correct_array", "correct_file", "mean_abs_lean_deg", "residual_lean_deg", "straighten_array",
           "straighten_file"]
