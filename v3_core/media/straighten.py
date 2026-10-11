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
KEYSTONE_STEPS = (KEYSTONE_STRENGTH, 0.7, 0.55, 0.4)  # interior / legacy back-off

# Exterior heroes (Gym 2026-10-11 villa feedback): rotate the virtual camera level so
# verticals end ≤ ~1° (full correction, no 'severe tilt → skip'); the foreshortened
# height is restored by the same homography. Distortion is capped by the building
# aspect change; over the cap the largest allowed partial rotation is applied.
EXTERIOR_SEGMENT_MAX_LEAN_DEG = 35.0  # strongly converging columns are evidence, not noise
EXTERIOR_MAX_EDGE_LEAN_DEG = 28.0     # per-side lean at the frame edge corrected fully; beyond → partial
EXTERIOR_MAX_ASPECT_CHANGE = 0.30     # building centre: at most 30 % taller/thinner than the source pixels
EXTERIOR_MAX_EDGE_ASPECT_CHANGE = 0.45 # worst corner of the kept frame
EXTERIOR_MIN_CROP_KEPT = 0.6
EXTERIOR_FOCAL = 0.6                   # assumed focal length / image width (wide phone lens; upshots are wide)
EXTERIOR_STRENGTHS = (1.0, 0.92, 0.84, 0.76, 0.68, 0.6, 0.52, 0.44, 0.36)  # max allowed partial
TARGET_RESIDUAL_DEG = 1.0


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


def _anisotropy(M: "np.ndarray", box: tuple[int, int, int, int]) -> list[float]:
    """sx/sy of the warp sampled over ``box`` (output space)."""
    Minv = np.linalg.inv(M)
    left, top, right, bottom = box
    ratios = []
    for fy in np.linspace(0.0, 1.0, 5):
        for fx in np.linspace(0.0, 1.0, 5):
            x, y = left + fx * (right - left), top + fy * (bottom - top)
            pts = np.float32([[[x, y], [x + 1.0, y], [x, y + 1.0]]])
            src = cv2.perspectiveTransform(pts, Minv)[0]
            sx = 1.0 / max(1e-6, float(np.hypot(*(src[1] - src[0]))))
            sy = 1.0 / max(1e-6, float(np.hypot(*(src[2] - src[0]))))
            ratios.append(sx / sy)
    return ratios


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


def _rotation_to(a: "np.ndarray", b: "np.ndarray") -> "np.ndarray":
    a, b = a / np.linalg.norm(a), b / np.linalg.norm(b)
    v, c = np.cross(a, b), float(a @ b)
    if np.linalg.norm(v) < 1e-9:
        return np.eye(3)
    vx = np.array([[0, -v[2], v[1]], [v[2], 0, -v[0]], [-v[1], v[0], 0]])
    return np.eye(3) + vx + vx @ vx * (1.0 / (1.0 + c))


def _axis_angle(R: "np.ndarray") -> tuple["np.ndarray", float]:
    angle = math.acos(max(-1.0, min(1.0, (float(np.trace(R)) - 1.0) / 2.0)))
    if angle < 1e-9:
        return np.array([1.0, 0.0, 0.0]), 0.0
    axis = np.array([R[2, 1] - R[1, 2], R[0, 2] - R[2, 0], R[1, 0] - R[0, 1]]) / (2.0 * math.sin(angle))
    return axis, angle


def _rodrigues(axis: "np.ndarray", angle: float) -> "np.ndarray":
    axis = axis / max(1e-12, np.linalg.norm(axis))
    vx = np.array([[0, -axis[2], axis[1]], [axis[2], 0, -axis[0]], [-axis[1], axis[0], 0]])
    return np.eye(3) + math.sin(angle) * vx + (1 - math.cos(angle)) * vx @ vx


def _level_camera(img: "np.ndarray", p: float, q: float, strength: float):
    """Virtually rotate the camera level (vertical vanishing point → infinity).

    This is what a level camera / shift lens would have recorded: verticals become
    parallel AND the foreshortened height is restored (no squat building), unlike a
    plain trapezoid keystone. ``strength`` < 1 rotates part of the way.
    Returns (warped crop, M, kept_source_fraction, angle_deg, crop box) or None.
    """
    H, W = img.shape[:2]
    if abs(q) < 1e-6 and abs(p) < 1e-6:
        return None
    f = EXTERIOR_FOCAL * W
    K = np.array([[f, 0.0, W / 2.0], [0.0, f, H / 2.0], [0.0, 0.0, 1.0]])
    Kinv = np.linalg.inv(K)
    if abs(q) > 1e-6:
        vp = np.array([W / 2.0 - p * W / q, H / 2.0 - W / q, 1.0])
        up = Kinv @ vp
    else:  # pure roll: verticals parallel but leaning by p
        up = Kinv @ np.array([W / 2.0 + p * H, H / 2.0 + H, 1.0]) - Kinv @ np.array([W / 2.0, H / 2.0, 1.0])
        up[2] = 0.0
    if up[1] > 0:
        up = -up
    axis, angle = _axis_angle(_rotation_to(up, np.array([0.0, -1.0, 0.0])))
    angle *= strength
    Hm = K @ _rodrigues(axis, angle) @ Kinv
    corners = cv2.perspectiveTransform(np.float32([[[0, 0], [W, 0], [W, H], [0, H]]]), Hm)[0]
    top_w, bottom_w = corners[1][0] - corners[0][0], corners[2][0] - corners[3][0]
    if min(top_w, bottom_w) <= 0:
        return None
    scale = W / float(min(top_w, bottom_w))
    x0, y0 = float(corners[:, 0].min()), float(corners[:, 1].min())
    x1, y1 = float(corners[:, 0].max()), float(corners[:, 1].max())
    Wc, Hc = int(math.ceil(scale * (x1 - x0))), int(math.ceil(scale * (y1 - y0)))
    if Wc > 3 * W or Hc > 3 * H:
        return None
    M = np.array([[scale, 0.0, -scale * x0], [0.0, scale, -scale * y0], [0.0, 0.0, 1.0]]) @ Hm
    c = cv2.perspectiveTransform(np.float32([[[0, 0], [W, 0], [W, H], [0, H]]]), M)[0]
    left = int(math.ceil(max(0.0, c[0][0], c[3][0]))) + 2
    top = int(math.ceil(max(0.0, c[0][1], c[1][1]))) + 2
    right = int(min(float(Wc), c[1][0], c[2][0])) - 2
    bottom = int(min(float(Hc), c[2][1], c[3][1])) - 2
    if right - left < 0.5 * W or bottom - top < 0.4 * H:
        return None
    warped = cv2.warpPerspective(img, M, (Wc, Hc), flags=cv2.INTER_LANCZOS4, borderMode=cv2.BORDER_CONSTANT)
    mask = cv2.warpPerspective(np.full((H, W), 255, np.uint8), M, (Wc, Hc), flags=cv2.INTER_NEAREST)
    step = max(2, int(min(Wc, Hc) * 0.003))
    for _ in range(300):
        if mask[top:bottom, left:right].min() == 255:
            break
        if mask[top:bottom, left].min() < 255:
            left += step
        if mask[top:bottom, right - 1].min() < 255:
            right -= step
        if mask[top, left:right].min() < 255:
            top += step
        if mask[bottom - 1, left:right].min() < 255:
            bottom -= step
        if right - left < 0.5 * W or bottom - top < 0.4 * H:
            return None
    else:
        return None
    back = cv2.perspectiveTransform(np.float32([[[left, top], [right, top], [right, bottom], [left, bottom]]]),
                                    np.linalg.inv(M))[0]
    kept = float(cv2.contourArea(back)) / float(W * H)
    return warped[top:bottom, left:right], M, kept, math.degrees(angle), (left, top, right, bottom)


def _correct_exterior(img: "np.ndarray") -> tuple[str, "np.ndarray | None", dict[str, Any]]:
    H, W = img.shape[:2]
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    segments = _vertical_segments(gray, EXTERIOR_SEGMENT_MAX_LEAN_DEG)
    if len(segments) < MIN_SEGMENTS:
        return "no_lines", None, {}
    p, q, inliers = _fit_lean(segments, W)
    if inliers < MIN_SEGMENTS:
        return "no_lines", None, {}
    edge = [math.degrees(math.atan(p + q * x)) for x in (-0.5, 0.5)]
    residual_before = residual_lean_deg(img) or 0.0
    metrics: dict[str, Any] = {
        "lean_before_deg": round(mean_abs_lean_deg(gray) or 0.0, 2),
        "residual_before_deg": round(residual_before, 2),
        "roll_before_deg": round(math.degrees(math.atan(p)), 2),
        "convergence_deg": round(abs(edge[0] - edge[1]), 2),
        "method": "level_camera",
    }
    if residual_before < LEVEL_LEAN_DEG:
        return "level", None, metrics
    worst_edge = max(abs(e) for e in edge)
    cap = 1.0 if worst_edge <= EXTERIOR_MAX_EDGE_LEAN_DEG else EXTERIOR_MAX_EDGE_LEAN_DEG / worst_edge
    chosen = None
    for step in EXTERIOR_STRENGTHS:
        strength = min(step, cap)
        result = _level_camera(img, p, q, strength)
        if result is None:
            continue
        out, M, kept, angle, (left, top, right, bottom) = result
        width, height = right - left, bottom - top
        centre = (left + int(0.2 * width), top + int(0.2 * height), left + int(0.8 * width), top + int(0.8 * height))
        central = _anisotropy(M, centre)
        whole = _anisotropy(M, (left, top, right, bottom))
        # Building aspect change = how much taller/thinner the centre of the building
        # became (median local sx/sy over the central 60 %); edge = worst corner.
        aspect = abs(float(np.median(central)) - 1.0)
        edge_change = max(abs(r - 1.0) for r in whole)
        candidate = {
            "rotation_deg": round(angle, 2), "keystone_strength": round(strength, 3),
            "crop_kept": round(kept, 3), "aspect_change": round(aspect, 4),
            "edge_aspect_change": round(edge_change, 4),
            "output_size": [int(out.shape[1]), int(out.shape[0])],
        }
        if (kept >= EXTERIOR_MIN_CROP_KEPT and aspect <= EXTERIOR_MAX_ASPECT_CHANGE
                and edge_change <= EXTERIOR_MAX_EDGE_ASPECT_CHANGE):
            chosen = (out, candidate)
            break
        if strength <= min(EXTERIOR_STRENGTHS):
            break
    if chosen is None:
        return "reverted:aspect_change", None, metrics
    out, candidate = chosen
    metrics.update(candidate)
    metrics["partial"] = candidate["keystone_strength"] < 1.0
    after = residual_lean_deg(out)
    if (after is not None and after > TARGET_RESIDUAL_DEG and not metrics["partial"]
            and min(out.shape[:2]) >= MIN_SHORT_SIDE):
        # one refinement pass on the result (fit noise / lens distortion leftovers)
        segs2 = _vertical_segments(cv2.cvtColor(out, cv2.COLOR_BGR2GRAY), EXTERIOR_SEGMENT_MAX_LEAN_DEG)
        if len(segs2) >= MIN_SEGMENTS:
            p2, q2, n2 = _fit_lean(segs2, out.shape[1])
            refined = _level_camera(out, p2, q2, 1.0) if n2 >= MIN_SEGMENTS else None
            if refined is not None:
                after2 = residual_lean_deg(refined[0])
                if after2 is not None and after2 < after and refined[2] >= 0.85:
                    out, after = refined[0], after2
                    metrics["refined"] = True
                    metrics["crop_kept"] = round(metrics["crop_kept"] * refined[2], 3)
                    metrics["output_size"] = [int(out.shape[1]), int(out.shape[0])]
    metrics["residual_after_deg"] = None if after is None else round(after, 2)
    lean_after = mean_abs_lean_deg(cv2.cvtColor(out, cv2.COLOR_BGR2GRAY))
    metrics["lean_after_deg"] = None if lean_after is None else round(lean_after, 2)
    if after is not None and after > residual_before - 0.2:
        return "reverted:no_improvement", None, metrics
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
