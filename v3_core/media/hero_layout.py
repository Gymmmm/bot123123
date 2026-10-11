"""Subject-aware hero framing + sky text slot for villa covers (Gym 2026-10-11).

Gym: do not crop the sky away — keep the building fully visible and put the
📍 title + 户型·类型 pill into the open sky above it.

* ``skyline``: per column, the first row (from the top) with real detail. Pixels
  above it are open sky (white walls below a roofline are not, because the roof
  edge stops the scan). Pure numpy/PIL, no OpenCV needed.
* ``frame_offset``: where to cut a hero window out of a taller photo so the
  building (incl. its roofline) stays in and there is room above it for text;
  surplus is taken from the foreground first.
* ``text_slot``: the best place for a text block inside the sky of the framed
  hero — centred / upper-middle when the sky allows, else the widest sky side.
  ``None`` → caller falls back to the classic top-left title with a shade.
"""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np
from PIL import Image, ImageFilter

GRID_W = 216                 # analysis width (px)
DETAIL_THRESHOLD = 16.0      # smoothed |∇| above this = structure (roof, tree, cable…)
MAX_OUTSIDE_SKY = 0.04       # a text box may overlap at most 4 % non-sky cells
BUILDING_BASE = 0.94         # below this the frame is usually ground / driveway


@dataclass(frozen=True)
class Slot:
    x: int
    y: int
    width: int
    height: int
    luminance: float         # mean 0..255 behind the text (choose dark/white ink)
    align: str               # "center" | "left" | "right"


def _detail(image: Image.Image) -> tuple[np.ndarray, np.ndarray, float]:
    """Return (detail map, luminance map, scale) at GRID_W width."""
    gray = image.convert("L")
    scale = GRID_W / float(gray.width)
    h = max(8, round(gray.height * scale))
    small = np.asarray(gray.resize((GRID_W, h), Image.Resampling.BILINEAR), dtype=np.float32)
    gx = np.abs(np.diff(small, axis=1, prepend=small[:, :1]))
    gy = np.abs(np.diff(small, axis=0, prepend=small[:1, :]))
    grad = Image.fromarray(np.clip((gx + gy) * 2.0, 0, 255).astype(np.uint8)).filter(ImageFilter.BoxBlur(2))
    return np.asarray(grad, dtype=np.float32), small, scale


def skyline(image: Image.Image) -> tuple[np.ndarray, float]:
    """First structured row per analysis column (in analysis px) and the scale used."""
    detail, _, scale = _detail(image)
    busy = detail > DETAIL_THRESHOLD
    h = busy.shape[0]
    first = np.where(busy.any(axis=0), busy.argmax(axis=0), h)
    # ignore 1–2 column spikes (a cable, a lamp post)
    padded = np.pad(first, 2, mode="edge")
    smooth = np.array([np.median(padded[i:i + 5]) for i in range(len(first))])
    return smooth, scale


def building_top(image: Image.Image) -> float:
    """Roofline height as a fraction of the image (0 = top), from the central 60 % of columns."""
    line, scale = skyline(image)
    w = len(line)
    centre = line[int(0.2 * w):int(0.8 * w)]
    h = image.height * scale
    return float(np.percentile(centre, 25)) / max(1.0, h)


def frame_offset(image: Image.Image, size: tuple[int, int], *, text_room: int,
                 base: float = BUILDING_BASE) -> tuple[float, float]:
    """``ImageOps.fit`` centering for a hero window out of a taller photo.

    Wanted span: from ``text_room`` px (output scale) above the roofline down to the
    building base (``base`` × height; the last few % are usually driveway). If the
    span fits, the window ends at the building base, so surplus is cut from the
    foreground and the sky (text area) is kept;
    if it does not, the window is centred on the span so roof and entrance lose
    equally — never only the bottom of the building.
    """
    target_w, target_h = size
    scale = max(target_w / image.width, target_h / image.height)
    scaled_h = image.height * scale
    surplus = scaled_h - target_h
    if surplus <= 1:
        return 0.5, 0.5
    roof = building_top(image) * scaled_h
    span_top, span_bottom = max(0.0, roof - text_room), base * scaled_h
    if span_bottom - span_top <= target_h:
        top = max(0.0, span_bottom - target_h)   # keep as much sky as possible, cut foreground
    else:
        top = (span_top + span_bottom - target_h) / 2.0
    top = max(0.0, min(top, surplus))
    return 0.5, top / surplus


def text_slot(hero: Image.Image, block: tuple[int, int], *, margin: int, max_bottom: float = 0.62) -> Slot | None:
    """Best sky position for a ``block`` (w, h) inside ``hero`` (framed, output px)."""
    detail, lum, scale = _detail(hero)
    line, _ = skyline(hero)
    gh, gw = detail.shape
    bw, bh = max(1, round(block[0] * scale)), max(1, round(block[1] * scale))
    m = max(1, round(margin * scale))
    if bw + 2 * m > gw:
        return None
    rows = np.arange(gh)[:, None]
    sky = rows < line[None, :]
    limit = int(max_bottom * gh)
    candidates = []
    xs = {"center": (gw - bw) // 2, "left": m, "right": gw - bw - m}
    for align, x in xs.items():
        cols = slice(x, x + bw)
        floor = int(min(line[cols].min(), limit))           # sky band above the building under this box
        for y in range(m, max(m, floor - bh) + 1):
            box = sky[y:y + bh, cols]
            if box.size == 0 or y + bh > limit:
                break
            outside = 1.0 - float(box.mean())
            if outside > MAX_OUTSIDE_SKY:
                continue
            busy = float(detail[y:y + bh, cols].mean())
            ideal = (m + max(m, floor - bh)) / 2.0           # vertically centred in the free sky band
            score = busy + 0.35 * abs(y - ideal) + (0.0 if align == "center" else 6.0) + 0.05 * y
            candidates.append((score, align, x, y))
    if not candidates:
        return None
    _, align, x, y = min(candidates)
    region = lum[y:y + bh, x:x + bw]
    return Slot(x=round(x / scale), y=round(y / scale), width=block[0], height=block[1],
                luminance=float(region.mean()), align=align)


__all__ = ["Slot", "building_top", "frame_offset", "skyline", "text_slot"]
