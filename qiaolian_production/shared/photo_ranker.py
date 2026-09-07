from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, Iterable

import numpy as np
from PIL import Image, ImageOps

try:
    import cv2
except ImportError:  # production requirements include OpenCV; keep import-safe fallback
    cv2 = None

from cover_generator import _score_image
from photo_formatter_v1_1 import IMAGE_EXTS, ordered_source_files

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

    total = (
        sharpness * 0.20
        + brightness * 0.12
        + contrast * 0.08
        + resolution * 0.08
        + exposure * 0.14
        + orientation * 0.22
        + space * 0.11
        + color * 0.05
    )
    if ratio < 0.90:
        total -= 18
    if ratio > 2.0:
        total -= 6
    if rejected:
        total -= 50

    return {
        "score": round(max(total, 0.0), 2),
        "reject": rejected,
        "reason": reason,
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
        legacy_score, legacy_reason = _score_image(str(path))
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
    """Two-stage real-estate cover ranking without changing gallery order."""
    ranked: list[dict[str, Any]] = []
    for index, raw in enumerate(paths, start=1):
        path = Path(raw).resolve()
        if not path.is_file() or path.suffix.lower() not in IMAGE_EXTS:
            continue
        ranked.append(_score_one(path, index))

    def sort_key(item: dict[str, Any]) -> tuple:
        ratio = float(item.get("ratio") or 0)
        landscape = ratio >= 1.05
        return (
            0 if item.get("reject") else 1,
            1 if landscape else 0,
            float(item.get("orientation") or 0),
            float(item.get("sharpness") or 0),
            float(item.get("exposure") or 0),
            float(item.get("resolution") or 0),
            float(item.get("score") or 0),
            -int(item.get("source_order") or 0),
        )

    ranked.sort(key=sort_key, reverse=True)
    return ranked


def rank_photos(input_folder: str | Path) -> dict[str, Any]:
    """Rank cover candidates while preserving a separate source-order gallery."""
    folder = Path(input_folder).resolve()
    source_files = ordered_source_files(folder)
    unique: list[Path] = []
    duplicates: list[dict[str, Any]] = []
    seen_exact: dict[str, Path] = {}
    seen_near: list[tuple[int | None, Path]] = []

    for source_index, path in enumerate(source_files, start=1):
        if path.suffix.lower() not in IMAGE_EXTS:
            continue
        try:
            file_hash = _sha256(path)
        except OSError:
            file_hash = ""
        dhash = _dhash(path)
        duplicate_of: Path | None = None
        duplicate_kind = ""
        if file_hash and file_hash in seen_exact:
            duplicate_of = seen_exact[file_hash]
            duplicate_kind = "exact"
        else:
            for previous_hash, previous_path in seen_near:
                if _hamming(dhash, previous_hash) <= NEAR_DUPLICATE_HAMMING:
                    duplicate_of = previous_path
                    duplicate_kind = "near"
                    break
        if duplicate_of is not None:
            duplicates.append({
                "file": str(path),
                "duplicate_of": str(duplicate_of),
                "kind": duplicate_kind,
                "source_order": source_index,
            })
            continue
        if file_hash:
            seen_exact[file_hash] = path
        seen_near.append((dhash, path))
        unique.append(path)

    ranked = rank_photo_paths(unique)
    source_order_by_path = {str(path.resolve()): i for i, path in enumerate(unique, start=1)}
    for item in ranked:
        item["source_order"] = source_order_by_path.get(str(Path(item["file"]).resolve()), item["source_order"])
        try:
            item["file_hash"] = _sha256(Path(item["file"]))
        except OSError:
            item["file_hash"] = ""

    return {
        "version": "photo_ranker_realestate_v2",
        "ranked": ranked,
        "duplicates": duplicates,
        "source_count": len(source_files),
    }


def get_cover_candidates_from_paths(paths: Iterable[str | Path], limit: int = 5) -> list[dict[str, Any]]:
    ranked = rank_photo_paths(paths)
    valid = [item for item in ranked if not item.get("reject")]
    return (valid or ranked)[: max(1, int(limit))]


def get_best_cover_from_paths(paths: Iterable[str | Path]) -> dict[str, Any] | None:
    candidates = get_cover_candidates_from_paths(paths, limit=1)
    return candidates[0] if candidates else None


def get_cover_candidates(folder: str | Path, limit: int = 5) -> list[dict[str, Any]]:
    data = rank_photos(folder)
    valid = [item for item in data["ranked"] if not item.get("reject")]
    return (valid or data["ranked"])[: max(1, int(limit))]


def get_best_cover(folder: str | Path) -> dict[str, Any] | None:
    candidates = get_cover_candidates(folder, limit=1)
    return candidates[0] if candidates else None
