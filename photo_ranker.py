from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

from PIL import Image, ImageOps

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


def rank_photos(input_folder: str | Path) -> dict[str, Any]:
    """Analyze one listing only.

    Ranking is for cover candidate/reject decisions. Gallery order must never
    consume this ranked order; callers keep collector/source order separately.
    """
    folder = Path(input_folder).resolve()
    source_files = ordered_source_files(folder)
    ranked: list[dict[str, Any]] = []
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
            duplicates.append(
                {
                    "file": str(path),
                    "duplicate_of": str(duplicate_of),
                    "kind": duplicate_kind,
                    "source_order": source_index,
                }
            )
            continue

        if file_hash:
            seen_exact[file_hash] = path
        seen_near.append((dhash, path))

        score, reason = _score_image(str(path))
        reject = score <= -900 or score < SEVERE_REJECT_SCORE
        ranked.append(
            {
                "file": str(path),
                "score": float(score),
                "reason": reason,
                "reject": bool(reject),
                "source_order": source_index,
                "file_hash": file_hash,
            }
        )

    ranked.sort(key=lambda item: (-float(item["score"]), int(item["source_order"])))
    return {
        "version": "photo_ranker_v1_1",
        "ranked": ranked,
        "duplicates": duplicates,
        "source_count": len(source_files),
    }
