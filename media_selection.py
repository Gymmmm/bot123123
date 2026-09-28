"""Single media-selection contract for publication packages.

Legacy top-level shim kept for older tests/tools. Production V3 uses
``v3_core.media.media_selection``; keep gallery ordering behavior aligned.
"""
from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, Iterable

from media_pipeline_v1_1 import IMAGE_EXTS
from photo_ranker import NEAR_DUPLICATE_HAMMING, _dhash, _hamming, rank_photo_paths


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _ordered_gallery(ranking: list[dict[str, Any]], unique: list[Path], rejected: set[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for item in ranking:
        path = str(Path(str(item.get("file") or "")).resolve())
        if not path or path in rejected or path in seen or item.get("reject"):
            continue
        ordered.append(path)
        seen.add(path)
    for path in unique:
        resolved = str(path.resolve())
        if resolved in rejected or resolved in seen:
            continue
        ordered.append(resolved)
        seen.add(resolved)
    return ordered


def select_publication_media(
    paths: Iterable[str | Path],
    *,
    manual_cover_path: str | Path | None = None,
) -> dict[str, Any]:
    """Return one cover source plus a ranking-ordered usable gallery."""
    source_paths: list[Path] = []
    for raw in paths:
        path = Path(str(raw or "")).expanduser().resolve()
        if path.is_file() and path.suffix.lower() in IMAGE_EXTS and path not in source_paths:
            source_paths.append(path)

    unique: list[Path] = []
    duplicates: list[dict[str, str]] = []
    seen_exact: dict[str, Path] = {}
    seen_near: list[tuple[int | None, Path]] = []
    for path in source_paths:
        digest = _sha256(path)
        dhash = _dhash(path)
        duplicate_of: Path | None = seen_exact.get(digest)
        kind = "exact" if duplicate_of else ""
        if duplicate_of is None:
            for previous_hash, previous_path in seen_near:
                if _hamming(dhash, previous_hash) <= NEAR_DUPLICATE_HAMMING:
                    duplicate_of = previous_path
                    kind = "near"
                    break
        if duplicate_of is not None:
            duplicates.append({"file": str(path), "duplicate_of": str(duplicate_of), "kind": kind})
            continue
        seen_exact[digest] = path
        seen_near.append((dhash, path))
        unique.append(path)

    ranking = rank_photo_paths(unique)
    rejected = {str(Path(item["file"]).resolve()) for item in ranking if item.get("reject")}
    gallery = _ordered_gallery(ranking, unique, rejected)
    if not gallery:
        raise ValueError("missing_usable_images")

    manual = str(Path(str(manual_cover_path)).expanduser().resolve()) if manual_cover_path else ""
    if manual and manual in gallery:
        cover = manual
    else:
        cover = next(
            (
                str(Path(item["file"]).resolve())
                for item in ranking
                if not item.get("reject") and str(Path(item["file"]).resolve()) in gallery
            ),
            gallery[0],
        )

    return {
        "cover_path": cover,
        "gallery_paths": gallery,
        "duplicates": duplicates,
        "rejected_paths": sorted(rejected),
        "ranking": ranking,
        "source_count": len(source_paths),
        "usable_count": len(gallery),
        "policy": "rank_ordered_gallery_cover_from_best",
    }


__all__ = ["select_publication_media"]
