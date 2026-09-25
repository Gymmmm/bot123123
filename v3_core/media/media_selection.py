"""Single media-selection contract for publication packages.

This module does not alter raw media. It reuses the extracted photo-ranking
rules for quality/reject decisions, reorders the usable gallery by the same
cover ranking (best living/exterior shots first), and returns original source
paths.
"""
from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, Iterable

from .photo_formatter import IMAGE_EXTS
from .ranker import NEAR_DUPLICATE_HAMMING, _dhash, _hamming, rank_photo_paths


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _pick_auto_cover(ranking: list[dict[str, Any]], gallery: list[str]) -> str:
    """Choose channel cover source: best ranked shot that is safe to show first.

    Skips hard rejects, soft rejects (text-heavy / toilet-like), and toilet labels
    when any better alternative exists in the usable gallery.
    """
    gallery_set = {str(Path(path).resolve()) for path in gallery}

    def _path(item: dict[str, Any]) -> str:
        return str(Path(str(item.get("file") or "")).resolve())

    def _usable(item: dict[str, Any], *, allow_soft: bool, allow_toilet: bool) -> bool:
        path = _path(item)
        if not path or path not in gallery_set or item.get("reject"):
            return False
        if not allow_soft and item.get("soft_reject"):
            return False
        label = str(item.get("room_label") or "")
        if not allow_toilet and label == "toilet":
            return False
        return True

    for allow_soft, allow_toilet in (
        (False, False),
        (True, False),
        (True, True),
    ):
        for item in ranking:
            if _usable(item, allow_soft=allow_soft, allow_toilet=allow_toilet):
                return _path(item)
    return gallery[0]


def _ordered_gallery(ranking: list[dict[str, Any]], unique: list[Path], rejected: set[str]) -> list[str]:
    """Usable gallery in ranking order (best first), then any unscored leftovers."""
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
    """Return one cover source plus a ranking-ordered usable gallery.

    Exact and near duplicates keep the first source occurrence. Severe rejects
    are removed from the gallery. Remaining shots follow cover ranking so living /
    exterior / kitchen lead the album; toilets and text-heavy frames sink. Cover
    auto-pick prefers the same ranking and skips soft-reject / toilet when possible.
    A manually selected cover is honoured only when it survives safety gates.
    """
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
            duplicates.append({
                "file": str(path),
                "duplicate_of": str(duplicate_of),
                "kind": kind,
            })
            continue
        seen_exact[digest] = path
        seen_near.append((dhash, path))
        unique.append(path)

    ranking = rank_photo_paths(unique)
    rejected = {
        str(Path(item["file"]).resolve())
        for item in ranking
        if item.get("reject")
    }
    gallery = _ordered_gallery(ranking, unique, rejected)
    if not gallery:
        raise ValueError("missing_usable_images")

    manual = (
        str(Path(str(manual_cover_path)).expanduser().resolve())
        if manual_cover_path
        else ""
    )
    if manual and manual in gallery:
        cover = manual
    else:
        cover = _pick_auto_cover(ranking, gallery)

    return {
        "cover_path": cover,
        "gallery_paths": gallery,
        "duplicates": duplicates,
        "rejected_paths": sorted(rejected),
        "ranking": ranking,
        "source_count": len(source_paths),
        "usable_count": len(gallery),
        "policy": "rank_ordered_gallery_cover_from_best_skip_toilet_text",
    }


__all__ = ["select_publication_media"]
