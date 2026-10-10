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


def _normalize_cover_preference(preference: object) -> str:
    value = str(preference or "").strip().lower()
    return "exterior" if value == "exterior" else "living"


def _passes_apartment_panorama_gate(item: dict[str, Any]) -> bool:
    """Clean landscape living-room shot (strict tier for apartment covers)."""
    if str(item.get("room_label") or "") != "living":
        return False
    room = item.get("room")
    text = item.get("text")
    # Compatibility for older/manual ranking rows that predate CV metrics.
    if not isinstance(room, dict) or not isinstance(text, dict):
        return not item.get("soft_reject")
    return (
        not item.get("soft_reject")
        and float(item.get("ratio") or 0) >= 1.15
        and float(room.get("living") or 0) >= 0.55
        and float(room.get("bed") or 0) < 0.36
        and float(room.get("toilet") or 0) < 0.48
        and float(text.get("text_heavy") or 0) < 0.48
    )


def _passes_villa_exterior_gate(item: dict[str, Any]) -> bool:
    """Unambiguous, clean facade/exterior (strict tier for villa covers)."""
    if str(item.get("room_label") or "") != "exterior" or item.get("soft_reject"):
        return False
    room = item.get("room")
    text = item.get("text")
    if not isinstance(room, dict) or not isinstance(text, dict):
        return True
    return (
        float(room.get("exterior") or 0) >= 0.48
        and float(room.get("bed") or 0) < 0.36
        and float(room.get("toilet") or 0) < 0.48
        and float(text.get("text_heavy") or 0) < 0.48
    )


def _vertical_lean_deg(path: str) -> float:
    """Mean |lean| of building verticals; lower = more frontal/level. 99 when unknown."""
    try:
        from .straighten import cv2 as _cv2, mean_abs_lean_deg

        if _cv2 is None:
            return 99.0
        gray = _cv2.imread(path, _cv2.IMREAD_GRAYSCALE)
        if gray is None:
            return 99.0
        value = mean_abs_lean_deg(gray)
        return 99.0 if value is None else float(value)
    except Exception:
        return 99.0


def _pick_auto_cover_with_reason(
    ranking: list[dict[str, Any]],
    gallery: list[str],
    *,
    cover_preference: object = "living",
) -> tuple[str, str]:
    """Case-based channel cover: never hard-fails while a usable photo exists.

    Apartment: clean living panorama → any living shot → brightest / most
    spacious usable photo (bathrooms and beds last).
    Villa: clean exterior → any exterior shot (most frontal/level first, by the
    lean of building verticals) → best interior, living room first.
    """
    gallery_set = {str(Path(path).resolve()) for path in gallery}
    preferred = _normalize_cover_preference(cover_preference)

    def _path(item: dict[str, Any]) -> str:
        return str(Path(str(item.get("file") or "")).resolve())

    usable = [item for item in ranking if _path(item) in gallery_set and not item.get("reject")]
    if not usable:
        return gallery[0], "first_usable_photo"
    label = lambda item: str(item.get("room_label") or "")  # noqa: E731
    score = lambda item: float(item.get("score") or 0)  # noqa: E731

    if preferred == "living":
        strict = [item for item in usable if _passes_apartment_panorama_gate(item)]
        if strict:
            return _path(strict[0]), "apartment_living"
        living = [item for item in usable if label(item) == "living"]
        if living:
            return _path(max(living, key=score)), "apartment_living_relaxed"

        def _bright_space(item: dict[str, Any]) -> tuple[int, float]:
            room = item.get("room") if isinstance(item.get("room"), dict) else {}
            demoted = label(item) == "toilet" or float(room.get("bed") or 0) >= 0.36 or bool(item.get("soft_reject"))
            return (0 if demoted else 1, float(item.get("brightness") or 0) + float(item.get("space") or 0))

        return _path(max(usable, key=_bright_space)), "apartment_fallback_bright_spacious"

    def _most_level(items: list[dict[str, Any]]) -> dict[str, Any]:
        if len(items) == 1:
            return items[0]
        return min(items, key=lambda item: (round(_vertical_lean_deg(_path(item)) / 2.0), -score(item)))

    strict = [item for item in usable if _passes_villa_exterior_gate(item)]
    if strict:
        return _path(_most_level(strict)), "villa_exterior"
    exterior = [item for item in usable if label(item) == "exterior"]
    if exterior:
        return _path(_most_level(exterior)), "villa_exterior_relaxed"
    interior = [item for item in usable if label(item) == "living" and not item.get("soft_reject")]
    interior = interior or [item for item in usable if label(item) == "living"]
    interior = interior or [item for item in usable if label(item) != "toilet"] or usable
    return _path(max(interior, key=score)), "villa_fallback_interior"


def _pick_auto_cover(
    ranking: list[dict[str, Any]],
    gallery: list[str],
    *,
    cover_preference: object = "living",
) -> str:
    return _pick_auto_cover_with_reason(ranking, gallery, cover_preference=cover_preference)[0]


THUMB_ROOM_ORDER = ("living", "bedroom", "kitchen", "toilet", "pool")


def order_cover_thumbnails(
    ranking: Iterable[dict[str, Any]],
    gallery: Iterable[str],
    cover_path: str,
    *,
    limit: int = 3,
) -> list[str]:
    """Cover thumbnails: 客厅 → 卧室 → 厨房 → 卫生间 → 泳池, skipping the hero's room.

    One photo per room (best ranked). When fewer distinct rooms exist the
    remaining slots take the best other shots, still never the hero's room.
    The rest of the gallery follows in its original order.
    """
    cover = str(Path(str(cover_path)).resolve())
    ordered_gallery = [str(Path(str(p)).resolve()) for p in gallery]
    available = [p for p in ordered_gallery if p != cover]
    info: dict[str, dict[str, Any]] = {}
    for item in ranking:
        path = str(Path(str(item.get("file") or "")).resolve())
        if path and path not in info:
            info[path] = item
    room = lambda path: str((info.get(path) or {}).get("room_label") or "")  # noqa: E731
    rank = lambda path: -float((info.get(path) or {}).get("score") or 0)  # noqa: E731
    hero_room = room(cover)
    chosen: list[str] = []
    for wanted in THUMB_ROOM_ORDER:
        if len(chosen) >= limit:
            break
        if wanted == hero_room:
            continue
        pool = sorted((p for p in available if room(p) == wanted and p not in chosen), key=rank)
        if pool:
            chosen.append(pool[0])
    if len(chosen) < limit:
        used_rooms = {room(p) for p in chosen}
        rest = sorted((p for p in available if p not in chosen and (not hero_room or room(p) != hero_room)), key=rank)
        fresh = [p for p in rest if room(p) not in used_rooms]
        for path in fresh + [p for p in rest if p not in fresh]:
            if len(chosen) >= limit:
                break
            chosen.append(path)
    if len(chosen) < limit:
        for path in available:
            if len(chosen) >= limit:
                break
            if path not in chosen:
                chosen.append(path)
    return chosen + [p for p in available if p not in chosen]


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
    cover_preference: object = "living",
) -> dict[str, Any]:
    """Return one cover source plus a ranking-ordered usable gallery.

    Exact and near duplicates keep the first source occurrence. Severe rejects
    are removed from the gallery. Remaining shots follow cover ranking so living /
    exterior / kitchen lead the album; toilets and text-heavy frames sink. Cover
    auto-pick is case based (see ``_pick_auto_cover_with_reason``) and only
    falls back to other rooms when the preferred room is missing. A manually selected cover is honoured
    only when it survives safety gates.
    """
    preference = _normalize_cover_preference(cover_preference)
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

    ranking = rank_photo_paths(unique, cover_preference=preference)
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
        cover, cover_reason = manual, "manual"
    else:
        cover, cover_reason = _pick_auto_cover_with_reason(ranking, gallery, cover_preference=preference)

    return {
        "cover_path": cover,
        "gallery_paths": gallery,
        "duplicates": duplicates,
        "rejected_paths": sorted(rejected),
        "ranking": ranking,
        "source_count": len(source_paths),
        "usable_count": len(gallery),
        "cover_preference": preference,
        "cover_reason": cover_reason,
        "cover_thumbnails": order_cover_thumbnails(ranking, gallery, cover)[:3],
        "policy": (
            "apartment_living_then_bright_spacious_fallback"
            if preference == "living"
            else "villa_level_exterior_then_interior_fallback"
        ),
    }


__all__ = ["order_cover_thumbnails", "select_publication_media"]
