from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Sequence

from .media_selection import select_publication_media
from .photo_formatter import format_gallery_photo, ordered_source_files

IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp"}


def _resolve_ranked_path(value: str | Path, input_folder: str | Path) -> Path:
    folder = Path(input_folder).resolve()
    raw = Path(str(value or ""))
    if raw.is_absolute():
        return raw.resolve()
    in_folder = (folder / raw).resolve()
    if in_folder.exists():
        return in_folder
    return raw.resolve()


def get_source_order_files(
    folder: str | Path,
    *,
    source_order: Sequence[str | Path | dict] | None = None,
    source_manifest: str | Path | None = None,
) -> list[Path]:
    return ordered_source_files(
        folder,
        source_order=source_order,
        source_manifest=source_manifest,
    )


def process_listing_media(
    listing_id: str,
    input_folder: str | Path,
    output_root: str | Path = "processed",
    logo_path: str | Path | None = None,
    *,
    logo_position: str = "top_left",
    source_order: Sequence[str | Path | dict] | None = None,
    source_manifest: str | Path | None = None,
) -> dict[str, Any]:
    input_folder = Path(input_folder).resolve()
    output_dir = Path(output_root) / str(listing_id)
    gallery_dir = output_dir / "gallery"
    output_dir.mkdir(parents=True, exist_ok=True)
    gallery_dir.mkdir(parents=True, exist_ok=True)

    source_files = get_source_order_files(
        input_folder,
        source_order=source_order,
        source_manifest=source_manifest,
    )
    selected = select_publication_media(source_files)

    duplicate_files = {
        _resolve_ranked_path(item.get("file", ""), input_folder)
        for item in selected.get("duplicates", [])
        if item.get("file")
    }
    rejected_files = {
        _resolve_ranked_path(value, input_folder)
        for value in selected.get("rejected_paths", [])
        if value
    }

    gallery: list[dict[str, Any]] = []
    order = 1
    usable = {Path(path).resolve() for path in selected.get("gallery_paths", [])}
    for src in source_files:
        resolved = src.resolve()
        if resolved not in usable:
            continue
        dst = gallery_dir / f"{order:02d}.jpg"
        info = format_gallery_photo(
            input_path=src,
            output_path=dst,
            logo_path=logo_path,
            logo_position=logo_position,
        )
        info["order"] = order
        info["source_order"] = src.name
        gallery.append(info)
        order += 1

    cover_path = str(selected.get("cover_path") or "")
    cover_candidate = next(
        (
            item
            for item in selected.get("ranking", [])
            if str(Path(item.get("file", "")).resolve()) == str(Path(cover_path).resolve())
        ),
        None,
    )

    report = {
        "version": "media_pipeline.v3",
        "listing_id": str(listing_id),
        "photos_source": len(source_files),
        "photos_duplicates": len(duplicate_files),
        "photos_rejected": len(rejected_files),
        "photos_gallery": len(gallery),
        "gallery_order_policy": "collector_source_order_after_dedup_and_severe_reject",
        "source_order_authority": "manifest_or_explicit_order_then_natural_filename_fallback",
        "cover_candidate": cover_candidate,
        "gallery": gallery,
        "duplicates": selected.get("duplicates", []),
        "ranking": selected.get("ranking", []),
    }

    report_path = output_dir / "media_report.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return report


if __name__ == "__main__":
    import sys

    listing_id = sys.argv[1] if len(sys.argv) > 1 else "7028"
    input_folder = sys.argv[2] if len(sys.argv) > 2 else f"houses/{listing_id}"
    logo_path = sys.argv[3] if len(sys.argv) > 3 else None
    source_manifest = sys.argv[4] if len(sys.argv) > 4 else None
    result = process_listing_media(
        listing_id=listing_id,
        input_folder=input_folder,
        logo_path=logo_path,
        source_manifest=source_manifest,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
