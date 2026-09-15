from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Sequence

from photo_formatter_v1_1 import format_gallery_photo, ordered_source_files
from photo_ranker import rank_photos

IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp"}


def _resolve_ranked_path(value: str | Path, input_folder: str | Path) -> Path:
    """Resolve ranker paths consistently, including bare filenames.

    Ranker output can be absolute, cwd-relative, or folder-relative. We always
    prefer the listing input folder for relative names so duplicate/reject
    filtering cannot silently miss files because the process cwd changed.
    """
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

    # Quality ranking is isolated from gallery ordering. It is used only for
    # cover candidate, duplicate detection and severe reject decisions.
    ranked_result = rank_photos(input_folder)

    duplicate_files = {
        _resolve_ranked_path(item.get("file", ""), input_folder)
        for item in ranked_result.get("duplicates", [])
        if item.get("file")
    }
    rejected_files = {
        _resolve_ranked_path(item.get("file", ""), input_folder)
        for item in ranked_result.get("ranked", [])
        if item.get("file") and item.get("reject")
    }

    source_files = get_source_order_files(
        input_folder,
        source_order=source_order,
        source_manifest=source_manifest,
    )

    gallery: list[dict[str, Any]] = []
    order = 1
    for src in source_files:
        resolved = src.resolve()
        if resolved in duplicate_files or resolved in rejected_files:
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

    cover_candidate = None
    for item in ranked_result.get("ranked", []):
        candidate_path = _resolve_ranked_path(item.get("file", ""), input_folder)
        if not item.get("reject") and candidate_path not in duplicate_files:
            cover_candidate = item
            break
    if cover_candidate is None and ranked_result.get("ranked"):
        cover_candidate = ranked_result["ranked"][0]

    report = {
        "version": "media_pipeline_v1_1",
        "listing_id": str(listing_id),
        "photos_source": len(source_files),
        "photos_duplicates": len(duplicate_files),
        "photos_rejected": len(rejected_files),
        "photos_gallery": len(gallery),
        "gallery_order_policy": "collector_source_order_after_dedup_and_severe_reject",
        "source_order_authority": "manifest_or_explicit_order_then_natural_filename_fallback",
        "cover_candidate": cover_candidate,
        "gallery": gallery,
        "duplicates": ranked_result.get("duplicates", []),
        "ranking": ranked_result.get("ranked", []),
    }

    report_path = output_dir / "media_report_v1_1.json"
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
