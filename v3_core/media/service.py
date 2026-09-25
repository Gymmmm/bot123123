"""Prepare immutable derived media for V3 publication packaging.

Raw source evidence is never modified. Publication media first goes through the
source-mark scrubber (crop bottom contact/slogan bands off, then light corner
inpaint; abandon only unsafe inpaint-only results), then dedupe / quality
ranking / cover selection runs on derived files. Gallery derivatives get a mild
enhance + cover-style corner mark. Cover rendering remains owned by
``CoverRenderService``.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
from pathlib import Path
import shutil
from typing import Any

from v3_core.ingest.source_reader import SourceReader
from .media_selection import select_publication_media
from .photo_formatter import format_gallery_photo, gallery_canvas_key, resolve_gallery_logo_path
from .source_scrub import scrub_file


MAX_SCRUB_COVERAGE = 0.08
SCRUB_REVISION = "source_scrub_v3_force_bottom_contact_crop_20260925"
GALLERY_BRAND_REVISION = "qiaolian_gallery_v9_no_frame_bright_logo_20260925"


@dataclass(frozen=True)
class PreparedSourceMedia:
    source_post_id: int
    cover_source_path: str
    gallery_paths: tuple[str, ...]
    source_identity: dict[str, Any]
    duplicates: tuple[dict[str, str], ...]
    rejected_paths: tuple[str, ...]
    ranking: tuple[dict[str, Any], ...]


class MediaPreparationService:
    def __init__(self, reader: SourceReader, *, prepared_dir: str | Path | None = None):
        self.reader = reader
        db_path = Path(reader.db_path).expanduser().resolve()
        self.prepared_dir = (
            Path(prepared_dir).expanduser().resolve()
            if prepared_dir is not None
            else db_path.parent.parent / "media" / "prepared_v3"
        )

    @staticmethod
    def _digest(path: Path) -> str:
        digest = hashlib.sha256()
        digest.update((SCRUB_REVISION + "\0").encode("utf-8"))
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    @staticmethod
    def _safe_suffix(path: Path) -> str:
        suffix = path.suffix.lower()
        return suffix if suffix in {".jpg", ".jpeg", ".png", ".webp"} else ".img"

    def _original_derivative(self, src: Path, target_dir: Path, digest: str) -> Path:
        fallback = target_dir / f"{digest[:24]}_source{self._safe_suffix(src)}"
        if not fallback.is_file():
            shutil.copy2(src, fallback)
        return fallback

    @staticmethod
    def _gallery_style_key(cover_style: str | None) -> str:
        """Stable cache key per cover mark: classic_blue / right_price / black_gold."""
        try:
            from .cover_styles import cover_style_family

            key = cover_style_family(cover_style)
        except Exception:
            key = str(cover_style or "").strip().lower()
            if key.endswith("_portrait"):
                key = key[: -len("_portrait")]
        if key in {"black_gold", "villa_premium", "dark_glass", "premium_photo", "premium"}:
            return "black_gold"
        if key in {
            "classic_blue",
            "classic",
            "left_info",
            "minimal",
            "minimal_white",
            "blue_banner",
            "premium_4image",
        } or str(key).startswith("video_"):
            return "classic_blue"
        return "right_price"

    @classmethod
    def _gallery_digest(
        cls,
        path: Path,
        cover_style: str | None = None,
        *,
        gallery_orientation: str | None = None,
    ) -> str:
        digest = hashlib.sha256()
        style_key = cls._gallery_style_key(cover_style)
        orient_key = gallery_canvas_key(gallery_orientation)
        digest.update(
            (GALLERY_BRAND_REVISION + "\0" + style_key + "\0" + orient_key + "\0").encode("utf-8")
        )
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    @staticmethod
    def _gallery_orientation_for_cover(cover_source_path: str | Path | None) -> str:
        """Lock every 更多实拍 frame to the main-cover orientation for smooth flips."""
        if not cover_source_path:
            return "landscape"
        try:
            from .cover_styles import source_image_orientation

            detected = source_image_orientation(cover_source_path)
        except Exception:
            return "landscape"
        return gallery_canvas_key(detected)

    def _branded_gallery(
        self,
        *,
        source_post_id: int | str,
        paths: list[str],
        cover_style: str | None = None,
        cover_source_path: str | Path | None = None,
    ) -> list[str]:
        """Create deterministic logo-bearing gallery copies without touching evidence.

        Cover rendering intentionally keeps using the clean selected source.  The
        returned files are only for the public ``更多实拍`` gallery, preventing a
        second brand mark from appearing underneath the cover template.
        All frames share the cover's landscape/portrait canvas so flips stay
        size-stable, with a thin white frame + brighten polish.
        """
        target_dir = self.prepared_dir / str(int(source_post_id)) / "gallery"
        target_dir.mkdir(parents=True, exist_ok=True)
        style_key = self._gallery_style_key(cover_style)
        gallery_orientation = self._gallery_orientation_for_cover(cover_source_path)
        branded: list[str] = []
        for raw in paths:
            source = Path(str(raw)).expanduser().resolve()
            if not source.is_file():
                raise FileNotFoundError(f"gallery_source_not_found:{source}")
            digest = self._gallery_digest(
                source,
                cover_style=cover_style,
                gallery_orientation=gallery_orientation,
            )
            target = target_dir / f"{style_key}_{gallery_orientation}_{digest[:16]}_gallery.jpg"
            if not target.is_file():
                format_gallery_photo(
                    source,
                    target,
                    logo_path=resolve_gallery_logo_path(cover_style),
                    logo_position="top_left",
                    add_logo=True,
                    enhance=True,
                    cover_style=cover_style,
                    force_orientation=gallery_orientation,
                )
            branded.append(str(target.resolve()))
        return branded

    def _scrubbed_paths(
        self,
        *,
        source_post_id: int | str,
        paths: list[str],
    ) -> tuple[list[str], dict[str, str], list[str]]:
        target_dir = self.prepared_dir / str(int(source_post_id))
        target_dir.mkdir(parents=True, exist_ok=True)
        accepted: list[str] = []
        raw_to_clean: dict[str, str] = {}
        rejected: list[str] = []

        for raw in paths:
            src = Path(str(raw)).expanduser().resolve()
            if not src.is_file():
                rejected.append(str(src))
                continue
            digest = self._digest(src)
            dst = target_dir / f"{digest[:24]}_clean.jpg"
            chosen: Path | None = None
            try:
                if dst.is_file():
                    chosen = dst
                else:
                    info = scrub_file(src, dst, prefer_crop=True)
                    methods = [str(item) for item in (info.get("methods") or [])]
                    cropped = any(item.startswith("crop_bottom") for item in methods)
                    inpaint_cov = float(info.get("inpaint_coverage") or 0.0)
                    detect_cov = float(info.get("coverage") or 0.0)
                    # Bottom contact/slogan crop must stick — never fall back to the
                    # watermarked original after the strip was already cut off.
                    if cropped:
                        chosen = dst
                    elif max(inpaint_cov, detect_cov if "inpaint_telea" in methods else 0.0) <= MAX_SCRUB_COVERAGE:
                        chosen = dst
                    else:
                        dst.unlink(missing_ok=True)
                        chosen = self._original_derivative(src, target_dir, digest)
            except Exception:
                dst.unlink(missing_ok=True)
                # Scrubbing is an enhancement stage, not permission to corrupt
                # or drop otherwise valid source evidence. Fail safely to an
                # immutable derived copy and let production quality gates decide.
                chosen = self._original_derivative(src, target_dir, digest)

            clean = str(chosen.resolve())
            accepted.append(clean)
            raw_to_clean[str(src)] = clean

        return accepted, raw_to_clean, rejected

    def prepare(
        self,
        *,
        source_post_id: int | str,
        manual_cover_path: str | None = None,
        cover_style: str | None = None,
    ) -> PreparedSourceMedia:
        raw_paths = self.reader.source_image_paths(source_post_id)
        cleaned_paths, raw_to_clean, scrub_rejected = self._scrubbed_paths(
            source_post_id=source_post_id,
            paths=raw_paths,
        )
        if not cleaned_paths:
            raise ValueError("missing_usable_images_after_source_scrub")

        manual_clean = ""
        if manual_cover_path:
            manual_raw = str(Path(manual_cover_path).expanduser().resolve())
            manual_clean = raw_to_clean.get(manual_raw, "")

        selected = select_publication_media(
            cleaned_paths,
            manual_cover_path=manual_clean or None,
        )
        branded_gallery = self._branded_gallery(
            source_post_id=source_post_id,
            paths=[str(path) for path in selected["gallery_paths"]],
            cover_style=cover_style,
            cover_source_path=selected["cover_path"],
        )
        rejected = [str(path) for path in scrub_rejected]
        rejected.extend(str(path) for path in selected["rejected_paths"])
        gallery_orientation = self._gallery_orientation_for_cover(selected["cover_path"])
        return PreparedSourceMedia(
            source_post_id=int(source_post_id),
            cover_source_path=str(selected["cover_path"]),
            gallery_paths=tuple(branded_gallery),
            source_identity={
                **self.reader.source_identity(source_post_id),
                "gallery_brand_revision": GALLERY_BRAND_REVISION,
                "gallery_cover_style": self._gallery_style_key(cover_style),
                "gallery_orientation": gallery_orientation,
            },
            duplicates=tuple(dict(item) for item in selected["duplicates"]),
            rejected_paths=tuple(rejected),
            ranking=tuple(dict(item) for item in selected["ranking"]),
        )


__all__ = [
    "GALLERY_BRAND_REVISION",
    "MediaPreparationService",
    "PreparedSourceMedia",
]
