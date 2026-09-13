"""Prepare immutable derived media for V3 publication packaging.

Raw source evidence is never modified. Publication media first goes through the
conservative source-mark scrubber, then the existing production dedupe / quality
ranking / cover selection contract runs on derived files. If a scrub would edit
more than the safety budget, an untouched derived copy is used instead of
blocking the listing or damaging the photo. Cover rendering remains owned by
``CoverRenderService`` and is intentionally unchanged.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
from pathlib import Path
import shutil
from typing import Any

from v3_core.ingest.source_reader import SourceReader
from .media_selection import select_publication_media
from .source_scrub import scrub_file


MAX_SCRUB_COVERAGE = 0.08
SCRUB_REVISION = "source_scrub_v1"


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
                    info = scrub_file(src, dst, prefer_crop=False)
                    actual = info.get("inpaint_coverage")
                    coverage = float(actual if actual is not None else info.get("coverage") or 0.0)
                    if coverage <= MAX_SCRUB_COVERAGE:
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
        rejected = [str(path) for path in scrub_rejected]
        rejected.extend(str(path) for path in selected["rejected_paths"])
        return PreparedSourceMedia(
            source_post_id=int(source_post_id),
            cover_source_path=str(selected["cover_path"]),
            gallery_paths=tuple(str(path) for path in selected["gallery_paths"]),
            source_identity=self.reader.source_identity(source_post_id),
            duplicates=tuple(dict(item) for item in selected["duplicates"]),
            rejected_paths=tuple(rejected),
            ranking=tuple(dict(item) for item in selected["ranking"]),
        )


__all__ = ["MediaPreparationService", "PreparedSourceMedia"]
