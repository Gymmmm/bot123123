"""Prepare immutable source media for V3 publication packaging.

This service selects usable source media only.  It does not render a cover,
write publication packages, or mutate source files.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from v3_core.ingest.source_reader import SourceReader
from .media_selection import select_publication_media


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
    def __init__(self, reader: SourceReader):
        self.reader = reader

    def prepare(
        self,
        *,
        source_post_id: int | str,
        manual_cover_path: str | None = None,
    ) -> PreparedSourceMedia:
        paths = self.reader.source_image_paths(source_post_id)
        selected = select_publication_media(
            paths,
            manual_cover_path=manual_cover_path,
        )
        return PreparedSourceMedia(
            source_post_id=int(source_post_id),
            cover_source_path=str(selected["cover_path"]),
            gallery_paths=tuple(str(path) for path in selected["gallery_paths"]),
            source_identity=self.reader.source_identity(source_post_id),
            duplicates=tuple(dict(item) for item in selected["duplicates"]),
            rejected_paths=tuple(str(path) for path in selected["rejected_paths"]),
            ranking=tuple(dict(item) for item in selected["ranking"]),
        )


__all__ = ["MediaPreparationService", "PreparedSourceMedia"]
