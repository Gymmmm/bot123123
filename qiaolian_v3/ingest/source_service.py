"""V3 ingest boundary: source evidence -> SourcePost/immutable Revision only."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Iterable

from qiaolian_v3.db.repositories.source_media import SourceMediaRepository
from qiaolian_v3.db.repositories.sources import SourceRepository
from qiaolian_v3.media.source_media import SourceMedia

from .sanitizer import sanitize_source_text
from .source_identity import make_source_content_hash


class IngestDisposition(str, Enum):
    NEW_SOURCE_POST = "NEW_SOURCE_POST"
    DUPLICATE_IGNORE = "DUPLICATE_IGNORE"
    SOURCE_UPDATED = "SOURCE_UPDATED"


@dataclass(frozen=True)
class IngestResult:
    disposition: IngestDisposition
    source_id: int
    source_post_id: int
    revision_id: int
    revision_no: int
    source_content_hash: str
    sanitized_text: str
    media_count: int
    insufficient_media: bool


def _timestamp(value: str | datetime | None) -> str:
    if isinstance(value, datetime):
        dt = value
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat()
    if value is not None:
        return str(value)
    return datetime.now(timezone.utc).isoformat()


class SourceIngestService:
    """Persist normalized source evidence without crossing into Parser/publication.

    The source content hash is based on sanitized text plus normalized ordered
    media identity. Exact repeats do not create a second revision, while edits
    to facts, media, or album media order create the next immutable revision.
    """

    def __init__(self, conn: sqlite3.Connection, *, min_listing_images: int = 4) -> None:
        self.conn = conn
        self.sources = SourceRepository(conn)
        self.source_media = SourceMediaRepository(conn)
        self.min_listing_images = max(1, int(min_listing_images))

    def ingest_telegram(
        self,
        *,
        source_name: str,
        source_external_identity: str,
        external_post_id: str,
        raw_text: str,
        media: Iterable[SourceMedia],
        source_created_at: str | datetime | None,
        fetched_at: str | datetime | None,
        source_type: str = "telegram_channel",
        source_mode: str = "collector",
        source_url: str = "",
        source_author: str = "channel",
        raw_payload: dict[str, Any] | None = None,
        source_enabled: bool = True,
        source_config: dict[str, Any] | None = None,
    ) -> IngestResult:
        fetched = _timestamp(fetched_at)
        created = _timestamp(source_created_at) if source_created_at is not None else None
        ordered_media = sorted(tuple(media), key=lambda item: item.sort_order)
        sanitized = sanitize_source_text(raw_text)
        content_hash = make_source_content_hash(
            sanitized.text,
            (item.media_identity for item in ordered_media),
        )

        existing_before = self.sources.get_source_post_by_identity(
            source_type=source_type,
            source_name=source_name,
            external_post_id=external_post_id,
        )
        current_before = None
        if existing_before is not None and existing_before["current_revision_id"] is not None:
            current_before = self.sources.get_revision(int(existing_before["current_revision_id"]))

        source_id = self.sources.register_source(
            source_type=source_type,
            source_name=source_name,
            external_identity=source_external_identity,
            enabled=source_enabled,
            collector_config=source_config or {},
        )
        source_post_id = self.sources.register_source_post(
            source_id=source_id,
            source_mode=source_mode,
            source_type=source_type,
            source_name=source_name,
            external_post_id=external_post_id,
            source_url=source_url,
            source_author=source_author,
            dedupe_key=content_hash,
            ingest_status="COLLECTED",
            parse_status="COLLECTED",
            first_seen_at=fetched,
            last_seen_at=fetched,
            status="active",
        )

        if current_before is not None and str(current_before["source_content_hash"]) == content_hash:
            return IngestResult(
                disposition=IngestDisposition.DUPLICATE_IGNORE,
                source_id=source_id,
                source_post_id=source_post_id,
                revision_id=int(current_before["id"]),
                revision_no=int(current_before["revision_no"]),
                source_content_hash=content_hash,
                sanitized_text=sanitized.text,
                media_count=len(ordered_media),
                insufficient_media=self._insufficient_media(ordered_media),
            )

        raw_images = [item.to_revision_json() for item in ordered_media if item.media_type == "photo"]
        raw_videos = [item.to_revision_json() for item in ordered_media if item.media_type == "video"]
        insufficient_media = self._insufficient_media(ordered_media)
        revision = self.sources.append_revision(
            source_post_id=source_post_id,
            source_content_hash=content_hash,
            raw_text=raw_text or "",
            sanitized_text=sanitized.text,
            raw_payload=raw_payload or {},
            raw_images=raw_images,
            raw_videos=raw_videos,
            raw_contact=" | ".join(sanitized.contacts),
            raw_meta={
                "source_contact_removed": bool(sanitized.contacts or sanitized.removed_lines),
                "removed_contact_count": len(sanitized.contacts),
                "removed_line_count": len(sanitized.removed_lines),
                "raw_image_count": len(raw_images),
                "raw_video_count": len(raw_videos),
                "media_status": "insufficient_media" if insufficient_media else "sufficient_media",
                "min_listing_images": self.min_listing_images,
            },
            source_created_at=created,
            fetched_at=fetched,
        )
        for item in ordered_media:
            self.source_media.link_revision_media(
                revision_id=revision.id,
                media_asset_key=item.media_identity,
                sort_order=item.sort_order,
            )

        disposition = (
            IngestDisposition.NEW_SOURCE_POST
            if current_before is None
            else IngestDisposition.SOURCE_UPDATED
        )
        return IngestResult(
            disposition=disposition,
            source_id=source_id,
            source_post_id=source_post_id,
            revision_id=revision.id,
            revision_no=revision.revision_no,
            source_content_hash=content_hash,
            sanitized_text=sanitized.text,
            media_count=len(ordered_media),
            insufficient_media=insufficient_media,
        )

    def _insufficient_media(self, media: Iterable[SourceMedia]) -> bool:
        photo_count = sum(1 for item in media if item.media_type == "photo")
        return photo_count < self.min_listing_images
