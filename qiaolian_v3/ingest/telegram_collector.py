"""Telegram collector core for V3 Phase 2.

This class accepts already-observed Telegram evidence and persists it through
SourceIngestService. Network/session wiring is intentionally not connected to
production in Phase 2. The boundary ends after SourcePost/Revision/media links.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Iterable

from qiaolian_v3.media.source_media import SourceMedia

from .source_identity import build_album_external_post_id, build_single_external_post_id
from .source_service import IngestResult, SourceIngestService


class TelegramCollector:
    def __init__(self, ingest_service: SourceIngestService) -> None:
        self.ingest_service = ingest_service

    def collect_single(
        self,
        *,
        source_name: str,
        source_external_identity: str,
        message_id: int | str,
        raw_text: str,
        media: Iterable[SourceMedia],
        source_created_at: str | datetime | None,
        fetched_at: str | datetime | None,
        source_url: str = "",
        source_author: str = "channel",
        raw_payload: dict[str, Any] | None = None,
    ) -> IngestResult:
        return self.ingest_service.ingest_telegram(
            source_name=source_name,
            source_external_identity=source_external_identity,
            external_post_id=build_single_external_post_id(message_id),
            raw_text=raw_text,
            media=media,
            source_created_at=source_created_at,
            fetched_at=fetched_at,
            source_url=source_url,
            source_author=source_author,
            raw_payload=raw_payload,
        )

    def collect_album(
        self,
        *,
        source_name: str,
        source_external_identity: str,
        grouped_id: int | str | None,
        anchor_message_id: int | str,
        raw_text: str,
        media: Iterable[SourceMedia],
        source_created_at: str | datetime | None,
        fetched_at: str | datetime | None,
        source_url: str = "",
        source_author: str = "channel",
        raw_payload: dict[str, Any] | None = None,
    ) -> IngestResult:
        return self.ingest_service.ingest_telegram(
            source_name=source_name,
            source_external_identity=source_external_identity,
            external_post_id=build_album_external_post_id(
                grouped_id=grouped_id,
                anchor_message_id=anchor_message_id,
            ),
            raw_text=raw_text,
            media=media,
            source_created_at=source_created_at,
            fetched_at=fetched_at,
            source_url=source_url,
            source_author=source_author,
            raw_payload=raw_payload,
        )
