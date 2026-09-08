"""Telethon-free source intake service.

Extracted from ``collector_bot.persist_source_post``.  The Telegram adapter is
responsible for downloading media and supplying source identity; this service
owns dedupe, sanitization, source evidence persistence and the insufficient
media gate.  It never runs the parser or builds a publication package.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
from typing import Any

from .source_repository import SourceRepository
from .source_sanitizer import sanitize_source_text


@dataclass(frozen=True)
class SourceIntake:
    source_type: str
    source_name: str
    source_post_id: str
    source_url: str
    source_author: str = "channel"
    source_id: str | None = None
    raw_text: str = ""
    raw_images: list[Any] = field(default_factory=list)
    raw_videos: list[Any] = field(default_factory=list)
    meta: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class IntakeResult:
    status: str
    source_post_pk: int | None
    reason: str = ""
    media_count: int = 0


def dedupe_hash_for(source: SourceIntake) -> str:
    payload = f"{source.source_type}-{source.source_name}-{source.source_post_id}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class IntakeService:
    def __init__(self, repository: SourceRepository, *, min_listing_images: int = 4):
        self.repository = repository
        self.min_listing_images = max(1, int(min_listing_images or 4))

    def persist(self, source: SourceIntake) -> IntakeResult:
        source_type = str(source.source_type or "telegram_channel")
        source_name = str(source.source_name or "").strip()
        source_post_id = str(source.source_post_id or "").strip()
        if not source_name or not source_post_id:
            raise ValueError("source_name and source_post_id are required")

        dedupe_hash = dedupe_hash_for(source)
        existing_id, reason = self.repository.find_duplicate(
            source_type=source_type,
            source_name=source_name,
            source_post_id=source_post_id,
            dedupe_hash=dedupe_hash,
            source_url=str(source.source_url or ""),
        )
        if existing_id is not None:
            return IntakeResult(
                status="duplicate",
                source_post_pk=existing_id,
                reason=reason,
                media_count=len(source.raw_images or []),
            )

        sanitized = sanitize_source_text(str(source.raw_text or ""))
        raw_images = list(source.raw_images or [])
        raw_videos = list(source.raw_videos or [])
        is_telegram_listing = source_type == "telegram_channel"
        insufficient_media = (
            is_telegram_listing and len(raw_images) < self.min_listing_images
        )
        parse_status = "insufficient_media" if insufficient_media else "pending"

        meta = dict(source.meta or {})
        meta.update(
            {
                "raw_image_count": len(raw_images),
                "raw_video_count": len(raw_videos),
                "sanitized_text": sanitized.text,
                "source_contact_removed": bool(
                    sanitized.contacts or sanitized.removed_lines
                ),
                "removed_contact_count": len(sanitized.contacts),
                "removed_line_count": len(sanitized.removed_lines),
                "visual_review_required": bool(raw_images),
                "republish_policy": "facts_only_review_required",
                "media_status": (
                    "insufficient_media" if insufficient_media else "sufficient_media"
                ),
                "min_listing_images": self.min_listing_images,
            }
        )

        source_post_pk = self.repository.save_source_post(
            source_id=source.source_id,
            source_type=source_type,
            source_name=source_name,
            source_post_id=source_post_id,
            source_url=str(source.source_url or ""),
            source_author=str(source.source_author or ""),
            raw_text=str(source.raw_text or ""),
            raw_images=raw_images,
            raw_videos=raw_videos,
            raw_contact=" | ".join(sanitized.contacts),
            raw_meta=meta,
            dedupe_hash=dedupe_hash,
            parse_status=parse_status,
        )
        written = self.repository.save_source_images(source_post_pk, raw_images)

        return IntakeResult(
            status="insufficient_media" if insufficient_media else "inserted",
            source_post_pk=source_post_pk,
            reason="",
            media_count=written,
        )


__all__ = [
    "IntakeResult",
    "IntakeService",
    "SourceIntake",
    "dedupe_hash_for",
]
