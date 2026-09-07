"""Source-media evidence helpers for Phase 2 ingest.

This module only reads/downloads Telegram source media and computes stable media
identity metadata. Ranking, cover selection and listing-level media policy
belong to later phases.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _telegram_ids(message: Any) -> tuple[str | None, str | None]:
    """Extract Telegram transport evidence without importing Telethon classes."""
    media = getattr(message, "media", None)
    if media is None:
        return None, None
    candidate = getattr(media, "photo", None) or getattr(media, "document", None)
    if candidate is None:
        return None, None
    file_id = getattr(candidate, "id", None)
    unique = getattr(candidate, "access_hash", None)
    return (
        str(file_id) if file_id is not None else None,
        str(unique) if unique is not None else None,
    )


@dataclass(frozen=True)
class SourceMedia:
    content_hash: str
    sort_order: int
    media_type: str = "photo"
    local_path: str | None = None
    telegram_file_id: str | None = None
    telegram_file_unique_id: str | None = None
    message_id: int | None = None

    @property
    def media_identity(self) -> str:
        """Stable identity: file hash first, Telegram unique identity fallback."""
        if self.content_hash:
            return self.content_hash
        if self.telegram_file_unique_id:
            return f"telegram_unique:{self.telegram_file_unique_id}"
        if self.telegram_file_id:
            return f"telegram_file:{self.telegram_file_id}"
        raise ValueError("source_media_identity_unavailable")

    @classmethod
    def from_bytes(
        cls,
        data: bytes,
        *,
        sort_order: int,
        message_id: int | None = None,
        telegram_file_id: str | None = None,
        telegram_file_unique_id: str | None = None,
        media_type: str = "photo",
    ) -> "SourceMedia":
        return cls(
            content_hash=_sha256_bytes(bytes(data)),
            sort_order=int(sort_order),
            media_type=str(media_type),
            telegram_file_id=telegram_file_id,
            telegram_file_unique_id=telegram_file_unique_id,
            message_id=message_id,
        )

    @classmethod
    def from_file(
        cls,
        path: str | Path,
        *,
        sort_order: int,
        message_id: int | None = None,
        telegram_file_id: str | None = None,
        telegram_file_unique_id: str | None = None,
        media_type: str = "photo",
    ) -> "SourceMedia":
        resolved = Path(path).expanduser().resolve()
        return cls(
            content_hash=_sha256_file(resolved),
            sort_order=int(sort_order),
            media_type=str(media_type),
            local_path=str(resolved),
            telegram_file_id=telegram_file_id,
            telegram_file_unique_id=telegram_file_unique_id,
            message_id=message_id,
        )

    def to_revision_json(self) -> dict[str, object]:
        return {
            "content_hash": self.content_hash,
            "media_identity": self.media_identity,
            "sort_order": self.sort_order,
            "media_type": self.media_type,
            "local_path": self.local_path,
            "telegram_file_id": self.telegram_file_id,
            "telegram_file_unique_id": self.telegram_file_unique_id,
            "message_id": self.message_id,
        }


class TelegramSourceMedia:
    """Read-only Telegram media adapter extracted from locked V2.2 collector.

    It may call the provided client's download_media method, but it never sends,
    edits, publishes, starts a session, or owns a Telegram listener.
    """

    @staticmethod
    async def download(
        client: Any,
        message: Any,
        *,
        download_dir: str | Path,
        sort_order: int,
        media_type: str = "photo",
    ) -> SourceMedia:
        target_dir = Path(download_dir).expanduser().resolve()
        target_dir.mkdir(parents=True, exist_ok=True)
        telegram_file_id, telegram_unique_id = _telegram_ids(message)
        downloaded_path = await client.download_media(getattr(message, "media", None), file=str(target_dir))

        if downloaded_path:
            resolved = Path(downloaded_path).expanduser().resolve()
            if resolved.is_file():
                return SourceMedia.from_file(
                    resolved,
                    sort_order=sort_order,
                    message_id=getattr(message, "id", None),
                    telegram_file_id=telegram_file_id,
                    telegram_file_unique_id=telegram_unique_id,
                    media_type=media_type,
                )

        item = SourceMedia(
            content_hash="",
            sort_order=int(sort_order),
            media_type=str(media_type),
            local_path=None,
            telegram_file_id=telegram_file_id,
            telegram_file_unique_id=telegram_unique_id,
            message_id=getattr(message, "id", None),
        )
        _ = item.media_identity
        return item
