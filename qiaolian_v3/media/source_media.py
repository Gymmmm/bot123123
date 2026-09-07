"""Source-media evidence helpers for Phase 2 ingest.

This module only computes/stores stable media identity metadata. Media ranking,
cover selection and listing-level media policy belong to later phases.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


@dataclass(frozen=True)
class SourceMedia:
    content_hash: str
    sort_order: int
    media_type: str = "photo"
    local_path: str | None = None
    telegram_file_id: str | None = None
    telegram_file_unique_id: str | None = None
    message_id: int | None = None

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
            "sort_order": self.sort_order,
            "media_type": self.media_type,
            "local_path": self.local_path,
            "telegram_file_id": self.telegram_file_id,
            "telegram_file_unique_id": self.telegram_file_unique_id,
            "message_id": self.message_id,
        }
