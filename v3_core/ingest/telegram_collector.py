"""V3 Telethon collector adapter.

The adapter preserves the existing intake chain and stops at ``IntakeService``.
Operational source enable/disable state is read from the shared V3 runtime-state
repository; the collector never imports parser or publishing business modules.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.tl.types import MessageMediaDocument, MessageMediaPhoto

from v3_core.ops.runtime_state import RuntimeStateRepository
from .intake_service import IntakeResult, IntakeService, SourceIntake
from .source_repository import SourceRepository


log = logging.getLogger("v3_collector")


def channel_slug(chat_id: int) -> str:
    value = str(chat_id)
    if value.startswith("-100"):
        return value[4:]
    return value.lstrip("-")


def message_link(chat_id: int, message_id: int) -> str:
    return f"https://t.me/c/{channel_slug(chat_id)}/{message_id}"


def sender_label(message: Any) -> str:
    sender = getattr(message, "sender", None)
    if sender is None:
        return "channel"
    return (
        getattr(sender, "username", None)
        or getattr(sender, "first_name", None)
        or "unknown"
    )


def normalize_source_row(raw: dict[str, Any], index: int) -> dict[str, Any] | None:
    """Keep production compatibility with entity_id and legacy entity formats."""
    if raw.get("is_enabled") is False:
        return None
    source = dict(raw)
    source.setdefault("source_type", "telegram_channel")
    entity_id = source.get("entity_id")
    if entity_id is None and source.get("entity"):
        entity_id = str(source["entity"]).strip().lstrip("@")
        source["entity_id"] = entity_id
    if not source.get("source_name") or not source.get("entity_id"):
        return None
    source.setdefault("source_db_id", source.get("source_id", index + 1))
    return source


def load_sources(path: str | Path) -> list[dict[str, Any]]:
    source_path = Path(path)
    if not source_path.is_file():
        raise FileNotFoundError(source_path)
    data = json.loads(source_path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError("sources.json 必须是 JSON 数组")
    result: list[dict[str, Any]] = []
    for index, row in enumerate(data):
        if not isinstance(row, dict):
            continue
        normalized = normalize_source_row(row, index)
        if normalized:
            result.append(normalized)
    return result


class TelegramCollectorApp:
    def __init__(
        self,
        *,
        db_path: str,
        sources_path: str,
        download_dir: str,
        session_path: str,
        api_id: int,
        api_hash: str,
        min_listing_images: int = 4,
    ):
        self.db_path = str(Path(db_path).expanduser().resolve())
        self.sources_path = str(Path(sources_path).expanduser().resolve())
        self.download_dir = Path(download_dir).expanduser().resolve()
        self.session_path = str(Path(session_path).expanduser().resolve())
        self.api_id = int(api_id)
        self.api_hash = str(api_hash or "").strip()
        if not self.api_id or not self.api_hash:
            raise ValueError("TG_API_ID and TG_API_HASH are required")
        self.download_dir.mkdir(parents=True, exist_ok=True)
        Path(self.session_path).parent.mkdir(parents=True, exist_ok=True)
        self.repository = SourceRepository(self.db_path)
        self.runtime = RuntimeStateRepository(self.db_path)
        self.intake = IntakeService(
            self.repository,
            min_listing_images=max(4, int(min_listing_images or 4)),
        )

    async def _heartbeat_loop(self) -> None:
        while True:
            self.runtime.heartbeat("collector", state="running")
            await asyncio.sleep(30)

    def _record_result(self, source_name: str, result: IntakeResult | None) -> None:
        if result is None:
            return
        self.runtime.touch_source(
            source_name,
            collected=True,
            duplicate=result.status == "duplicate",
        )
        self.runtime.heartbeat(
            "collector",
            state="running",
            event=True,
            meta={"source": source_name, "status": result.status},
        )

    async def download_media(self, client: TelegramClient, message: Any) -> dict[str, Any] | None:
        if not getattr(message, "media", None):
            return None
        try:
            downloaded = await client.download_media(
                message.media,
                file=str(self.download_dir),
            )
            if not downloaded:
                return None
            path = str(Path(downloaded).resolve())
            digest = hashlib.sha256(Path(path).read_bytes()).hexdigest()
            telegram_file_id = ""
            telegram_unique_id = ""
            if isinstance(message.media, MessageMediaPhoto):
                telegram_file_id = str(message.media.photo.id)
                telegram_unique_id = str(message.media.photo.access_hash)
            elif isinstance(message.media, MessageMediaDocument) and message.media.document:
                telegram_file_id = str(message.media.document.id)
                telegram_unique_id = str(message.media.document.access_hash)
            return {
                "local_path": path,
                "file_hash": digest,
                "telegram_file_id": telegram_file_id,
                "telegram_file_unique_id": telegram_unique_id,
                "message_id": int(message.id),
            }
        except Exception:
            log.exception("collector media download failed message_id=%s", getattr(message, "id", ""))
            return None

    async def append_supported_media(
        self,
        client: TelegramClient,
        message: Any,
        raw_images: list[dict[str, Any]],
    ) -> str:
        """Production policy: collect images only; videos are admin-import only."""
        if getattr(message, "photo", None):
            item = await self.download_media(client, message)
            if item:
                raw_images.append(item)
            return "image"
        if getattr(message, "video", None):
            return "skip_video"
        media = getattr(message, "media", None)
        if isinstance(media, MessageMediaDocument):
            document = media.document
            if document and document.mime_type and "image" in document.mime_type:
                item = await self.download_media(client, message)
                if item:
                    raw_images.append(item)
                return "image"
        return "unsupported"

    def persist(
        self,
        *,
        source_cfg: dict[str, Any],
        chat_id: int,
        source_post_id: str,
        anchor_message_id: int,
        raw_text: str,
        raw_images: list[dict[str, Any]],
        grouped_id: int | None,
        source_author: str,
        ingest_kind: str,
        message_count: int,
    ) -> IntakeResult:
        return self.intake.persist(
            SourceIntake(
                source_id=str(source_cfg.get("source_db_id") or "") or None,
                source_type=str(source_cfg.get("source_type") or "telegram_channel"),
                source_name=str(source_cfg["source_name"]),
                source_post_id=str(source_post_id),
                source_url=message_link(chat_id, anchor_message_id),
                source_author=str(source_author or "channel"),
                raw_text=str(raw_text or ""),
                raw_images=list(raw_images),
                raw_videos=[],
                meta={
                    "chat_id": str(chat_id),
                    "grouped_id": grouped_id,
                    "ingest_kind": str(ingest_kind),
                    "message_count": int(message_count),
                    "anchor_message_id": int(anchor_message_id),
                },
            )
        )

    async def handle_single(self, event: Any, source_cfg: dict[str, Any]) -> IntakeResult | None:
        message = event.message
        if not getattr(message, "text", None) and not getattr(message, "media", None):
            return None
        raw_images: list[dict[str, Any]] = []
        if getattr(message, "media", None):
            mode = await self.append_supported_media(event.client, message, raw_images)
            if mode == "skip_video":
                return None
        if not raw_images:
            return None
        return self.persist(
            source_cfg=source_cfg,
            chat_id=int(event.chat_id),
            source_post_id=str(message.id),
            anchor_message_id=int(message.id),
            raw_text=str(message.message or ""),
            raw_images=raw_images,
            grouped_id=getattr(message, "grouped_id", None),
            source_author=sender_label(message),
            ingest_kind="single",
            message_count=1,
        )

    async def handle_album(self, event: Any, source_cfg: dict[str, Any]) -> IntakeResult | None:
        messages = list(event.messages or [])
        if not messages:
            return None
        messages.sort(key=lambda item: item.id)
        anchor = messages[0]
        raw_images: list[dict[str, Any]] = []
        for message in messages:
            if getattr(message, "media", None):
                await self.append_supported_media(event.client, message, raw_images)
        if not raw_images:
            return None
        grouped_id = getattr(anchor, "grouped_id", None)
        source_post_id = f"album_{grouped_id}" if grouped_id is not None else f"album_{anchor.id}"
        return self.persist(
            source_cfg=source_cfg,
            chat_id=int(event.chat_id),
            source_post_id=source_post_id,
            anchor_message_id=int(anchor.id),
            raw_text=str((event.text or event.raw_text or "").strip()),
            raw_images=raw_images,
            grouped_id=grouped_id,
            source_author=sender_label(anchor),
            ingest_kind="album",
            message_count=len(messages),
        )

    async def run(self) -> None:
        sources = load_sources(self.sources_path)
        if not sources:
            raise RuntimeError("sources.json 中没有可用源")
        client = TelegramClient(self.session_path, self.api_id, self.api_hash)
        await client.start()
        heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        configured = 0
        try:
            for cfg in sources:
                source_cfg = dict(cfg)
                source_name = str(source_cfg["source_name"])
                self.runtime.ensure_source(source_name, enabled=True)
                try:
                    entity = await client.get_entity(source_cfg["entity_id"])
                except Exception as exc:
                    self.runtime.touch_source(source_name, error=f"{type(exc).__name__}: {exc}")
                    log.exception("collector source resolve failed source=%s", source_name)
                    continue
                configured += 1

                @client.on(events.NewMessage(chats=entity, func=lambda event: event.grouped_id is None))
                async def _single(event: Any, config=source_cfg):
                    name = str(config["source_name"])
                    if not self.runtime.source_enabled(name):
                        return
                    try:
                        result = await self.handle_single(event, config)
                    except Exception as exc:
                        self.runtime.touch_source(name, error=f"{type(exc).__name__}: {exc}")
                        self.runtime.heartbeat("collector", state="running", event=True, error=f"{type(exc).__name__}: {exc}")
                        log.exception("collector intake failed source=%s", name)
                        return
                    self._record_result(name, result)
                    if result:
                        log.info(
                            "intake source=%s status=%s post=%s images=%s",
                            name, result.status, result.source_post_pk, result.media_count,
                        )

                @client.on(events.Album(chats=entity))
                async def _album(event: Any, config=source_cfg):
                    name = str(config["source_name"])
                    if not self.runtime.source_enabled(name):
                        return
                    try:
                        result = await self.handle_album(event, config)
                    except Exception as exc:
                        self.runtime.touch_source(name, error=f"{type(exc).__name__}: {exc}")
                        self.runtime.heartbeat("collector", state="running", event=True, error=f"{type(exc).__name__}: {exc}")
                        log.exception("collector album failed source=%s", name)
                        return
                    self._record_result(name, result)
                    if result:
                        log.info(
                            "album intake source=%s status=%s post=%s images=%s",
                            name, result.status, result.source_post_pk, result.media_count,
                        )

            if configured == 0:
                raise RuntimeError("所有采集源均无法连接")
            self.runtime.heartbeat("collector", state="running", event=True, meta={"configured_sources": configured})
            await client.run_until_disconnected()
        finally:
            heartbeat_task.cancel()
            self.runtime.heartbeat("collector", state="stopped")


def from_environment(repo_root: str | Path | None = None) -> TelegramCollectorApp:
    root = Path(repo_root or Path.cwd()).resolve()
    load_dotenv(root / ".env")
    session = str(os.getenv("TELETHON_SESSION_PATH") or "").strip()
    if not session:
        name = str(os.getenv("COLLECTOR_SESSION_NAME") or "qiaolian_collector").strip()
        session = str(root / "telethon_sessions" / name)
    return TelegramCollectorApp(
        db_path=os.getenv("DB_PATH", str(root / "data" / "qiaolian_dual_bot.db")),
        sources_path=os.getenv("COLLECTOR_SOURCES_JSON", str(root / "sources.json")),
        download_dir=os.getenv("COLLECTOR_DOWNLOAD_DIR", str(root / "media" / "collector_downloads")),
        session_path=session,
        api_id=int(os.getenv("TG_API_ID", "0") or 0),
        api_hash=os.getenv("TG_API_HASH", ""),
        min_listing_images=int(os.getenv("COLLECTOR_MIN_IMAGES", "4") or 4),
    )


__all__ = [
    "TelegramCollectorApp",
    "channel_slug",
    "from_environment",
    "load_sources",
    "message_link",
    "normalize_source_row",
    "sender_label",
]
