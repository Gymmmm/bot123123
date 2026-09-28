#!/usr/bin/env python3
"""One-shot V3 Telegram source history backfill.

Defaults to importing the latest 50 logical Telegram post groups from a single
configured source. Album messages sharing grouped_id count as one group.
Existing source posts are safe: IntakeService keeps its normal idempotency.
This script does not publish anything to the destination channel.

Environment:
  SOURCE_NAME=zufang555
  TARGET_GROUPS=50
  FETCH_LIMIT=1200

Run from the deployed repo root with the normal V3 .env loaded.
"""
from __future__ import annotations

import asyncio
import os
import sqlite3
from pathlib import Path
from collections import defaultdict
from dataclasses import dataclass
from typing import Any

from telethon import TelegramClient

from v3_core.ingest.telegram_collector import (
    from_environment,
    load_sources,
    sender_label,
)


@dataclass
class Unit:
    kind: str
    messages: list[Any]

    @property
    def newest_id(self) -> int:
        return max(int(m.id) for m in self.messages)


def history_client(app):
    if os.getenv("BACKFILL_READ_ONLY_SESSION") != "1":
        return TelegramClient(app.session_path, app.api_id, app.api_hash)
    # Read the existing authorization into memory; never lock/write the live
    # collector session or request a new login during a history-only run.
    from telethon.crypto import AuthKey
    from telethon.sessions import MemorySession
    path = str(app.session_path)
    if not path.endswith('.session'):
        path += '.session'
    with sqlite3.connect('file:' + str(Path(path).resolve()) + '?mode=ro', uri=True) as conn:
        row = conn.execute('SELECT dc_id,server_address,port,auth_key FROM sessions LIMIT 1').fetchone()
    if not row or not row[3]:
        raise RuntimeError('existing_collector_authorization_required')
    memory = MemorySession()
    memory.set_dc(row[0], row[1], row[2])
    memory.auth_key = AuthKey(row[3])
    return TelegramClient(memory, app.api_id, app.api_hash, receive_updates=False)


async def main_async() -> None:
    app = from_environment()
    stream_task = str(os.getenv("BACKFILL_STREAM_TASK", "")).strip()
    if stream_task:
        from v3_core.ingest.history_stream import set_status, publish_ready
        set_status(app.db_path, stream_task, "running")
    source_name = str(os.getenv("SOURCE_NAME", "zufang555")).strip()
    target_groups = max(1, int(os.getenv("TARGET_GROUPS", "50") or 50))
    fetch_limit = max(target_groups, int(os.getenv("FETCH_LIMIT", "1200") or 1200))

    sources = load_sources(app.sources_path)
    source_cfg = next(
        (dict(row) for row in sources if str(row.get("source_name", "")).strip() == source_name),
        None,
    )
    if source_cfg is None:
        raise SystemExit(f"source not found: {source_name}")

    client = history_client(app)
    if os.getenv("BACKFILL_READ_ONLY_SESSION") == "1":
        await client.connect()
        if not await client.is_user_authorized():
            await client.disconnect()
            raise RuntimeError('existing_collector_authorization_required')
    else:
        await client.start()
    try:
        entity = await client.get_entity(source_cfg["entity_id"])
        messages: list[Any] = []
        before = max(0, int(os.getenv('BACKFILL_BEFORE_MESSAGE_ID', '0')))
        async for message in client.iter_messages(entity, limit=fetch_limit, offset_id=before):
            messages.append(message)

        grouped: dict[int, list[Any]] = defaultdict(list)
        standalone: list[Any] = []
        for message in messages:
            gid = getattr(message, "grouped_id", None)
            if gid is None:
                standalone.append(message)
            else:
                grouped[int(gid)].append(message)

        units: list[Unit] = [
            Unit("album", sorted(parts, key=lambda m: int(m.id)))
            for parts in grouped.values()
        ]
        units.extend(Unit("single", [message]) for message in standalone)
        units.sort(key=lambda unit: unit.newest_id, reverse=True)

        selected = units[:target_groups]
        if not selected:
            print(f"BACKFILL_RESULT source={source_name} scanned=0 selected=0")
            return

        chat_id = int(selected[0].messages[0].chat_id)
        inserted = 0
        duplicates = 0
        ignored = 0
        failed = 0
        image_count = 0

        # Persist oldest -> newest so downstream review ordering remains natural.
        for unit in (selected if stream_task else reversed(selected)):
            try:
                if unit.kind == "album":
                    parts = unit.messages
                    anchor = parts[0]
                    raw_images: list[dict[str, Any]] = []
                    has_video = False
                    raw_text = ""
                    for message in parts:
                        if getattr(message, "video", None):
                            has_video = True
                        if getattr(message, "media", None):
                            await app.append_supported_media(client, message, raw_images)
                        text = str(getattr(message, "message", "") or "").strip()
                        if text and not raw_text:
                            raw_text = text
                    if not raw_images and not raw_text and not has_video:
                        ignored += 1
                        continue
                    gid = getattr(anchor, "grouped_id", None)
                    result = app.persist(
                        source_cfg=source_cfg,
                        chat_id=chat_id,
                        source_post_id=f"album_{gid}" if gid is not None else f"album_{anchor.id}",
                        anchor_message_id=int(anchor.id),
                        raw_text=raw_text,
                        raw_images=raw_images,
                        grouped_id=int(gid) if gid is not None else None,
                        source_author=sender_label(anchor),
                        ingest_kind="history_album",
                        message_count=len(parts),
                    )
                    image_count += len(raw_images)
                else:
                    message = unit.messages[0]
                    raw_text = str(getattr(message, "message", "") or "")
                    raw_images: list[dict[str, Any]] = []
                    mode = "text"
                    if getattr(message, "media", None):
                        mode = await app.append_supported_media(client, message, raw_images)
                    if mode == "unsupported" and not raw_text:
                        ignored += 1
                        continue
                    result = app.persist(
                        source_cfg=source_cfg,
                        chat_id=int(message.chat_id),
                        source_post_id=str(message.id),
                        anchor_message_id=int(message.id),
                        raw_text=raw_text,
                        raw_images=raw_images,
                        grouped_id=None,
                        source_author=sender_label(message),
                        ingest_kind=f"history_{mode}",
                        message_count=1,
                    )
                    image_count += len(raw_images)

                if result.status == "duplicate":
                    duplicates += 1
                else:
                    inserted += 1
                if stream_task and result.source_post_pk:
                    status = await asyncio.to_thread(publish_ready, app.db_path, stream_task, result.source_post_pk)
                    print(f"STREAM_READY anchor={unit.newest_id} status={status}", flush=True)
            except Exception as exc:
                failed += 1
                print(
                    f"BACKFILL_ERROR source={source_name} newest_id={unit.newest_id} "
                    f"type={type(exc).__name__} error={exc}"
                )

        print(
            "BACKFILL_RESULT "
            f"source={source_name} scanned_messages={len(messages)} "
            f"logical_groups={len(units)} selected={len(selected)} "
            f"inserted={inserted} duplicates={duplicates} ignored={ignored} "
            f"failed={failed} images={image_count}"
        )
    finally:
        await client.disconnect()
        if stream_task:
            set_status(app.db_path, stream_task, "complete")


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
