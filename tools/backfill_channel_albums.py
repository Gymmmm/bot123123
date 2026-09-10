#!/usr/bin/env python3
"""Traverse channel history and feed albums into the production V3 intake chain."""
from __future__ import annotations

import argparse
import asyncio
from collections import defaultdict
from pathlib import Path
from typing import Any

from telethon import TelegramClient

from v3_core.ingest.telegram_collector import (
    TelegramCollectorApp,
    from_environment,
    load_sources,
    sender_label,
)


async def collect_channel(
    client: TelegramClient,
    app: TelegramCollectorApp,
    source: dict[str, Any],
    *,
    scan: int,
    max_new: int,
) -> dict[str, Any]:
    entity = await client.get_entity(source["entity_id"])
    messages = [message async for message in client.iter_messages(entity, limit=scan)]
    groups: dict[int, list[Any]] = defaultdict(list)
    for message in messages:
        if message.grouped_id:
            groups[int(message.grouped_id)].append(message)

    inserted = skipped = duplicates = failed = 0
    for grouped_id, album in sorted(
        groups.items(),
        key=lambda item: max(message.id for message in item[1]),
        reverse=True,
    ):
        if inserted >= max_new:
            break
        album.sort(key=lambda message: message.id)
        raw_images: list[dict[str, Any]] = []
        for message in album:
            if getattr(message, "media", None):
                await app.append_supported_media(client, message, raw_images)
        if len(raw_images) < app.intake.min_listing_images:
            skipped += 1
            continue
        anchor = album[0]
        caption = next((str(message.message or "").strip() for message in album if message.message), "")
        try:
            result = app.persist(
                source_cfg=source,
                chat_id=int(anchor.chat_id),
                source_post_id=f"album_{grouped_id}",
                anchor_message_id=int(anchor.id),
                raw_text=caption,
                raw_images=raw_images,
                grouped_id=grouped_id,
                source_author=sender_label(anchor),
                ingest_kind="historical_album",
                message_count=len(album),
            )
            app._record_result(str(source["source_name"]), result)
        except Exception as exc:
            failed += 1
            print(source["source_name"], anchor.id, len(raw_images), type(exc).__name__, flush=True)
            continue
        if result.status == "inserted":
            inserted += 1
        elif result.status == "duplicate":
            duplicates += 1
        else:
            skipped += 1
        print(source["source_name"], anchor.id, len(raw_images), result.status, flush=True)
    return {
        "channel": str(source["source_name"]),
        "inserted": inserted,
        "duplicates": duplicates,
        "skipped": skipped,
        "failed": failed,
    }


async def run(args: argparse.Namespace) -> None:
    root = Path(__file__).resolve().parents[1]
    app = from_environment(root)
    configured = load_sources(app.sources_path)
    requested = {str(value).strip().lstrip("@").lower() for value in (args.channels or [])}
    sources = [
        source
        for source in configured
        if not requested
        or str(source["source_name"]).lower() in requested
        or str(source["entity_id"]).strip().lstrip("@").lower() in requested
    ]
    if not sources:
        raise RuntimeError("No configured source matched --channels")

    client = TelegramClient(app.session_path, app.api_id, app.api_hash)
    await client.connect()
    if not await client.is_user_authorized():
        raise RuntimeError("Telethon session is not authorized")
    try:
        for source in sources:
            result = await collect_channel(
                client,
                app,
                source,
                scan=max(1, int(args.scan)),
                max_new=max(1, int(args.max_new)),
            )
            print(result, flush=True)
    finally:
        await client.disconnect()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--channels", nargs="*")
    parser.add_argument("--scan", type=int, default=500)
    parser.add_argument("--max-new", type=int, default=20)
    asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    main()
