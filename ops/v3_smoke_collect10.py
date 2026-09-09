from __future__ import annotations

import asyncio
import os
from collections import defaultdict

from telethon import TelegramClient

from collector_bot import (
    SESSION_PATH,
    _append_image_or_video,
    _sender_label,
    load_sources,
    persist_source_post,
)

TARGET = 10
SCAN = 700
MAX_ATTEMPTS = 150


def build_units(messages):
    grouped = defaultdict(list)
    singles = []
    for message in messages:
        grouped_id = getattr(message, "grouped_id", None)
        if grouped_id is None:
            singles.append(message)
        else:
            grouped[grouped_id].append(message)
    units = []
    for parts in grouped.values():
        if any(getattr(item, "media", None) for item in parts):
            units.append(("album", sorted(parts, key=lambda item: item.id)))
    for message in singles:
        if getattr(message, "media", None):
            units.append(("single", [message]))
    units.sort(key=lambda unit: max(item.id for item in unit[1]), reverse=True)
    return units


async def main() -> None:
    client = TelegramClient(
        SESSION_PATH,
        int(os.environ["TG_API_ID"]),
        os.environ["TG_API_HASH"],
    )
    await client.start()
    good = []
    attempts = 0
    try:
        for source in load_sources():
            entity = await client.get_entity(source["entity_id"])
            messages = [item async for item in client.iter_messages(entity, limit=SCAN)]
            if not messages:
                continue
            chat_id = messages[0].chat_id
            for kind, parts in build_units(messages):
                if len(good) >= TARGET or attempts >= MAX_ATTEMPTS:
                    break
                attempts += 1
                images = []
                videos = []
                text = ""
                for message in parts:
                    if message.media:
                        await _append_image_or_video(client, message, images, videos)
                if len(images) < 4:
                    continue
                for message in parts:
                    if (message.message or "").strip():
                        text = (message.message or "").strip()
                        break
                anchor = parts[0]
                grouped_id = getattr(anchor, "grouped_id", None)
                source_post_id = (
                    f"album_{grouped_id}"
                    if kind == "album" and grouped_id is not None
                    else str(anchor.id)
                )
                result = await persist_source_post(
                    client,
                    source,
                    chat_id=chat_id,
                    source_post_id=source_post_id,
                    anchor_message_id=anchor.id,
                    raw_text=text,
                    raw_images=images,
                    raw_videos=videos,
                    grouped_id=grouped_id,
                    source_author=_sender_label(anchor),
                    ingest_kind=kind,
                    message_count=len(parts),
                    classify_after_insert=True,
                )
                print("COLLECT_RESULT", source["source_name"], source_post_id, result)
                classification = result.get("classification") or {}
                if result.get("status") == "inserted" and classification.get("status") == "package_ready":
                    good.append(int(result["post_id"]))
            if len(good) >= TARGET:
                break
    finally:
        await client.disconnect()

    print("COLLECT_GOOD_IDS", good)
    if len(good) < TARGET:
        raise SystemExit(f"only_{len(good)}_publishable_collected")


if __name__ == "__main__":
    asyncio.run(main())
