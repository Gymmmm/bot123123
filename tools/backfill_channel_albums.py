#!/usr/bin/env python3
"""只读频道历史、下载至少 N 图相册并进入现有采集/分类/发布包链路。"""
from __future__ import annotations

import argparse
import asyncio
import os
from collections import defaultdict

from telethon import TelegramClient

import collector_bot as collector


async def collect_channel(client, username: str, scan: int, max_new: int) -> dict:
    entity = await client.get_entity(username)
    messages = [m async for m in client.iter_messages(entity, limit=scan)]
    groups = defaultdict(list)
    for message in messages:
        if message.grouped_id:
            groups[int(message.grouped_id)].append(message)
    inserted = skipped = failed = 0
    source = {
        "source_name": username.lstrip("@"),
        "source_type": "telegram_channel",
        "entity": username,
    }
    for _, album in sorted(groups.items(), key=lambda item: max(m.id for m in item[1]), reverse=True):
        if inserted >= max_new:
            break
        album.sort(key=lambda m: m.id)
        raw_images, raw_videos = [], []
        for message in album:
            await collector._append_image_or_video(client, message, raw_images, raw_videos)
        if len(raw_images) < collector.MIN_LISTING_IMAGES:
            skipped += 1
            continue
        caption = next((m.message for m in album if m.message), "") or ""
        anchor = album[0]
        result = await collector.persist_source_post(
            client, source, chat_id=anchor.chat_id,
            source_post_id=str(anchor.id), anchor_message_id=anchor.id,
            raw_text=caption, raw_images=raw_images, raw_videos=raw_videos,
            grouped_id=anchor.grouped_id, source_author="channel",
            ingest_kind="historical_album", message_count=len(album),
        )
        if result.get("status") == "inserted": inserted += 1
        elif result.get("status") in {"duplicate", "skipped"}: skipped += 1
        else: failed += 1
        print(username, anchor.id, len(raw_images), result.get("status"), flush=True)
    return {"channel": username, "inserted": inserted, "skipped": skipped, "failed": failed}


async def run(args):
    client = TelegramClient(
        collector.SESSION_PATH,
        int(os.getenv("TG_API_ID", "0") or 0),
        os.getenv("TG_API_HASH", ""),
    )
    await client.connect()
    if not await client.is_user_authorized():
        raise RuntimeError("Telethon session is not authorized")
    try:
        for username in args.channels:
            print(await collect_channel(client, username, args.scan, args.max_new), flush=True)
    finally:
        await client.disconnect()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--channels", nargs="+", required=True)
    parser.add_argument("--scan", type=int, default=500)
    parser.add_argument("--max-new", type=int, default=20)
    asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    main()
