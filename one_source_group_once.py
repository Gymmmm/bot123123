import asyncio
import os
import re
from collections import defaultdict
from telethon import TelegramClient

import collector_bot as cb


async def collect_one_group() -> None:
    sources = cb.load_sources()
    api_id = int(os.getenv("TG_API_ID", "0") or 0)
    api_hash = (os.getenv("TG_API_HASH", "") or "").strip()
    if not api_id or not api_hash:
        raise RuntimeError("missing_telegram_api_config")

    client = TelegramClient(cb.SESSION_PATH, api_id, api_hash)
    await client.start()
    try:
        for source_cfg in sources:
            entity = await client.get_entity(source_cfg["entity_id"])
            messages = await client.get_messages(entity, limit=150)
            groups = defaultdict(list)
            singles = []
            for message in messages:
                if getattr(message, "grouped_id", None) is not None:
                    groups[int(message.grouped_id)].append(message)
                elif getattr(message, "photo", None):
                    singles.append(message)

            candidates = []
            for gid, group in groups.items():
                group = sorted(group, key=lambda item: item.id)
                photo_count = sum(1 for item in group if getattr(item, "photo", None))
                if photo_count >= cb.MIN_LISTING_IMAGES:
                    candidates.append((group[-1].id, "album", gid, group))
            candidates.sort(reverse=True, key=lambda item: item[0])

            for _, kind, gid, group in candidates:
                raw_images = []
                raw_videos = []
                for message in group:
                    await cb._append_image_or_video(client, message, raw_images, raw_videos)
                raw_text = next(
                    (str(getattr(message, "message", "") or "").strip() for message in group if getattr(message, "message", "")),
                    "",
                )
                # 只把有明确文字和租金线索的相册送入新发布链路；无文字相册仍可由常驻采集器按原策略留存，但本次不拿来做公开测试。
                if not raw_text or not re.search(r"(?:\$|美元|美金|租金|月租|月供|押一付一|押金)", raw_text, flags=re.I):
                    continue
                result = await cb.persist_source_post(
                    client,
                    source_cfg,
                    chat_id=int(entity.id),
                    source_post_id=f"album_{gid}",
                    anchor_message_id=group[0].id,
                    raw_text=raw_text,
                    raw_images=raw_images,
                    raw_videos=raw_videos,
                    grouped_id=gid,
                    source_author=cb._sender_label(group[0]),
                    ingest_kind="album",
                    message_count=len(group),
                    classify_after_insert=True,
                )
                print({"source": source_cfg["source_name"], "grouped_id": gid, "result": result})
                if result.get("status") == "inserted":
                    return

            for message in sorted(singles, key=lambda item: item.id, reverse=True):
                raw_images = []
                raw_videos = []
                await cb._append_image_or_video(client, message, raw_images, raw_videos)
                raw_text = str(getattr(message, "message", "") or "").strip()
                if not raw_text or not re.search(r"(?:\$|美元|美金|租金|月租|月供|押一付一|押金)", raw_text, flags=re.I):
                    continue
                result = await cb.persist_source_post(
                    client,
                    source_cfg,
                    chat_id=int(entity.id),
                    source_post_id=str(message.id),
                    anchor_message_id=message.id,
                    raw_text=str(getattr(message, "message", "") or ""),
                    raw_images=raw_images,
                    raw_videos=raw_videos,
                    grouped_id=None,
                    source_author=cb._sender_label(message),
                    ingest_kind="single",
                    message_count=1,
                    classify_after_insert=True,
                )
                print({"source": source_cfg["source_name"], "message_id": message.id, "result": result})
                if result.get("status") == "inserted":
                    return
        raise RuntimeError("no_new_source_group_found")
    finally:
        await client.disconnect()


if __name__ == "__main__":
    asyncio.run(collect_one_group())
