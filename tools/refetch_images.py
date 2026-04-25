#!/usr/bin/env python3
import asyncio
import hashlib
import json
import os
import sqlite3
import sys
import uuid
from pathlib import Path

from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.tl.types import MessageMediaDocument, MessageMediaPhoto

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

DB_PATH = str(BASE_DIR / "data/qiaolian_dual_bot.db")
DOWNLOAD_DIR = BASE_DIR / "media" / "collector_downloads"
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

SESSION_PATH = os.getenv("TELETHON_SESSION_PATH", str(BASE_DIR / "v2/qiaolian_crawler_session"))
API_ID = int(os.getenv("TG_API_ID", "0"))
API_HASH = os.getenv("TG_API_HASH", "")
CHANNEL = "@zufang555"

DRY_RUN = "--dry-run" in sys.argv

async def download_media(client, message):
    if not message.media:
        return None, None, None, None
    try:
        path = await client.download_media(message.media, file=str(DOWNLOAD_DIR))
        if not path:
            return None, None, None, None
        path = str(Path(path).resolve())
        with open(path, "rb") as f:
            fhash = hashlib.sha256(f.read()).hexdigest()
        if isinstance(message.media, MessageMediaPhoto):
            fid = str(message.media.photo.id)
            fuq = str(message.media.photo.access_hash)
        elif isinstance(message.media, MessageMediaDocument) and message.media.document:
            fid = str(message.media.document.id)
            fuq = str(message.media.document.access_hash)
        else:
            fid = fuq = None
        return path, fhash, fid, fuq
    except Exception as e:
        print(f"  download error: {e}")
        return None, None, None, None

def get_msg_id(source_post_id, source_url):
    if str(source_post_id).isdigit():
        return int(source_post_id)
    # album_xxx: extract from source_url like https://t.me/c/2498584369/6658
    if source_url:
        parts = source_url.rstrip("/").split("/")
        if parts[-1].isdigit():
            return int(parts[-1])
    return None

async def fetch_images_for(client, msg_id):
    raw_images = []
    # for albums, also check nearby messages (grouped_id links them)
    ids_to_check = list(range(msg_id, msg_id + 12))
    messages = await client.get_messages(CHANNEL, ids=ids_to_check)
    if not messages:
        return raw_images
    msgs = messages if isinstance(messages, list) else [messages]
    first_grouped = None
    for msg in msgs:
        if not msg or not msg.media:
            continue
        if first_grouped is None and hasattr(msg, "grouped_id") and msg.grouped_id:
            first_grouped = msg.grouped_id
        if first_grouped and hasattr(msg, "grouped_id") and msg.grouped_id != first_grouped:
            break
        if msg.id == msg_id or (first_grouped and getattr(msg, "grouped_id", None) == first_grouped):
            fp, fh, fid, fuq = await download_media(client, msg)
            if fp:
                raw_images.append({
                    "local_path": fp,
                    "file_hash": fh,
                    "telegram_file_id": fid,
                    "telegram_file_unique_id": fuq,
                    "message_id": msg.id,
                })
    return raw_images

async def main():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT id, source_post_id, source_url FROM source_posts WHERE source_name='zufang555' ORDER BY id")
    rows = cur.fetchall()
    print(f"共 {len(rows)} 条 zufang555 源帖需要补图")

    client = TelegramClient(SESSION_PATH, API_ID, API_HASH)
    await client.start()

    updated = 0
    for sp_id, source_post_id, source_url in rows:
        msg_id = get_msg_id(source_post_id, source_url)
        if not msg_id:
            print(f"source_post {sp_id}: 无法解析 msg_id，跳过")
            continue

        print(f"source_post {sp_id} / msg {msg_id} ...", end=" ", flush=True)
        try:
            raw_images = await fetch_images_for(client, msg_id)
            if not raw_images:
                # fallback: single message
                msgs = await client.get_messages(CHANNEL, ids=msg_id)
                m = msgs if not isinstance(msgs, list) else (msgs[0] if msgs else None)
                if m and m.media:
                    fp, fh, fid, fuq = await download_media(client, m)
                    if fp:
                        raw_images.append({"local_path": fp, "file_hash": fh,
                            "telegram_file_id": fid, "telegram_file_unique_id": fuq, "message_id": m.id})

            if not raw_images:
                print("无图片")
                continue

            print(f"{len(raw_images)} 张图")
            if DRY_RUN:
                continue

            cur.execute("UPDATE source_posts SET raw_images_json=? WHERE id=?",
                (json.dumps(raw_images, ensure_ascii=False), sp_id))

            # also reset related drafts so pipeline re-processes them
            cur.execute("""UPDATE drafts SET review_status='pending'
                WHERE source_post_id=? AND review_status IN ('pending','missing_real_media')""", (sp_id,))

            for i, item in enumerate(raw_images):
                aid = f"AST_{uuid.uuid4().hex[:16].upper()}"
                try:
                    cur.execute("""INSERT OR IGNORE INTO media_assets
                        (asset_id, owner_type, owner_ref_id, owner_ref_key, asset_type,
                         source_type, source_file_id, local_path, file_hash,
                         telegram_file_id, telegram_file_unique_id, media_type,
                         is_watermarked, is_cover, sort_order, created_at, updated_at)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,datetime('now','localtime'),datetime('now','localtime'))""",
                        (aid, "source_post", sp_id, str(sp_id), "photo",
                         "telegram", item.get("telegram_file_id"), item["local_path"], item["file_hash"],
                         item.get("telegram_file_id"), item.get("telegram_file_unique_id"), "photo",
                         0, 1 if i == 0 else 0, i))
                except Exception as e:
                    print(f"  media_assets insert error: {e}")
            conn.commit()
            updated += 1
            await asyncio.sleep(1)
        except Exception as e:
            print(f"错误: {e}")

    await client.disconnect()
    conn.close()
    print(f"\nDone: {updated}/{len(rows)} 条已补图")

asyncio.run(main())
