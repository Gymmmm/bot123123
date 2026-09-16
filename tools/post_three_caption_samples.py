#!/usr/bin/env python3
"""Post two factual listing samples for each caption variant to the configured test channel."""
from __future__ import annotations

import asyncio
import json
import os
import sqlite3
from pathlib import Path

from dotenv import load_dotenv
from telegram import Bot, InputMediaPhoto
from telegram.constants import ParseMode
from telegram.error import BadRequest

ROOT = Path("/opt/qiaolian_dual_bots")


def load_rows() -> list[dict]:
    db_path = Path(os.environ.get("DB_PATH", "data/qiaolian_dual_bot.db"))
    if not db_path.is_absolute():
        db_path = ROOT / db_path
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT d.*, p.cover_path, p.main_images_json, p.package_id,
                   p.cover_template, p.post_text
            FROM publication_packages p
            JOIN drafts d ON d.draft_id = p.draft_id
            WHERE p.status = 'approved'
              AND d.review_status = 'approved'
              AND d.price IS NOT NULL
              AND COALESCE(d.area, '') <> ''
              AND COALESCE(d.layout, '') <> ''
            ORDER BY p.created_at DESC
            LIMIT 30
            """
        ).fetchall()
    selected: list[dict] = []
    for row in rows:
        item = dict(row)
        cover = Path(str(item.get("cover_path") or ""))
        if not cover.is_absolute():
            cover = ROOT / cover
        if not cover.is_file():
            try:
                candidates = json.loads(item.get("main_images_json") or "[]")
            except json.JSONDecodeError:
                candidates = []
            cover = next((Path(x) if Path(x).is_absolute() else ROOT / x for x in candidates if (Path(x) if Path(x).is_absolute() else ROOT / x).is_file()), Path())
        if cover.is_file():
            item["_cover"] = str(cover)
            selected.append(item)
        if len(selected) == 6:
            break
    if len(selected) < 6:
        raise RuntimeError(f"Only {len(selected)} publishable sample packages found")
    return selected


async def main() -> None:
    load_dotenv(ROOT / ".env")
    token = os.environ["PUBLISHER_BOT_TOKEN"]
    channel = os.environ["CHANNEL_ID"]
    expected_username = os.environ.get("CHANNEL_USERNAME", "").strip().lstrip("@").lower()
    bot = Bot(token=token)
    chat = await bot.get_chat(channel)
    actual_username = (chat.username or "").lower()
    if expected_username and actual_username != expected_username:
        raise RuntimeError(f"Refusing target @{actual_username}; expected @{expected_username}")
    if actual_username != "jinbianzufanz":
        raise RuntimeError(f"Refusing non-test channel @{actual_username}")

    rows = load_rows()
    groups = (("a", "字段清晰型"), ("b", "卖点阅读型"), ("c", "极简留白型"))
    sent: list[int] = []
    edit_ids = [2744, 2745, 2747, 2748, 2750, 2751] if os.environ.get("EDIT_EXISTING") == "1" else []
    rebuild_media = os.environ.get("REBUILD_MEDIA") == "1"
    if rebuild_media:
        raise RuntimeError("REBUILD_MEDIA is disabled: approved package media is frozen")
    edit_pos = 0
    for offset, (variant, label) in enumerate(groups):
        if not edit_ids:
            marker = await bot.send_message(channel, f"<b>{label}</b>｜手机样板", parse_mode=ParseMode.HTML)
            sent.append(marker.message_id)
        for item in rows[offset * 2 : offset * 2 + 2]:
            caption = str(item.get("post_text") or "")
            if not caption:
                raise RuntimeError(f"approved package {item['package_id']} has no frozen post_text")
            if edit_ids:
                await bot.edit_message_caption(channel, edit_ids[edit_pos], caption=caption, parse_mode=ParseMode.HTML)
                sent.append(edit_ids[edit_pos])
                edit_pos += 1
                continue
            with open(item["_cover"], "rb") as photo:
                msg = await bot.send_photo(channel, photo=photo, caption=caption, parse_mode=ParseMode.HTML)
            sent.append(msg.message_id)
    print("target", f"@{actual_username}")
    print("message_ids", ",".join(map(str, sent)))


if __name__ == "__main__":
    asyncio.run(main())
