#!/usr/bin/env python3
"""把测试频道现有房源帖按 A/B/C 三种现有文案节奏就地更新。

默认只预览；必须显式传 --apply，并且目标频道必须是 Jinbianzufanz。
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sqlite3
import sys
from collections import Counter

from dotenv import load_dotenv
from telegram import Bot
from telegram.constants import ParseMode

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
load_dotenv(os.path.join(ROOT, ".env"))

from meihua_publisher import build_chinese_listing_post, build_keyboard  # noqa: E402


TEST_CHANNEL_USERNAME = "Jinbianzufanz"
ASSIGNMENTS = {
    "a": (2684, 2688, 2692, 2696, 2744),
    "b": (2699, 2700, 2701, 2704, 2707),
    "c": (2711, 2719, 2732, 2750, 2751),
}
SAMPLE_DRAFTS = {
    2744: "DRF_ac30a37d-bc45-49d5-a442-634452e4eb5e",
    2750: "DRF_ff520bea-8813-4aff-9786-93a353be4ab0",
    2751: "DRF_5563edae-3195-48ad-8d99-b751e9823706",
}


def _channel_username() -> str:
    return str(os.getenv("CHANNEL_USERNAME") or TEST_CHANNEL_USERNAME).strip().lstrip("@")


def _load_rows(db_path: str) -> dict[int, dict]:
    wanted = [message_id for ids in ASSIGNMENTS.values() for message_id in ids]
    placeholders = ",".join("?" for _ in wanted)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            f"""
            SELECT p.channel_message_id, p.listing_id AS published_listing_id, d.*
            FROM posts p
            JOIN drafts d ON d.draft_id=p.draft_id
            WHERE CAST(p.channel_message_id AS INTEGER) IN ({placeholders})
            """,
            wanted,
        ).fetchall()
    result: dict[int, dict] = {}
    for row in rows:
        item = dict(row)
        message_id = int(item.pop("channel_message_id"))
        item["listing_id"] = str(item.pop("published_listing_id") or item.get("listing_id") or "")
        result[message_id] = item
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        for message_id, draft_id in SAMPLE_DRAFTS.items():
            if message_id not in wanted or message_id in result:
                continue
            row = conn.execute("SELECT * FROM drafts WHERE draft_id=?", (draft_id,)).fetchone()
            if row is not None:
                item = dict(row)
                item["listing_id"] = str(item.get("listing_id") or f"l_{item.get('id')}")
                result[message_id] = item
    missing = sorted(set(wanted) - set(result))
    if missing:
        raise RuntimeError(f"数据库缺少帖子映射: {missing}")
    return result


async def run(*, apply: bool) -> None:
    channel_username = _channel_username()
    if channel_username.lower() != TEST_CHANNEL_USERNAME.lower():
        raise RuntimeError(f"拒绝操作非测试频道: @{channel_username}")
    db_path = os.getenv("DB_PATH", os.path.join(ROOT, "data", "qiaolian_dual_bot.db"))
    rows = _load_rows(db_path)
    planned = Counter({variant: len(ids) for variant, ids in ASSIGNMENTS.items()})
    print("PLAN", dict(planned), "CHANNEL", f"@{channel_username}")
    if not apply:
        return

    token = str(os.getenv("PUBLISHER_BOT_TOKEN") or "").strip()
    # 维护脚本永远按测试频道公开用户名定位，不信任 .env 中可能变动的 CHANNEL_ID。
    channel_id = f"@{TEST_CHANNEL_USERNAME}"
    if not token:
        raise RuntimeError("PUBLISHER_BOT_TOKEN 未配置")
    bot = Bot(token=token)
    edited: Counter[str] = Counter()
    failures: list[str] = []
    try:
        for variant, message_ids in ASSIGNMENTS.items():
            for message_id in message_ids:
                item = rows[message_id]
                listing_id = str(item.get("listing_id") or "")
                caption = build_chinese_listing_post(item, caption_variant=variant)
                keyboard = build_keyboard(listing_id, str(item.get("area") or "金边"), caption_variant=variant)
                try:
                    await bot.edit_message_caption(
                        chat_id=channel_id,
                        message_id=message_id,
                        caption=caption,
                        parse_mode=ParseMode.HTML,
                        reply_markup=keyboard,
                    )
                    edited[variant] += 1
                    with sqlite3.connect(db_path) as conn:
                        conn.execute(
                            "UPDATE posts SET post_text=? WHERE CAST(channel_message_id AS INTEGER)=?",
                            (caption, message_id),
                        )
                        conn.execute(
                            "UPDATE publish_analytics SET caption_variant=? WHERE message_id=?",
                            (variant, message_id),
                        )
                    print("EDITED", variant.upper(), message_id, listing_id)
                except Exception as exc:  # 逐条继续，最后统一判定是否达标
                    if "message is not modified" in str(exc).lower():
                        edited[variant] += 1
                        print("UNCHANGED", variant.upper(), message_id, listing_id)
                        continue
                    failures.append(f"{variant}:{message_id}:{type(exc).__name__}:{exc}")
                    print("FAILED", failures[-1])
    finally:
        await bot.shutdown()

    print("RESULT", dict(edited))
    if failures or any(edited[v] < 5 for v in ("a", "b", "c")):
        raise RuntimeError(f"未达到每版 5 条: edited={dict(edited)} failures={failures}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    asyncio.run(run(apply=args.apply))
