#!/usr/bin/env python3
"""按频道消息号从小到大，重新编辑数据库记录中的全部房源帖。

默认只预演；必须显式传入 --apply。脚本不删除、不重发、不修改媒体。
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sqlite3
import sys

from dotenv import load_dotenv
from telegram import Bot
from telegram.constants import ParseMode
from telegram.error import BadRequest, RetryAfter

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
load_dotenv(os.path.join(ROOT, ".env"))

from meihua_publisher import build_chinese_listing_post, build_keyboard  # noqa: E402


EXPECTED_CHANNEL = "Jinbianzufanz"


def load_rows(db_path: str) -> list[dict]:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT p.id AS post_row_id, p.listing_id, p.channel_message_id,
                   p.publish_status, p.post_text,
                   l.title, l.property_type, l.area, l.community, l.price,
                   l.layout, l.size_sqm, l.highlights, l.deposit_rule,
                   l.available_date, l.status, l.canonical_provenance_json,
                   l.quality_json
              FROM posts p
              JOIN listings l ON l.listing_id = p.listing_id
             WHERE cast(coalesce(p.channel_message_id, '0') AS integer) > 0
             ORDER BY cast(p.channel_message_id AS integer), p.id
            """
        ).fetchall()
    result = []
    for row in rows:
        d = dict(row)
        d.update({
            "room_type": d.get("layout"),
            "size": d.get("size_sqm"),
            "deposit": d.get("deposit_rule"),
            "normalized_data": d.get("canonical_provenance_json") or "{}",
        })
        result.append(d)
    return result


async def run(*, apply: bool) -> None:
    channel = str(os.getenv("CHANNEL_USERNAME") or EXPECTED_CHANNEL).strip().lstrip("@")
    if channel.lower() != EXPECTED_CHANNEL.lower():
        raise RuntimeError(f"拒绝操作非目标频道：@{channel}")
    db_path = os.getenv("DB_PATH", os.path.join(ROOT, "data", "qiaolian_dual_bot.db"))
    rows = load_rows(db_path)
    if len(rows) != 61:
        raise RuntimeError(f"帖子数量已变化，预期 61，实际 {len(rows)}；拒绝继续")
    plan = []
    for item in rows:
        caption = build_chinese_listing_post(item, caption_variant="a")
        keyboard = build_keyboard(str(item["listing_id"]), str(item.get("area") or ""))
        labels = [[button.text for button in row] for row in keyboard.inline_keyboard]
        if labels != [["📋 租赁详情", "📸 更多实拍"], ["📅 预约看房"]]:
            raise RuntimeError(f"CTA 不符合冻结规范：{item['channel_message_id']} {labels}")
        plan.append({
            "message_id": int(item["channel_message_id"]),
            "property_id": str(item["listing_id"]),
            "listing_status": str(item.get("status") or ""),
            "record_status": str(item.get("publish_status") or ""),
            "caption": caption,
            "caption_length": len(caption),
        })
    print("PLAN", json.dumps([{k: x[k] for k in ("message_id", "property_id", "listing_status", "record_status", "caption_length")} for x in plan], ensure_ascii=False))
    if not apply:
        return

    token = str(os.getenv("PUBLISHER_BOT_TOKEN") or "").strip()
    if not token:
        raise RuntimeError("PUBLISHER_BOT_TOKEN 未配置")
    bot = Bot(token=token)
    success = []
    failures = []
    try:
        for item, planned in zip(rows, plan):
            message_id = planned["message_id"]
            keyboard = build_keyboard(str(item["listing_id"]), str(item.get("area") or ""))
            result = ""
            for attempt in range(1, 4):
                try:
                    await bot.edit_message_caption(
                        chat_id=f"@{EXPECTED_CHANNEL}",
                        message_id=message_id,
                        caption=planned["caption"],
                        parse_mode=ParseMode.HTML,
                        reply_markup=keyboard,
                    )
                    result = "edited_caption"
                    break
                except RetryAfter as exc:
                    if attempt >= 3:
                        failures.append({"message_id": message_id, "property_id": item["listing_id"], "error": f"RetryAfter: {exc}"})
                        print("FAILED", message_id, item["listing_id"], "RetryAfter", str(exc))
                        break
                    wait_seconds = int(exc.retry_after.total_seconds() if hasattr(exc.retry_after, "total_seconds") else exc.retry_after) + 2
                    print("WAIT", message_id, wait_seconds)
                    await asyncio.sleep(wait_seconds)
                    continue
                except BadRequest as exc:
                    lowered = str(exc).lower()
                    if "message is not modified" in lowered:
                        result = "unchanged"
                    elif "there is no caption" in lowered or "message text is empty" in lowered:
                        try:
                            await bot.edit_message_text(
                                chat_id=f"@{EXPECTED_CHANNEL}",
                                message_id=message_id,
                                text=planned["caption"],
                                parse_mode=ParseMode.HTML,
                                reply_markup=keyboard,
                            )
                            result = "edited_text"
                        except Exception as text_exc:
                            failures.append({"message_id": message_id, "property_id": item["listing_id"], "error": f"{type(text_exc).__name__}: {text_exc}"})
                            print("FAILED", message_id, item["listing_id"], type(text_exc).__name__, str(text_exc))
                    else:
                        failures.append({"message_id": message_id, "property_id": item["listing_id"], "error": f"BadRequest: {exc}"})
                        print("FAILED", message_id, item["listing_id"], "BadRequest", str(exc))
                    break
                except Exception as exc:
                    failures.append({"message_id": message_id, "property_id": item["listing_id"], "error": f"{type(exc).__name__}: {exc}"})
                    print("FAILED", message_id, item["listing_id"], type(exc).__name__, str(exc))
                    break
            if not result:
                await asyncio.sleep(1.2)
                continue

            with sqlite3.connect(db_path) as conn:
                conn.execute("UPDATE posts SET post_text=?, updated_at=CURRENT_TIMESTAMP WHERE id=?", (planned["caption"], item["post_row_id"]))
            success.append({"message_id": message_id, "property_id": item["listing_id"], "result": result})
            print("OK", message_id, item["listing_id"], result)
            await asyncio.sleep(1.2)
    finally:
        await bot.shutdown()
    print("SUMMARY", json.dumps({"success": len(success), "failed": len(failures), "failures": failures}, ensure_ascii=False))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    asyncio.run(run(apply=args.apply))
