#!/usr/bin/env python3
"""
从各采集源频道拉取近期历史消息，按「单帖 / 相册」成组写入 source_posts（与 collector_bot 同库同规则）。
默认每频道最多 PER_CHANNEL 组，且只统计带图片或视频的组（与房源采集一致）。

用法（在服务器项目根、加载 .env）：
  systemctl stop qiaolian-collector.service
  python3 backfill_collector_history.py
  systemctl start qiaolian-collector.service

环境变量：
  PER_CHANNEL   每频道最多少组，默认 20
  FETCH_LIMIT   每条频道最多扫描多少条 Message，默认 800
"""
from __future__ import annotations

import asyncio
import os
import sys
from collections import defaultdict
from pathlib import Path

from dotenv import load_dotenv
from telethon import TelegramClient

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

# 在导入 collector_bot 之前确保工作目录与 env 已就绪
os.chdir(BASE_DIR)
sys.path.insert(0, str(BASE_DIR))

from collector_bot import (  # noqa: E402
    SESSION_PATH,
    _append_image_or_video,
    _sender_label,
    load_sources,
    logger,
    persist_source_post,
)

PER_CHANNEL = int(os.getenv("PER_CHANNEL", "20"))
FETCH_LIMIT = int(os.getenv("FETCH_LIMIT", "800"))


def _partition_messages(messages: list) -> tuple[dict, list]:
    """返回 (grouped_id -> [msg], standalone_msgs)。带 grouped_id 的只进相册，不重复进 standalone。"""
    by_gid: dict = defaultdict(list)
    standalone: list = []
    for m in messages:
        gid = getattr(m, "grouped_id", None)
        if gid is not None:
            by_gid[gid].append(m)
        else:
            standalone.append(m)
    return by_gid, standalone


def _unit_has_media(parts: list) -> bool:
    return any(getattr(p, "media", None) for p in parts)


async def _persist_album(client: TelegramClient, source_cfg: dict, chat_id: int, parts: list) -> None:
    parts = sorted(parts, key=lambda m: m.id)
    anchor = parts[0]
    gid = getattr(anchor, "grouped_id", None)
    raw_images: list = []
    raw_videos: list = []
    raw_text = ""
    for msg in parts:
        if msg.media:
            await _append_image_or_video(client, msg, raw_images, raw_videos)
    for msg in parts:
        t = (msg.message or "").strip()
        if t:
            raw_text = t
            break
    source_post_id = f"album_{gid}" if gid is not None else f"album_{anchor.id}"
    await persist_source_post(
        client,
        source_cfg,
        chat_id=chat_id,
        source_post_id=source_post_id,
        anchor_message_id=anchor.id,
        raw_text=raw_text,
        raw_images=raw_images,
        raw_videos=raw_videos,
        grouped_id=gid,
        source_author=_sender_label(anchor),
    )


async def _persist_single(client: TelegramClient, source_cfg: dict, chat_id: int, message) -> None:
    raw_text = message.message or ""
    raw_images: list = []
    raw_videos: list = []
    if message.media:
        mode = await _append_image_or_video(client, message, raw_images, raw_videos)
        if mode == "strip_text":
            raw_text = ""
    await persist_source_post(
        client,
        source_cfg,
        chat_id=chat_id,
        source_post_id=str(message.id),
        anchor_message_id=message.id,
        raw_text=raw_text,
        raw_images=raw_images,
        raw_videos=raw_videos,
        grouped_id=getattr(message, "grouped_id", None),
        source_author=_sender_label(message),
    )


async def backfill_one(
    client: TelegramClient,
    source_cfg: dict,
    *,
    per_channel: int,
    fetch_limit: int,
) -> int:
    """返回本频道实际尝试写入的组数（含被去重跳过的）。"""
    entity_id = source_cfg["entity_id"]
    entity = await client.get_entity(entity_id)
    name = source_cfg.get("source_name", entity_id)

    messages: list = []
    async for m in client.iter_messages(entity, limit=fetch_limit):
        messages.append(m)

    if not messages:
        logger.info("[%s] 无消息，跳过", name)
        return 0

    chat_id = messages[0].chat_id
    by_gid, standalone = _partition_messages(messages)

    units: list[tuple[str, list]] = []
    for gid, parts in by_gid.items():
        if _unit_has_media(parts):
            units.append(("album", parts))
    for m in standalone:
        if m.media:
            units.append(("single", [m]))

    def _recency(u: tuple[str, list]) -> int:
        return max(x.id for x in u[1])

    units.sort(key=_recency, reverse=True)
    cand = len(units)
    units = units[:per_channel]

    logger.info(
        "[%s] 扫描 %s 条消息，含媒体组共 %s，取最近 %s 组写入",
        name,
        len(messages),
        cand,
        len(units),
    )

    n = 0
    for kind, parts in units:
        try:
            if kind == "album":
                await _persist_album(client, source_cfg, chat_id, parts)
            else:
                await _persist_single(client, source_cfg, chat_id, parts[0])
            n += 1
        except Exception:
            logger.exception("[%s] 写入失败 kind=%s", name, kind)
    return n


async def main_async() -> None:
    sources = load_sources()
    if not sources:
        logger.error("sources.json 无可用源")
        return

    api_id = int(os.getenv("TG_API_ID", "0") or 0)
    api_hash = (os.getenv("TG_API_HASH", "") or "").strip()
    if not api_id or not api_hash:
        logger.error("请配置 TG_API_ID / TG_API_HASH")
        sys.exit(1)

    client = TelegramClient(SESSION_PATH, api_id, api_hash)
    await client.start()

    total = 0
    for sc in sources:
        try:
            c = await backfill_one(
                client,
                sc,
                per_channel=PER_CHANNEL,
                fetch_limit=FETCH_LIMIT,
            )
            total += c
        except Exception:
            logger.exception("频道失败: %s", sc.get("source_name"))

    logger.info("补采结束，各源已处理（含跳过重复）合计约 %s 组", total)
    await client.disconnect()


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
