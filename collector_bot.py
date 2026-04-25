#!/usr/bin/env python3
"""
侨联采集器（Telethon 用户会话）— 监听指定频道/群，下载媒体写入 source_posts，
供 run_pipeline_autopilot → drafts → 封面 →（预览）→ ready → 定时发帖。

- 单图 / 单视频：NewMessage（跳过 grouped_id，避免与相册重复）
- 多图相册（如 9～11 张）：events.Album，整组写入一条 source_posts，raw_images_json 保序

配置：项目根 sources.json（见 sources.json.example），凭证读 .env 的 TG_API_ID / TG_API_HASH。
首次登录请在终端交互执行：  python3 collector_bot.py
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import sys
import uuid
from collections import Counter
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.tl.types import MessageMediaDocument, MessageMediaPhoto

from db import DatabaseManager

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] collector: %(message)s",
)
logger = logging.getLogger("collector_bot")

DB_PATH = str(Path(os.getenv("DB_PATH", str(BASE_DIR / "data/qiaolian_dual_bot.db"))).resolve())
SOURCES_CONFIG_PATH = Path(
    os.getenv("COLLECTOR_SOURCES_JSON", str(BASE_DIR / "sources.json"))
).resolve()
DOWNLOAD_DIR = Path(
    os.getenv("COLLECTOR_DOWNLOAD_DIR", str(BASE_DIR / "media" / "collector_downloads"))
).resolve()
def _resolve_session_path() -> str:
    """优先 TELETHON_SESSION_PATH；否则 COLLECTOR_SESSION_NAME → telethon_sessions/<name>。"""
    explicit = (os.getenv("TELETHON_SESSION_PATH") or "").strip()
    if explicit:
        return str(Path(explicit).expanduser().resolve())
    name = (os.getenv("COLLECTOR_SESSION_NAME") or "").strip()
    if name:
        return str((BASE_DIR / "telethon_sessions" / name).resolve())
    return str((BASE_DIR / "telethon_sessions" / "qiaolian_collector").resolve())


SESSION_PATH = _resolve_session_path()

DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
Path(SESSION_PATH).parent.mkdir(parents=True, exist_ok=True)

db_manager = DatabaseManager(DB_PATH)
RUN_STATS: Counter[str] = Counter()


def _channel_slug(chat_id: int) -> str:
    s = str(chat_id)
    if s.startswith("-100"):
        return s[4:]
    return s.lstrip("-")


def _message_link(chat_id: int, message_id: int) -> str:
    return f"https://t.me/c/{_channel_slug(chat_id)}/{message_id}"


def _inc_stat(key: str, value: int = 1) -> None:
    RUN_STATS[key] += int(value)


def _stats_digest() -> str:
    keys = (
        "seen",
        "inserted",
        "duplicate",
        "failed",
        "single_events",
        "album_events",
        "media_assets_written",
    )
    return " ".join(f"{k}={RUN_STATS.get(k, 0)}" for k in keys)


def _maybe_log_stats() -> None:
    seen = RUN_STATS.get("seen", 0)
    if seen > 0 and seen % 20 == 0:
        logger.info("采集累计统计 %s", _stats_digest())


async def download_media(client: TelegramClient, message) -> tuple:
    """返回 (local_path, file_hash, tg_file_id, tg_unique) 或全 None。"""
    if not message.media:
        return None, None, None, None

    file_path = None
    file_hash = None
    telegram_file_id = None
    telegram_file_unique_id = None

    try:
        downloaded_path = await client.download_media(message.media, file=str(DOWNLOAD_DIR))
        if not downloaded_path:
            return None, None, None, None
        file_path = str(Path(downloaded_path).resolve())
        with open(file_path, "rb") as f:
            file_hash = hashlib.sha256(f.read()).hexdigest()

        if isinstance(message.media, MessageMediaPhoto):
            telegram_file_id = str(message.media.photo.id)
            telegram_file_unique_id = str(message.media.photo.access_hash)
        elif isinstance(message.media, MessageMediaDocument) and message.media.document:
            telegram_file_id = str(message.media.document.id)
            telegram_file_unique_id = str(message.media.document.access_hash)
    except Exception as e:
        logger.exception("download_media: %s", e)
        return None, None, None, None

    return file_path, file_hash, telegram_file_id, telegram_file_unique_id


def _sender_label(message) -> str:
    s = getattr(message, "sender", None)
    if s is None:
        return "channel"
    un = getattr(s, "username", None) or ""
    fn = getattr(s, "first_name", None) or ""
    return un or fn or "unknown"


async def _append_image_or_video(
    client: TelegramClient, message, raw_images: list, raw_videos: list
) -> str:
    """处理单条 Message 的媒体，返回是否吃掉了 caption（清空主文案用）。"""
    if message.photo:
        fp, fh, fid, fuq = await download_media(client, message)
        if fp:
            raw_images.append(
                {
                    "local_path": fp,
                    "file_hash": fh,
                    "telegram_file_id": fid,
                    "telegram_file_unique_id": fuq,
                    "message_id": message.id,
                }
            )
            return "strip_text"
    elif message.video:
        fp, fh, fid, fuq = await download_media(client, message)
        if fp:
            raw_videos.append(
                {
                    "local_path": fp,
                    "file_hash": fh,
                    "telegram_file_id": fid,
                    "telegram_file_unique_id": fuq,
                    "message_id": message.id,
                }
            )
            return "strip_text"
    elif message.media and isinstance(message.media, MessageMediaDocument):
        doc = message.media.document
        if doc and doc.mime_type and "image" in doc.mime_type:
            fp, fh, fid, fuq = await download_media(client, message)
            if fp:
                raw_images.append(
                    {
                        "local_path": fp,
                        "file_hash": fh,
                        "telegram_file_id": fid,
                        "telegram_file_unique_id": fuq,
                        "message_id": message.id,
                    }
                )
                return "strip_text"
    return "keep_text"


def _write_media_assets(source_post_pk: int, raw_images: list) -> int:
    """封面/相册优先读 media_assets；与 cover_generator 约定 owner_type=source_post。"""
    written = 0
    for i, item in enumerate(raw_images):
        if not isinstance(item, dict):
            continue
        lp = item.get("local_path")
        if not lp:
            continue
        aid = f"AST_{uuid.uuid4().hex[:16].upper()}"
        try:
            db_manager.save_media_asset(
                asset_id=aid,
                owner_type="source_post",
                owner_ref_id=source_post_pk,
                owner_ref_key=str(source_post_pk),
                asset_type="photo",
                source_type="telegram",
                source_url=None,
                source_file_id=item.get("telegram_file_id"),
                local_path=lp,
                file_url=None,
                file_hash=item.get("file_hash"),
                telegram_file_id=item.get("telegram_file_id"),
                telegram_file_unique_id=item.get("telegram_file_unique_id"),
                media_type="photo",
                is_watermarked=0,
                is_cover=1 if i == 0 else 0,
                sort_order=i,
            )
            written += 1
        except Exception:
            logger.exception("save_media_asset failed for %s", lp)
    return written


def _find_existing_post(
    source_type: str,
    source_name: str,
    source_post_id: str,
    dedupe_hash: str,
    source_url: str,
) -> tuple[int | None, str]:
    by_tuple = db_manager._fetch_one(
        "SELECT id FROM source_posts WHERE source_type = ? AND source_name = ? AND source_post_id = ? ORDER BY id DESC LIMIT 1",
        (source_type, source_name, source_post_id),
    )
    if by_tuple:
        return int(by_tuple[0]), "source_tuple"

    by_hash = db_manager._fetch_one(
        "SELECT id FROM source_posts WHERE dedupe_hash = ? ORDER BY id DESC LIMIT 1",
        (dedupe_hash,),
    )
    if by_hash:
        return int(by_hash[0]), "dedupe_hash"

    by_url = db_manager._fetch_one(
        "SELECT id FROM source_posts WHERE source_url = ? ORDER BY id DESC LIMIT 1",
        (source_url,),
    )
    if by_url:
        return int(by_url[0]), "source_url"

    return None, ""


async def persist_source_post(
    client: TelegramClient,
    source_cfg: dict,
    *,
    chat_id: int,
    source_post_id: str,
    anchor_message_id: int,
    raw_text: str,
    raw_images: list,
    raw_videos: list,
    grouped_id: int | None,
    source_author: str = "channel",
    ingest_kind: str = "single",
    message_count: int = 1,
) -> dict[str, Any]:
    source_type = source_cfg.get("source_type", "telegram_channel")
    source_name = source_cfg["source_name"]
    source_db_id = source_cfg.get("source_db_id")
    _inc_stat("seen", 1)
    _inc_stat(f"source.{source_name}.seen", 1)

    dedupe_hash_input = f"{source_type}-{source_name}-{source_post_id}"
    dedupe_hash = hashlib.sha256(dedupe_hash_input.encode("utf-8")).hexdigest()
    source_url = _message_link(chat_id, anchor_message_id)

    existing_id, duplicate_reason = _find_existing_post(
        source_type=source_type,
        source_name=source_name,
        source_post_id=source_post_id,
        dedupe_hash=dedupe_hash,
        source_url=source_url,
    )
    if existing_id:
        _inc_stat("duplicate", 1)
        _inc_stat(f"source.{source_name}.duplicate", 1)
        logger.info(
            "跳过重复 source=%s post=%s reason=%s existing_id=%s",
            source_name,
            source_post_id,
            duplicate_reason,
            existing_id,
        )
        _maybe_log_stats()
        return {"status": "duplicate", "reason": duplicate_reason, "post_id": existing_id}

    meta: dict[str, Any] = {
        "chat_id": str(chat_id),
        "grouped_id": grouped_id,
        "ingest_kind": ingest_kind,
        "message_count": int(message_count),
        "raw_image_count": len(raw_images),
        "raw_video_count": len(raw_videos),
        "anchor_message_id": int(anchor_message_id),
    }

    try:
        post_pk = db_manager.save_source_post(
            source_id=source_db_id,
            source_type=source_type,
            source_name=source_name,
            source_post_id=source_post_id,
            source_url=source_url,
            source_author=source_author,
            raw_text=raw_text or "",
            raw_images_json=raw_images,
            raw_videos_json=raw_videos,
            raw_contact="",
            raw_meta_json=meta,
            dedupe_hash=dedupe_hash,
            parse_status="pending",
        )
        if raw_images:
            written = _write_media_assets(post_pk, raw_images)
            _inc_stat("media_assets_written", written)
        _inc_stat("inserted", 1)
        _inc_stat(f"source.{source_name}.inserted", 1)
        logger.info(
            "已入库 source_posts id=%s source=%s kind=%s messages=%s 图=%s 视频=%s",
            post_pk,
            source_name,
            ingest_kind,
            message_count,
            len(raw_images),
            len(raw_videos),
        )
        _maybe_log_stats()
        return {"status": "inserted", "post_id": post_pk}
    except Exception as e:
        _inc_stat("failed", 1)
        _inc_stat(f"source.{source_name}.failed", 1)
        logger.exception("save_source_post 失败: %s", e)
        _maybe_log_stats()
        return {"status": "failed", "reason": str(e)}


async def handle_single_message(event, source_cfg: dict) -> None:
    message = event.message
    if not message.text and not message.media:
        return
    _inc_stat("single_events", 1)

    raw_text = message.message or ""
    raw_images: list = []
    raw_videos: list = []

    if message.media:
        mode = await _append_image_or_video(event.client, message, raw_images, raw_videos)
        if mode == "strip_text":
            raw_text = ""

    await persist_source_post(
        event.client,
        source_cfg,
        chat_id=event.chat_id,
        source_post_id=str(message.id),
        anchor_message_id=message.id,
        raw_text=raw_text,
        raw_images=raw_images,
        raw_videos=raw_videos,
        grouped_id=getattr(message, "grouped_id", None),
        source_author=_sender_label(message),
        ingest_kind="single",
        message_count=1,
    )


async def handle_album(event, source_cfg: dict) -> None:
    """一组相册（多图）合并为一条 source_posts，顺序与 Telegram 一致。"""
    messages = list(event.messages)
    if not messages:
        return
    _inc_stat("album_events", 1)
    messages.sort(key=lambda m: m.id)
    anchor = messages[0]
    chat_id = event.chat_id

    raw_images: list = []
    raw_videos: list = []
    raw_text = (event.text or event.raw_text or "").strip()

    for msg in messages:
        if not msg.media:
            continue
        await _append_image_or_video(event.client, msg, raw_images, raw_videos)

    gid = getattr(anchor, "grouped_id", None)
    source_post_id = f"album_{gid}" if gid is not None else f"album_{anchor.id}"

    await persist_source_post(
        event.client,
        source_cfg,
        chat_id=chat_id,
        source_post_id=source_post_id,
        anchor_message_id=anchor.id,
        raw_text=raw_text,
        raw_images=raw_images,
        raw_videos=raw_videos,
        grouped_id=gid,
        source_author=_sender_label(anchor),
        ingest_kind="album",
        message_count=len(messages),
    )


def _normalize_source_row(raw: dict, idx: int) -> dict | None:
    """兼容两种 sources.json：新格式 entity_id；旧格式 entity: \"@username\"。"""
    if raw.get("is_enabled") is False:
        return None
    s = dict(raw)
    s.setdefault("source_type", "telegram_channel")
    ent = s.get("entity_id")
    if ent is None and s.get("entity"):
        ent = str(s["entity"]).strip().lstrip("@")
        s["entity_id"] = ent
    if not s.get("source_name") or not s.get("entity_id"):
        logger.warning("跳过无效源（缺 source_name 或 entity/entity_id）: %s", raw)
        return None
    s.setdefault("source_db_id", s.get("source_id", idx + 1))
    return s


def load_sources() -> list[dict]:
    if not SOURCES_CONFIG_PATH.is_file():
        logger.error("缺少配置文件: %s（可复制 sources.json.example）", SOURCES_CONFIG_PATH)
        return []
    data = json.loads(SOURCES_CONFIG_PATH.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError("sources.json 必须是 JSON 数组")
    out: list[dict] = []
    for i, row in enumerate(data):
        if not isinstance(row, dict):
            continue
        n = _normalize_source_row(row, i)
        if n:
            out.append(n)
    return out


async def main_async() -> None:
    enabled = load_sources()
    if not enabled:
        logger.error("sources.json 中没有可用源")
        return

    api_id = int(os.getenv("TG_API_ID", "0") or 0)
    api_hash = (os.getenv("TG_API_HASH", "") or "").strip()
    if not api_id or not api_hash:
        logger.error("请在 .env 设置 TG_API_ID 与 TG_API_HASH（my.telegram.org）")
        sys.exit(1)

    client = TelegramClient(SESSION_PATH, api_id, api_hash)
    await client.start()
    logger.info("Telethon 已连接 session=%s", SESSION_PATH)

    for idx, sc in enumerate(enabled):
        sc = dict(sc)
        entity_id = sc["entity_id"]
        try:
            entity = await client.get_entity(entity_id)
        except Exception as e:
            logger.error("无法解析 entity %s (%s): %s", sc.get("source_name"), entity_id, e)
            continue

        logger.info("监听: %s → %s", sc["source_name"], getattr(entity, "title", entity_id))

        @client.on(events.NewMessage(chats=entity, func=lambda e: e.grouped_id is None))
        async def _on_single(event, cfg=sc):
            await handle_single_message(event, cfg)

        @client.on(events.Album(chats=entity))
        async def _on_album(event, cfg=sc):
            await handle_album(event, cfg)

    logger.info("采集器运行中；下载目录 %s", DOWNLOAD_DIR)
    await client.run_until_disconnected()


def main() -> None:
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        logger.info("已退出")


if __name__ == "__main__":
    main()
