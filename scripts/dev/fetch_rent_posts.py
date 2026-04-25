"""
fetch_rent_posts.py
一次性抓取脚本：从三个频道各抓取30条出租相关消息，写入 source_posts 表。
"""
import os
import sys
import json
import hashlib
import asyncio
import logging
from pathlib import Path
from datetime import datetime, timezone

BASE_DIR = Path(__file__).parent.resolve()

# 加载 .env
from dotenv import load_dotenv
load_dotenv(BASE_DIR / ".env")

sys.path.insert(0, str(BASE_DIR))
from db import DatabaseManager

# 联系方式清洗（可选）
try:
    from contact_cleaner import clean_contact_info
except ImportError:
    def clean_contact_info(text, log=False): return text

from telethon import TelegramClient
from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ── 配置 ──────────────────────────────────────────────────
TG_API_ID    = int(os.getenv("TG_API_ID", "0"))
TG_API_HASH  = os.getenv("TG_API_HASH", "")
SESSION_PATH = str(BASE_DIR / "v2" / "qiaolian_crawler_session")
MEDIA_DIR    = BASE_DIR / "media"
DB_PATH      = os.getenv("DB_PATH", "data/qiaolian_dual_bot.db")

SOURCES = [
    {"source_name": "jinbianfangchanzushou", "entity": "@jinbianfangchanzushou"},
    {"source_name": "pprealestate_property", "entity": "@pprealestate_property"},
    {"source_name": "zufang555",             "entity": "@zufang555"},
    {"source_name": "pacific_real_estate",   "entity": "@jinbianzufangUP3888"},
    {"source_name": "cbre_cambodia",           "entity": "@cbrecambodia"},
    {"source_name": "ips_cambodia",            "entity": "@IPSCambodia"},
    {"source_name": "century21_fuji",          "entity": "@wellwin_investment"},
    {"source_name": "knight_frank",           "entity": "@RealEstateInformationSharing"},
    {"source_name": "cambo_housing",          "entity": "@RentHomePhnomPenh"},
]

# 出租关键词过滤
RENT_KEYWORDS = [
    "出租", "租", "rent", "zufang", "月租", "押", "付",
    "$/月", "美元", "USD", "$", "bedroom", "BR", "房", "公寓", "别墅", "套房"
]
FETCH_LIMIT   = 200   # 每个频道最多扫描200条，从中筛出30条出租
TARGET_COUNT  = 30    # 每个频道目标 30 条
SKIP_MEDIA    = False  # 跳过媒体下载，仅抓文字快速入库

def is_rent_related(text: str) -> bool:
    if not text:
        return False
    text_lower = text.lower()
    return any(kw.lower() in text_lower for kw in RENT_KEYWORDS)


def make_dedupe_hash(source_type: str, source_name: str, source_post_id: int) -> str:
    raw = f"{source_type}::{source_name}::{source_post_id}"
    return hashlib.sha256(raw.encode()).hexdigest()


def is_duplicate(db: DatabaseManager, dedupe_hash: str) -> bool:
    result = db._fetch_one(
        "SELECT id FROM source_posts WHERE dedupe_hash = ?", (dedupe_hash,)
    )
    return result is not None


async def download_media(client, msg, source_name: str) -> tuple[list, list]:
    """下载消息媒体，返回 (images_list, videos_list)"""
    images, videos = [], []
    if not msg.media:
        return images, videos

    try:
        if isinstance(msg.media, MessageMediaPhoto):
            photo_dir = MEDIA_DIR / "photos" / source_name
            photo_dir.mkdir(parents=True, exist_ok=True)
            fname = f"{source_name}_{msg.id}.jpg"
            fpath = photo_dir / fname
            if not fpath.exists():
                await client.download_media(msg, file=str(fpath))
            images.append(str(fpath))

        elif isinstance(msg.media, MessageMediaDocument):
            doc = msg.media.document
            mime = doc.mime_type if doc else ""
            if mime.startswith("video"):
                video_dir = MEDIA_DIR / "videos" / source_name
                video_dir.mkdir(parents=True, exist_ok=True)
                ext = mime.split("/")[-1] if "/" in mime else "mp4"
                fname = f"{source_name}_{msg.id}.{ext}"
                fpath = video_dir / fname
                if not fpath.exists():
                    await client.download_media(msg, file=str(fpath))
                videos.append(str(fpath))
    except Exception as e:
        logger.warning(f"媒体下载失败 msg_id={msg.id}: {e}")

    return images, videos


async def fetch_channel(client, source: dict, db: DatabaseManager):
    source_name = source["source_name"]
    entity_str  = source["entity"]
    source_type = "telegram_channel"

    logger.info(f"[{source_name}] 开始抓取，目标：{TARGET_COUNT} 条出租消息...")

    try:
        entity = await client.get_entity(entity_str)
        channel_username = entity_str.lstrip("@")
    except Exception as e:
        logger.error(f"[{source_name}] 获取频道失败: {e}")
        return 0

    saved = 0
    skipped_dup = 0
    skipped_non_rent = 0
    scanned = 0

    # media_group 缓冲区：{media_group_id: [msg, ...]}
    group_buf: dict[str, list] = {}

    async for msg in client.iter_messages(entity, limit=FETCH_LIMIT):
        scanned += 1
        if saved >= TARGET_COUNT:
            break

        text = (msg.message or "").strip()

        # media_group 合并
        if msg.grouped_id:
            gid = str(msg.grouped_id)
            if gid not in group_buf:
                group_buf[gid] = []
            group_buf[gid].append(msg)
            continue

        # 单条消息处理
        if not is_rent_related(text):
            skipped_non_rent += 1
            continue

        source_post_id = msg.id
        dedupe_hash = make_dedupe_hash(source_type, source_name, source_post_id)

        if is_duplicate(db, dedupe_hash):
            skipped_dup += 1
            continue

        # 清洗联系方式
        clean_text = clean_contact_info(text, log=False)

        # 下载媒体
        if SKIP_MEDIA:
            images, videos = [], []
        else:
            images, videos = await download_media(client, msg, source_name)

        raw_meta = {
            "msg_date": msg.date.isoformat() if msg.date else None,
            "views": getattr(msg, "views", None),
            "forwards": getattr(msg, "forwards", None),
        }

        # 提取原始联系方式（存档用）
        try:
            from contact_cleaner import extract_contacts
            contacts = extract_contacts(text)
            raw_contact = json.dumps(contacts, ensure_ascii=False)
        except Exception:
            raw_contact = "{}"

        post_id = db.save_source_post(
            source_id=None,
            source_type=source_type,
            source_name=source_name,
            source_post_id=source_post_id,
            source_url=f"https://t.me/{channel_username}/{source_post_id}",
            source_author=None,
            raw_text=clean_text,
            raw_images_json=images,
            raw_videos_json=videos,
            raw_contact=raw_contact,
            raw_meta_json=raw_meta,
            dedupe_hash=dedupe_hash,
        )

        if post_id:
            # 写入 media_assets
            for i, img_path in enumerate(images):
                db.save_media_asset(
                    source_post_id=post_id,
                    file_path=img_path,
                    media_type="image",
                    sort_order=i,
                )
            for i, vid_path in enumerate(videos):
                db.save_media_asset(
                    source_post_id=post_id,
                    file_path=vid_path,
                    media_type="video",
                    sort_order=i,
                )
            saved += 1
            logger.info(f"[{source_name}] ✅ 已保存 {saved}/{TARGET_COUNT} msg_id={source_post_id} text={clean_text[:40]!r}")

    # 处理 media_group 缓冲区
    for gid, msgs in group_buf.items():
        if saved >= TARGET_COUNT:
            break

        # 合并文本（取最长的那条）
        texts = [m.message or "" for m in msgs if m.message]
        combined_text = max(texts, key=len) if texts else ""

        if not is_rent_related(combined_text):
            skipped_non_rent += 1
            continue

        primary = msgs[0]
        source_post_id = primary.id
        dedupe_hash = make_dedupe_hash(source_type, source_name, source_post_id)

        if is_duplicate(db, dedupe_hash):
            skipped_dup += 1
            continue

        clean_text = clean_contact_info(combined_text, log=False)

        # 下载所有图片
        all_images, all_videos = [], []
        if not SKIP_MEDIA:
            for m in msgs:
                imgs, vids = await download_media(client, m, source_name)
                all_images.extend(imgs)
                all_videos.extend(vids)

        raw_meta = {
            "msg_date": primary.date.isoformat() if primary.date else None,
            "media_group_id": gid,
            "group_size": len(msgs),
        }

        try:
            from contact_cleaner import extract_contacts
            contacts = extract_contacts(combined_text)
            raw_contact = json.dumps(contacts, ensure_ascii=False)
        except Exception:
            raw_contact = "{}"

        post_id = db.save_source_post(
            source_id=None,
            source_type=source_type,
            source_name=source_name,
            source_post_id=source_post_id,
            source_url=f"https://t.me/{channel_username}/{source_post_id}",
            source_author=None,
            raw_text=clean_text,
            raw_images_json=all_images,
            raw_videos_json=all_videos,
            raw_contact=raw_contact,
            raw_meta_json=raw_meta,
            dedupe_hash=dedupe_hash,
        )

        if post_id:
            for i, img_path in enumerate(all_images):
                db.save_media_asset(source_post_id=post_id, file_path=img_path, media_type="image", sort_order=i)
            saved += 1
            logger.info(f"[{source_name}] ✅ 已保存(group) {saved}/{TARGET_COUNT} gid={gid} text={clean_text[:40]!r}")

    logger.info(f"[{source_name}] 完成：保存={saved}，跳过重复={skipped_dup}，非出租={skipped_non_rent}，扫描={scanned}")
    return saved


async def main():
    if not TG_API_ID or not TG_API_HASH:
        logger.error("TG_API_ID 或 TG_API_HASH 未设置")
        sys.exit(1)

    db = DatabaseManager(DB_PATH)
    total_before = db._fetch_one("SELECT COUNT(*) FROM source_posts")[0]
    logger.info(f"抓取前 source_posts 总数：{total_before}")

    client = TelegramClient(SESSION_PATH, TG_API_ID, TG_API_HASH)
    await client.start()
    logger.info("Telegram 登录成功")

    total_saved = 0
    for source in SOURCES:
        n = await fetch_channel(client, source, db)
        total_saved += n
        await asyncio.sleep(3)  # 频道间间隔3秒

    await client.disconnect()

    total_after = db._fetch_one("SELECT COUNT(*) FROM source_posts")[0]
    logger.info(f"\n{'='*50}")
    logger.info(f"抓取完成！新增：{total_saved} 条，source_posts 总数：{total_after}")

    # 打印各频道统计
    rows = db._fetch_all(
        "SELECT source_name, parse_status, COUNT(*) FROM source_posts GROUP BY source_name, parse_status ORDER BY source_name"
    )
    logger.info("\n各频道统计：")
    for row in rows:
        logger.info(f"  {row[0]:30s} {row[1]:10s} {row[2]} 条")


if __name__ == "__main__":
    asyncio.run(main())
