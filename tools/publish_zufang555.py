import sqlite3
import asyncio
import json
import sys
from pathlib import Path
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto
from telegram.constants import ParseMode
from telegram.error import RetryAfter

DB_PATH = "/opt/qiaolian_dual_bots/data/qiaolian_dual_bot.db"
BOT_TOKEN = "8275201351:AAFSKHRaPlLP1LnBvfXLsstuWTUX6Y4iRgk"
CHANNEL_ID = -1003784908965
ADVISOR_TG = "@pengqingw"
USER_BOT_USERNAME = "jinbianzufan_bot"
INTERVAL = 5


def parse_cli_args(argv):
    dry_run = "--dry-run" in argv
    limit = 1
    for i, arg in enumerate(argv):
        if arg == "--limit" and i + 1 < len(argv):
            limit = int(argv[i + 1])
    return dry_run, limit


DRY_RUN, LIMIT = parse_cli_args(sys.argv[1:])

TYPE_ICONS = {
    "公寓": "🏢", "别墅": "🏡", "排屋": "🏘️",
    "联排别墅": "🏘️", "双拼别墅": "🏡", "独栋别墅": "🏠",
}

START_ACTION_ALIASES = {
    "consult": "q",
    "appoint": "a",
    "fav": "f",
    "more": "m",
}

def clean(val):
    if not val or str(val).strip() in ("", "[]", "None"):
        return ""
    return str(val).strip()


def build_start_payload(action, target):
    action_code = START_ACTION_ALIASES.get(action, action)
    return f"{action_code}_{str(target or '').strip()}"

def format_post(row):
    draft_id, title, area, prop_type, price, layout, size_, floor_, deposit, highlights = row
    clean_title = title.split("｜")[0].strip() if title and "｜" in title else (title or "").strip()
    clean_title = clean_title.replace("🇨🇳", "").strip()
    icon = TYPE_ICONS.get(prop_type or "", "🏠")

    lines = [f"{icon} <b>{clean_title}</b>", ""]
    if area:
        lines.append(f"📍 <b>区域：</b>{area}")
    if prop_type:
        lines.append(f"🏷️ <b>类型：</b>{prop_type}")
    if layout:
        lines.append(f"🛏 <b>户型：</b>{layout}")
    if clean(size_):
        lines.append(f"📐 <b>面积：</b>{size_}㎡")
    if clean(floor_):
        lines.append(f"🏬 <b>楼层：</b>{floor_}")
    if price:
        lines.append(f"💰 <b>月租：</b>${price}/月")
    if clean(deposit):
        lines.append(f"🔑 <b>押金：</b>${deposit}")
    if clean(highlights):
        lines.extend(["", f"✨ {highlights}"])
    lines.extend([
        "",
        "─────────────────",
        f"📲 咨询顾问：{ADVISOR_TG}",
        "侨联地产 · 金边靠谱租房服务",
    ])
    return "\n".join(lines)

def get_keyboard(draft_id):
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("💬 立即咨询", url=f"https://t.me/{USER_BOT_USERNAME}?start={build_start_payload('consult', draft_id)}"),
        InlineKeyboardButton("🔖 收藏房源", url=f"https://t.me/{USER_BOT_USERNAME}?start={build_start_payload('fav', draft_id)}"),
    ]])

def get_image_paths(raw_images_json):
    if not raw_images_json:
        return []
    try:
        imgs = json.loads(raw_images_json)
    except Exception:
        return []
    paths = []
    for item in imgs:
        if isinstance(item, dict):
            p = item.get("local_path", "")
        else:
            p = str(item)
        if p and Path(p).exists():
            paths.append(p)
    return paths

async def send_with_retry(coro):
    while True:
        try:
            return await coro
        except RetryAfter as e:
            wait = e.retry_after + 2
            print(f"  [flood] 等待 {wait}s ...")
            await asyncio.sleep(wait)

async def send_with_images(bot, draft_id, text, image_paths, keyboard):
    if not image_paths:
        msg = await send_with_retry(bot.send_message(
            chat_id=CHANNEL_ID,
            text=text,
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard,
        ))
        return msg.message_id

    if len(image_paths) == 1:
        with open(image_paths[0], "rb") as f:
            data = f.read()
        msg = await send_with_retry(bot.send_photo(
            chat_id=CHANNEL_ID,
            photo=data,
            caption=text,
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard,
        ))
        return msg.message_id

    # Multiple images: send as media group (caption on first), then text+keyboard as separate message
    media = []
    for i, p in enumerate(image_paths[:10]):
        with open(p, "rb") as f:
            data = f.read()
        if i == 0:
            media.append(InputMediaPhoto(media=data, caption=text, parse_mode=ParseMode.HTML))
        else:
            media.append(InputMediaPhoto(media=data))
    msgs = await send_with_retry(bot.send_media_group(chat_id=CHANNEL_ID, media=media))
    await asyncio.sleep(2)
    # Send keyboard as separate message linked to album
    await send_with_retry(bot.send_message(
        chat_id=CHANNEL_ID,
        text="👇 点下面按钮继续，不用重新解释是哪套房。",
        reply_markup=keyboard,
        reply_to_message_id=msgs[0].message_id,
    ))
    return msgs[0].message_id

async def main():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        SELECT d.id, d.title, d.area, d.property_type, d.price, d.layout, d.size, d.floor, d.deposit, d.highlights,
               s.raw_images_json
        FROM drafts d
        LEFT JOIN source_posts s ON d.source_post_id = s.id
        WHERE s.source_name = 'zufang555'
          AND d.review_status != 'published'
        ORDER BY d.id ASC
    """)
    rows = cur.fetchall()
    if LIMIT:
        rows = rows[:LIMIT]

    print(f"共 {len(rows)} 条 zufang555 待发房源，{'DRY RUN' if DRY_RUN else '实网发布'}...")

    bot = Bot(token=BOT_TOKEN)
    published = 0

    for row in rows:
        draft_id = row[0]
        raw_images_json = row[10]
        text = format_post(row[:10])
        image_paths = get_image_paths(raw_images_json)

        if DRY_RUN:
            print(f"\n--- draft {draft_id} | {len(image_paths)} 张图 ---\n{text[:120]}...\n---")
            continue

        try:
            first_msg_id = await send_with_images(bot, draft_id, text, image_paths, get_keyboard(draft_id))
            cur.execute(
                "UPDATE drafts SET review_status='published', published_at=datetime('now','localtime') WHERE id=?",
                (draft_id,),
            )
            conn.commit()
            published += 1
            print(f"✓ draft {draft_id}: {(row[1] or '')[:30]}  msg_id={first_msg_id}  图:{len(image_paths)}")
            await asyncio.sleep(INTERVAL)
        except Exception as e:
            print(f"✗ draft {draft_id} 失败: {e}")

    conn.close()
    if not DRY_RUN:
        print(f"\n完成，共发布 {published}/{len(rows)} 条")


if __name__ == "__main__":
    asyncio.run(main())
