import os
import json
import asyncio
from pathlib import Path
from datetime import datetime
from telegram import Bot, InputMediaPhoto, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.constants import ParseMode
from telegram.error import TelegramError

# 配置从环境变量读取（假设由调用方或 env 加载）
BOT_TOKEN    = os.getenv("PUBLISHER_BOT_TOKEN")
CHANNEL_ID   = os.getenv("CHANNEL_ID")
BOT_USERNAME = os.getenv("USER_BOT_USERNAME")

START_ACTION_ALIASES = {
    "consult": "q",
    "appoint": "a",
    "fav": "f",
    "more": "m",
}


def build_start_payload(action: str, target: str) -> str:
    action_code = START_ACTION_ALIASES.get(action, action)
    return f"{action_code}_{str(target or '').strip()}"

def build_caption(listing: dict) -> str:
    """
    第一张图片的 caption，参考 qiaolian_house_tool.html 的简洁风格进行增强。
    包含：项目名、户型、价格、面积、楼层、区域、卖点。
    """
    project    = (listing.get("project") or "").strip()
    layout     = (listing.get("layout") or "").strip()
    price      = (listing.get("price") or "").strip()
    size       = (listing.get("size") or "").strip()
    floor      = (listing.get("floor") or "").strip()
    area       = (listing.get("area") or "").strip()
    
    # 处理价格格式，确保有 $ 符号
    if price and not price.startswith('$'):
        price = f"${price}"
    if price and "/月" not in price:
        price = f"{price}/月"

    # 1. 核心标题行 (参考 HTML 工具：项目名 + 户型)
    title_parts = []
    if project: title_parts.append(project)
    if layout: title_parts.append(layout)
    header = f"🏠【侨联实拍】{' · '.join(title_parts)}" if title_parts else "🏠【侨联实拍】优质房源"

    # 2. 详细参数行 (参考 HTML 工具的输入项)
    info_lines = []
    if price:
        info_lines.append(f"💰 <b>租金：{price}</b>")
    
    specs = []
    if layout: specs.append(f"🛏 {layout}")
    if size: specs.append(f"📐 {size}")
    if floor: specs.append(f"🏢 {floor}")
    if specs:
        info_lines.append(" | ".join(specs))
    
    if area:
        info_lines.append(f"📍 区域：{area}")

    # 3. 卖点/描述 (参考 HTML 工具的 desc)
    highlights_raw = listing.get("highlights") or ""
    if isinstance(highlights_raw, str):
        highlights = [h.strip() for h in highlights_raw.replace("，", ",").split(",") if h.strip()]
    else:
        highlights = list(highlights_raw)
    
    # 4. 组合最终文案
    lines = [header, ""]
    lines.extend(info_lines)
    
    if highlights:
        lines.append("")
        lines.append("✨ <b>房源亮点</b>")
        for h in highlights[:4]:
            lines.append(f"• {h}")
    
    lines.append("")
    lines.append("💎 <b>侨联地产 · 在金边，把找房办明白</b>")
    lines.append("实拍房源，细节真实可核。")
    
    return "\n".join(lines)

# ── 按钮消息正文构造 ──────────────────────────────────────
def build_detail_text(listing: dict) -> str:
    """
    Media Group 后紧跟的文字消息正文。
    包含：顾问点评、缺点提醒、费用说明。
    """
    advisor   = (listing.get("advisor_comment") or "").strip()
    drawbacks = (listing.get("drawbacks") or "").strip()
    cost_notes= (listing.get("cost_notes") or "").strip()
    project   = listing.get("project") or ""
    area      = listing.get("area") or ""
    price     = listing.get("price") or ""
    
    lines = [f"<b>📋 {area} · {project}</b>　{price}/月\n"]
    if advisor:
        lines.append(f"💬 <b>顾问说</b>\n{advisor}\n")
    if drawbacks:
        lines.append(f"⚠️ <b>提前说清楚</b>\n{drawbacks}\n")
    if cost_notes:
        lines.append(f"💵 <b>费用说明</b>\n{cost_notes}\n")
    
    lines.append("👇 点下面按钮继续，不用重新解释是哪套房。")
    return "\n".join(lines)

# ── Inline Keyboard 构造 ──────────────────────────────────
def build_keyboard(listing: dict) -> InlineKeyboardMarkup:
    lid  = listing["listing_id"]
    area = (listing.get("area") or "金边").strip()
    if BOT_USERNAME:
        base = f"https://t.me/{BOT_USERNAME}?start="
        rows = [
            [
                InlineKeyboardButton("💬 立即咨询",  url=f"{base}{build_start_payload('consult', lid)}"),
                InlineKeyboardButton("📅 预约看房",  url=f"{base}{build_start_payload('appoint', lid)}"),
            ],
            [
                InlineKeyboardButton("❤️ 收藏房源",  url=f"{base}{build_start_payload('fav', lid)}"),
                InlineKeyboardButton("🏠 同区域更多", url=f"{base}{build_start_payload('more', area)}"),
            ],
        ]
    else:
        # 无 Bot 时退到频道链接
        ch_link = f"https://t.me/c/{str(CHANNEL_ID).lstrip('-100')}"
        rows = [
            [InlineKeyboardButton("💬 联系侨联顾问", url=ch_link)],
            [InlineKeyboardButton("🏠 更多房源",    url=ch_link)],
        ]
    return InlineKeyboardMarkup(rows)

# ── 内部异步实现 ──────────────────────────────────────────
async def _publish(listing: dict) -> tuple[str, list[int], int, list[str]]:
    """
    发布一条房源到频道。
    返回 (media_group_id, media_message_ids, button_message_id, file_ids)
    """
    bot = Bot(token=BOT_TOKEN)
    
    images = listing.get("images") or []
    if isinstance(images, str):
        try:
            images = json.loads(images)
        except:
            images = [i.strip() for i in images.split(",") if i.strip()]
    
    if not images:
        raise ValueError("房源没有图片，无法发布到频道")

    caption = build_caption(listing)
    
    opened_files = []
    media_items = []
    try:
        for i, img in enumerate(images):
            cap  = caption if i == 0 else None
            mode = ParseMode.HTML if i == 0 else None
            
            img = str(img).strip()
            if _is_file_id(img):
                # 复用已上传的 TG file_id
                media_items.append(InputMediaPhoto(media=img, caption=cap, parse_mode=mode))
            elif img.startswith("http"):
                media_items.append(InputMediaPhoto(media=img, caption=cap, parse_mode=mode))
            else:
                p = Path(img)
                if not p.exists():
                    print(f"[WARN] 图片不存在，跳过: {img}")
                    continue
                f = open(p, "rb")
                opened_files.append(f)
                media_items.append(InputMediaPhoto(media=f, caption=cap, parse_mode=mode))

        if not media_items:
            raise ValueError("所有图片均无法加载")

        # 发送 Media Group（≥2 张）或单张
        if len(media_items) == 1:
            sent = await bot.send_photo(
                chat_id=CHANNEL_ID,
                photo=media_items[0].media,
                caption=caption,
                parse_mode=ParseMode.HTML,
            )
            messages         = [sent]
            media_group_id   = str(sent.message_id)
            media_message_ids = [sent.message_id]
            file_ids         = [sent.photo[-1].file_id]
        else:
            messages = await bot.send_media_group(
                chat_id=CHANNEL_ID,
                media=media_items,
            )
            media_group_id    = messages[0].media_group_id or str(messages[0].message_id)
            media_message_ids = [m.message_id for m in messages]
            file_ids          = [m.photo[-1].file_id for m in messages if m.photo]

    finally:
        for f in opened_files:
            f.close()

    # 发送按钮消息
    detail_text = build_detail_text(listing)
    keyboard    = build_keyboard(listing)
    btn_msg = await bot.send_message(
        chat_id=CHANNEL_ID,
        text=detail_text,
        parse_mode=ParseMode.HTML,
        reply_markup=keyboard,
    )
    
    return media_group_id, media_message_ids, btn_msg.message_id, file_ids

async def _edit_btn_msg(button_message_id: int, text: str, 
                        keyboard: InlineKeyboardMarkup | None = None):
    # 仅用于下架/已租等低频状态更新；避免对同一条频道帖高频 edit 造成刷闪
    bot = Bot(token=BOT_TOKEN)
    await bot.edit_message_text(
        chat_id=CHANNEL_ID,
        message_id=button_message_id,
        text=text,
        parse_mode=ParseMode.HTML,
        reply_markup=keyboard,
    )

async def _delete_msgs(message_ids: list[int]):
    bot = Bot(token=BOT_TOKEN)
    for mid in message_ids:
        try:
            await bot.delete_message(chat_id=CHANNEL_ID, message_id=int(mid))
        except TelegramError as e:
            print(f"[WARN] 删除消息 {mid} 失败: {e}")

# ── 公开同步接口 ──────────────────────────────────────────
def publish_listing(listing: dict) -> tuple[str, list[int], int, list[str]]:
    """
    发布房源到频道。
    返回 (media_group_id, media_message_ids, button_message_id, file_ids)
    """
    return asyncio.run(_publish(listing))

def offline_listing(listing: dict, post: dict):
    """
    将频道已发帖子标注为下架。
    编辑按钮消息，移除 keyboard。
    """
    lid = listing["listing_id"]
    text = (
        f"🔴 <b>此房源已下架</b>\n\n"
        f"编号：{lid}\n"
        f"{listing.get('area','')} · {listing.get('project','')} "
        f"· {listing.get('price','')}/月\n\n"
        f"如需咨询其他房源，请联系侨联顾问。"
    )
    asyncio.run(_edit_btn_msg(post["button_message_id"], text, keyboard=None))

def rented_listing(listing: dict, post: dict):
    """
    将频道已发帖子标注为已租出。
    保留一个"查看更多"按钮。
    """
    lid = listing["listing_id"]
    text = (
        f"✅ <b>此房源已租出，感谢信任侨联</b>\n\n"
        f"编号：{lid}\n"
        f"{listing.get('area','')} · {listing.get('project','')} "
        f"· {listing.get('price','')}/月\n\n"
        f"侨联还有更多优质房源，欢迎继续关注 👇"
    )
    more_url = (
        f"https://t.me/{BOT_USERNAME}?start=latest" 
        if BOT_USERNAME 
        else f"https://t.me/c/{str(CHANNEL_ID).lstrip('-100')}"
    )
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("🏠 查看在租房源", url=more_url)
    ]])
    asyncio.run(_edit_btn_msg(post["button_message_id"], text, keyboard=keyboard))

def repub_listing(listing: dict, old_post: dict) -> tuple[str, list[int], int, list[str]]:
    """
    重发房源：先删除旧消息，再重新发布。
    返回同 publish_listing。
    """
    ids_to_del = []
    media_ids = old_post.get("media_message_ids") or []
    if isinstance(media_ids, str):
        try:
            media_ids = json.loads(media_ids)
        except Exception:
            media_ids = []
    ids_to_del.extend(int(i) for i in media_ids if i)
    
    btn_id = old_post.get("button_message_id")
    if btn_id:
        ids_to_del.append(int(btn_id))
    
    if ids_to_del:
        asyncio.run(_delete_msgs(ids_to_del))
    
    return publish_listing(listing)

# ── 工具 ─────────────────────────────────────────────────
def _is_file_id(s: str) -> bool:
    """
    粗略判断是否是 TG file_id（长度 > 30，无路径符，无 http）。
    """
    return (
        len(s) > 30 
        and "/" not in s 
        and "\\" not in s 
        and not s.startswith("http")
        and s.replace("-", "").replace("_", "").isalnum()
    )
