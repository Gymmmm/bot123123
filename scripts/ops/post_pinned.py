#!/usr/bin/env python3
"""
post_pinned.py
发布侨联地产频道置顶消息，并置顶
"""
import os
import asyncio
from dotenv import load_dotenv

load_dotenv("/opt/qiaolian_dual_bots/.env")

PUBLISHER_TOKEN = os.getenv("PUBLISHER_BOT_TOKEN", "")
CHANNEL_ID_STR  = os.getenv("CHANNEL_ID", "")
USER_BOT_USERNAME = os.getenv("USER_BOT_USERNAME", "").strip().lstrip("@")

try:
    CHANNEL_ID = int(CHANNEL_ID_STR)
except Exception:
    CHANNEL_ID = CHANNEL_ID_STR

PINNED_TEXT = """<b>🏠 侨联地产｜金边华人租房</b>
实拍房源 · 费用先说 · 中文带看

看中房源，直接点帖内「咨询」或「预约」，
系统会自动带上房源编号。

👇 也可以按区域或预算开始找"""

async def main():
    from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup
    from telegram.constants import ParseMode

    if not PUBLISHER_TOKEN:
        print("错误：PUBLISHER_BOT_TOKEN 未配置")
        return

    bot = Bot(token=PUBLISHER_TOKEN)
    me = await bot.get_me()
    print(f"Bot: @{me.username}")
    print(f"Channel: {CHANNEL_ID}")

    reply_markup = None
    if USER_BOT_USERNAME:
        base = f"https://t.me/{USER_BOT_USERNAME}?start="
        reply_markup = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("📍 区域找房", url=f"{base}find_area"),
                    InlineKeyboardButton("💰 预算找房", url=f"{base}find_budget"),
                ],
                [
                    InlineKeyboardButton("🆕 最新房源", url=f"{base}latest"),
                    InlineKeyboardButton("💬 中文顾问", url=f"{base}advisor"),
                ],
            ]
        )

    # 发送置顶消息
    msg = await bot.send_message(
        chat_id=CHANNEL_ID,
        text=PINNED_TEXT,
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
        reply_markup=reply_markup,
    )
    print(f"消息已发送，message_id={msg.message_id}")

    # 置顶
    try:
        await bot.pin_chat_message(
            chat_id=CHANNEL_ID,
            message_id=msg.message_id,
            disable_notification=True,
        )
        print("消息已置顶")
    except Exception as e:
        print(f"置顶失败（可能需要管理员权限）：{e}")

    await bot.close()

if __name__ == "__main__":
    asyncio.run(main())
