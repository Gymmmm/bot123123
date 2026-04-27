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

PINNED_TEXT = """<b>🏠 侨联地产 · 您在金边的自己人</b>
金边华人实拍租房 · 中文顾问全程陪跑

<b>📍 找房路径（3 步）</b>
1️⃣ 在频道里看实拍相册，找感兴趣的户型
2️⃣ 点帖内「💬 咨询这套」或「📅 预约看房」
3️⃣ Bot 会直接接住当前入口，不用重新解释是哪套房

<b>🔍 主动找房</b>
点下方按钮 → 按区域 / 按预算 / 最新房源 三路直达

<b>✅ 三项承诺</b>
• 实拍直发，编号可追溯，所见即实况
• 水电押付等隐性成本帖里先列，签前不踩坑
• 从预约到入住售后，管理号全程不断档

👇 直接点下方按钮开始"""

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
                    InlineKeyboardButton("品牌故事", url=f"{base}brand"),
                    InlineKeyboardButton("关于侨联", url=f"{base}about"),
                ],
                [
                    InlineKeyboardButton("预约想住", url=f"{base}want_home"),
                    InlineKeyboardButton("立即咨询", url=f"{base}ask"),
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

asyncio.run(main())
