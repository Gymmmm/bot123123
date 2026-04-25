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

PINNED_TEXT = """<b>🏠 侨联地产｜金边您的自己人</b>

实拍房源更新频道 · 中文顾问全程协助

<b>📌 快速入口</b>
🔍 <b>按区域看</b> → 点下方按钮直达
💰 <b>按预算看</b> → 点下方按钮直达  
⭐ <b>今日新上</b> → 频道内上滑查看
💎 <b>降价房源</b> → 频道内上滑查看

<b>❓ 看中房源怎么做</b>
1️⃣ 直接点帖内「📅 预约看房」或「💬 问这套」
2️⃣ 机器人自动带房源信息进私聊
3️⃣ 确认需求 → 给你同预算同区域选项 → 留资（可选）
4️⃣ 完成后点「返回频道继续看」

💡 <b>核心承诺</b>
✅ 中文顾问实地带看 / 实时视频代看  
✅ 入住与售后一条龙（报修、物业沟通）
✅ 费用隐性项前置说清楚

👇 点下方按钮开始浏览"""

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
