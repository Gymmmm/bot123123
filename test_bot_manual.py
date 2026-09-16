#!/usr/bin/env python3
"""手动测试 Bot 功能 - 模拟真实用户交互"""
import asyncio
import sys
import os

# 添加项目路径
sys.path.insert(0, os.path.dirname(__file__))

from telegram import Bot, Update
from telegram.ext import ApplicationBuilder
import sqlite3

BOT_TOKEN = "<redacted-test-token>"
TEST_CHAT_ID = None  # 需要一个真实的 chat_id 来测试

async def test_bot_responses():
    """测试 Bot 响应能力"""
    bot = Bot(BOT_TOKEN)

    print("=== Bot 基础信息 ===")
    me = await bot.get_me()
    print(f"Bot ID: {me.id}")
    print(f"Username: @{me.username}")
    print(f"Name: {me.first_name}")
    print()

    print("=== 检查数据库状态 ===")
    db_path = "data/qiaolian_dual_bot.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 检查表
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = [row[0] for row in cursor.fetchall()]
    print(f"数据库表数量: {len(tables)}")

    # 检查关键表数据
    for table in ['listings', 'users', 'favorites', 'appointments', 'leads']:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"  {table}: {count} 条记录")

    conn.close()
    print()

    print("=== 获取最近更新 ===")
    try:
        updates = await bot.get_updates(limit=5, timeout=3)
        print(f"最近消息数: {len(updates)}")
        for update in updates:
            if update.message:
                print(f"  - {update.message.from_user.username}: {update.message.text[:50] if update.message.text else 'N/A'}")
    except Exception as e:
        print(f"获取更新失败: {e}")

    print()
    print("=== 测试说明 ===")
    print("1. 在 Telegram 中搜索 @XxxXiaopengbot")
    print("2. 发送 /start 测试首页")
    print("3. 点击各个按钮测试功能")
    print("4. 观察本地日志输出")
    print()

if __name__ == "__main__":
    asyncio.run(test_bot_responses())
