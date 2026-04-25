"""
tg_login.py
交互式 Telegram 登录，更新 session 文件
"""
import asyncio
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).parent.resolve()
load_dotenv(BASE_DIR / ".env")

from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError

TG_API_ID   = int(os.getenv("TG_API_ID", "0"))
TG_API_HASH = os.getenv("TG_API_HASH", "")
SESSION     = str(BASE_DIR / "v2" / "qiaolian_crawler_session")
PHONE       = "+855716539768"

async def main():
    print(f"API_ID={TG_API_ID}, SESSION={SESSION}")
    client = TelegramClient(SESSION, TG_API_ID, TG_API_HASH)
    await client.connect()

    if await client.is_user_authorized():
        me = await client.get_me()
        print(f"已登录: {me.first_name} @{me.username} id={me.id}")
        await client.disconnect()
        return

    print(f"正在向 {PHONE} 发送验证码...")
    await client.send_code_request(PHONE)

    code = input("请输入收到的验证码: ").strip()
    try:
        await client.sign_in(PHONE, code)
    except SessionPasswordNeededError:
        pwd = input("需要两步验证密码: ").strip()
        await client.sign_in(password=pwd)

    me = await client.get_me()
    print(f"\n✅ 登录成功！用户: {me.first_name} @{me.username} id={me.id}")
    print("Session 文件已更新，可以开始采集了。")
    await client.disconnect()

asyncio.run(main())
