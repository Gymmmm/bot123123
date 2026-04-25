#!/usr/bin/env python3
from __future__ import annotations

import os
import sys
from typing import Optional

import requests


def _env(name: str, default: str = "") -> str:
    return str(os.getenv(name, default) or "").strip()


def build_index_text(bot_username: str) -> str:
    username = bot_username.lstrip("@")
    return (
        "🏠 <b>侨联租房服务助手</b>\n\n"
        "建议优先点按钮完成流程，全程少打字：\n\n"
        "• <code>/start</code> 回首页\n"
        "• <code>/find</code> 快速找房\n"
        "• <code>/favorites</code> 我的收藏\n"
        "• <code>/appointments</code> 我的预约\n"
        "• <code>/contact</code> 联系顾问\n"
        "• <code>/help</code> 使用说明\n\n"
        "从频道帖子点进来时，咨询和预约会自动带上对应房源，不会丢上下文。\n\n"
        f"Bot 入口：<a href=\"https://t.me/{username}\">https://t.me/{username}</a>"
    )


def tg_api(token: str, method: str, payload: dict) -> dict:
    url = f"https://api.telegram.org/bot{token}/{method}"
    resp = requests.post(url, json=payload, timeout=20)
    resp.raise_for_status()
    data = resp.json()
    if not data.get("ok"):
        raise RuntimeError(f"Telegram API {method} failed: {data}")
    return data


def send_and_pin_index(token: str, chat_id: str, text: str) -> int:
    sent = tg_api(
        token,
        "sendMessage",
        {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        },
    )
    msg = sent.get("result") or {}
    message_id = int(msg.get("message_id") or 0)
    if message_id <= 0:
        raise RuntimeError("sendMessage succeeded but message_id missing")

    tg_api(
        token,
        "pinChatMessage",
        {
            "chat_id": chat_id,
            "message_id": message_id,
            "disable_notification": True,
        },
    )
    return message_id


def resolve_target_chat_id() -> Optional[str]:
    return _env("INDEX_CHAT_ID") or _env("ADMIN_CHAT_ID") or None


def main() -> int:
    token = _env("USER_BOT_TOKEN")
    bot_username = _env("USER_BOT_USERNAME", "qiaolian_rent_bot")
    chat_id = resolve_target_chat_id()

    if not token:
        print("ERROR: USER_BOT_TOKEN is required.")
        return 2
    if not chat_id:
        print("ERROR: INDEX_CHAT_ID or ADMIN_CHAT_ID is required.")
        return 2

    text = build_index_text(bot_username)
    try:
        message_id = send_and_pin_index(token, chat_id, text)
    except Exception as exc:
        print(f"ERROR: deploy index failed: {exc}")
        return 1

    print("OK: index message sent and pinned")
    print(f"chat_id={chat_id}")
    print(f"message_id={message_id}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
