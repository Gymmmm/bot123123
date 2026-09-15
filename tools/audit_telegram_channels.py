#!/usr/bin/env python3
"""只读盘点房源频道历史消息，为批量改版生成 JSON 证据清单。"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from telethon import TelegramClient


ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")


async def audit_channel(client: TelegramClient, username: str, limit: int) -> dict:
    entity = await client.get_entity(username)
    records = []
    grouped = Counter()
    async for message in client.iter_messages(entity, limit=limit):
        grouped_id = str(message.grouped_id or "")
        if grouped_id:
            grouped[grouped_id] += 1
        text = str(message.message or "")
        records.append(
            {
                "message_id": message.id,
                "date": message.date.astimezone(timezone.utc).isoformat() if message.date else "",
                "grouped_id": grouped_id,
                "has_media": bool(message.media),
                "text_length": len(text),
                "text": text,
                "views": message.views,
                "forwards": message.forwards,
                "reply_count": getattr(getattr(message, "replies", None), "replies", None),
                "edit_date": message.edit_date.astimezone(timezone.utc).isoformat() if message.edit_date else "",
                "link": f"https://t.me/{username.lstrip('@')}/{message.id}",
            }
        )
    return {
        "username": username,
        "channel_id": entity.id,
        "title": getattr(entity, "title", ""),
        "messages_scanned": len(records),
        "album_groups": len(grouped),
        "messages": records,
    }


async def main_async(args: argparse.Namespace) -> None:
    api_id = int(os.getenv("TG_API_ID", "0") or 0)
    api_hash = str(os.getenv("TG_API_HASH", ""))
    session = str(os.getenv("TELETHON_SESSION_PATH", ROOT / "telethon_sessions/qiaolian_collector"))
    client = TelegramClient(session, api_id, api_hash)
    await client.connect()
    if not await client.is_user_authorized():
        raise RuntimeError("Telethon session is not authorized")
    result = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "read_only": True,
        "channels": [],
    }
    for username in args.channels:
        try:
            result["channels"].append(await audit_channel(client, username, args.limit))
        except Exception as exc:
            result["channels"].append(
                {"username": username, "error": type(exc).__name__, "detail": str(exc)[:200]}
            )
    await client.disconnect()
    target = Path(args.output)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(result, ensure_ascii=False, indent=2))
    for item in result["channels"]:
        print(
            f"{item.get('username')}: messages={item.get('messages_scanned', 0)} "
            f"albums={item.get('album_groups', 0)} error={item.get('error', '')}"
        )
    print(f"report={target}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--channels", nargs="+", required=True)
    parser.add_argument("--limit", type=int, default=300)
    parser.add_argument("--output", required=True)
    asyncio.run(main_async(parser.parse_args()))


if __name__ == "__main__":
    main()
