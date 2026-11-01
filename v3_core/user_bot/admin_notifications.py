"""Explicit best-effort admin notification boundary for the V3 User Bot.

Locked production sends one Telegram message per configured admin, continues when
one delivery fails, and never turns an otherwise successful user flow into a
failure because an admin notification could not be delivered. This module keeps
that delivery contract separate from lead/appointment/search persistence.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable
from pathlib import Path
import asyncio
import json
import os
import time
from uuid import uuid4
from telegram import InlineKeyboardMarkup

from telegram.constants import ParseMode


@dataclass(frozen=True)
class AdminNotification:
    title: str
    lines: tuple[str, ...]
    show_bell: bool = True
    reply_markup: Any = None
    media: tuple[tuple[str, str], ...] = ()

    @property
    def text(self) -> str:
        body = "\n".join(line for line in self.lines if str(line or "").strip())
        prefix = "🔔 " if self.show_bell else ""
        return f"{prefix}<b>{self.title}</b>\n\n{body}".strip()


@dataclass(frozen=True)
class AdminNotificationResult:
    attempted_admin_ids: tuple[int, ...]
    sent_admin_ids: tuple[int, ...]
    failed_admin_ids: tuple[int, ...]

    @property
    def ok(self) -> bool:
        return not self.failed_admin_ids


class TelegramAdminNotifier:
    def __init__(self, admin_ids: Iterable[int], *, outbox_dir: str | Path | None = None):
        self.admin_ids = tuple(sorted({int(value) for value in admin_ids if int(value) > 0}))
        self.outbox_dir = Path(outbox_dir) if outbox_dir is not None else None
        self._retry_lock = asyncio.Lock()

    def _save(self, path: Path, payload: dict) -> None:
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        temporary = path.with_suffix(".tmp")
        temporary.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        os.chmod(temporary, 0o600)
        temporary.replace(path)

    async def _deliver(self, bot: Any, payload: dict, path: Path | None = None) -> None:
        if payload["text_pending"]:
            markup = InlineKeyboardMarkup.de_json(payload["reply_markup"], bot) if payload.get("reply_markup") else None
            await bot.send_message(
                chat_id=payload["chat_id"], text=payload["text"],
                parse_mode=ParseMode.HTML, disable_web_page_preview=True,
                reply_markup=markup,
            )
            payload["text_pending"] = False
            if path is not None:
                self._save(path, payload)
        while payload["media"]:
            kind, file_id = payload["media"][0]
            if kind == "video":
                await bot.send_video(chat_id=payload["chat_id"], video=file_id)
            else:
                await bot.send_photo(chat_id=payload["chat_id"], photo=file_id)
            payload["media"].pop(0)
            if path is not None:
                self._save(path, payload)

    async def send(self, bot: Any, notification: AdminNotification) -> AdminNotificationResult:
        sent, failed = [], []
        for admin_id in self.admin_ids:
            payload = {
                "chat_id": admin_id, "text": notification.text, "text_pending": True,
                "reply_markup": notification.reply_markup.to_dict() if notification.reply_markup is not None else None,
                "media": [list(item) for item in notification.media],
                "next_attempt": time.time() + 60,
            }
            path = self.outbox_dir / (uuid4().hex + ".json") if self.outbox_dir is not None else None
            # Preserve the complete handoff before sending, including attachment IDs.
            if path is not None:
                self._save(path, payload)
            try:
                await self._deliver(bot, payload, path)
            except Exception:
                if path is not None:
                    self._save(path, payload)
                failed.append(admin_id)
                continue
            if path is not None:
                path.unlink(missing_ok=True)
            sent.append(admin_id)
        return AdminNotificationResult(self.admin_ids, tuple(sent), tuple(failed))

    async def retry_pending(self, bot: Any) -> None:
        if self.outbox_dir is None or not self.outbox_dir.exists() or self._retry_lock.locked():
            return
        async with self._retry_lock:
            for path in sorted(self.outbox_dir.glob("*.json"))[:20]:
                try:
                    payload = json.loads(path.read_text(encoding="utf-8"))
                    if int(payload["chat_id"]) not in self.admin_ids or payload.get("next_attempt", 0) > time.time():
                        continue
                    payload["next_attempt"] = time.time() + 120
                    self._save(path, payload)
                    await self._deliver(bot, payload, path)
                    path.unlink(missing_ok=True)
                except Exception:
                    # Keep failed handoffs durable for the next scheduled retry.
                    continue


__all__ = ["AdminNotification", "AdminNotificationResult", "TelegramAdminNotifier"]
