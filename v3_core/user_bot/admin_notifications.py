"""Explicit best-effort admin notification boundary for the V3 User Bot.

Locked production sends one Telegram message per configured admin, continues when
one delivery fails, and never turns an otherwise successful user flow into a
failure because an admin notification could not be delivered. This module keeps
that delivery contract separate from lead/appointment/search persistence.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

from telegram.constants import ParseMode


@dataclass(frozen=True)
class AdminNotification:
    title: str
    lines: tuple[str, ...]
    show_bell: bool = True
    reply_markup: Any = None

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
    def __init__(self, admin_ids: Iterable[int]):
        self.admin_ids = tuple(sorted({int(value) for value in admin_ids if int(value) > 0}))

    async def send(self, bot: Any, notification: AdminNotification) -> AdminNotificationResult:
        sent: list[int] = []
        failed: list[int] = []
        for admin_id in self.admin_ids:
            try:
                await bot.send_message(
                    chat_id=admin_id,
                    text=notification.text,
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                    reply_markup=notification.reply_markup,
                )
            except Exception:
                failed.append(admin_id)
                continue
            sent.append(admin_id)
        return AdminNotificationResult(
            attempted_admin_ids=self.admin_ids,
            sent_admin_ids=tuple(sent),
            failed_admin_ids=tuple(failed),
        )


__all__ = [
    "AdminNotification",
    "AdminNotificationResult",
    "TelegramAdminNotifier",
]
