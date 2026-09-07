from __future__ import annotations

import html
import re
from datetime import datetime
from typing import Iterable

from telegram import Update

from .config import ADMIN_IDS, USER_BASE_URL


def e(value: object | None) -> str:
    if value is None:
        return ""
    return html.escape(str(value), quote=False)


def now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def get_display_name(update: Update) -> str:
    user = update.effective_user
    if user is None:
        return "朋友"
    name = " ".join(part for part in [user.first_name, user.last_name] if part)
    return name.strip() or "朋友"


def get_contact_tag(update: Update) -> str:
    user = update.effective_user
    if user is None:
        return ""
    if user.username:
        return f"@{user.username}"
    return f"tg://user?id={user.id}"


def is_admin(user_id: int | None) -> bool:
    return bool(user_id and user_id in ADMIN_IDS)


def deep_link(payload: str) -> str:
    return f"{USER_BASE_URL}?start={payload}"


def split_tags(raw: str) -> list[str]:
    parts = re.split(r"[,，/、\s]+", raw.strip())
    seen: set[str] = set()
    result: list[str] = []
    for part in parts:
        token = part.strip()
        if token and token not in seen:
            seen.add(token)
            result.append(token)
    return result


def compact_join(items: Iterable[str], sep: str = " / ") -> str:
    return sep.join(str(item).strip() for item in items if str(item).strip())
