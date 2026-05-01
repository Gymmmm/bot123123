
from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
# 兼容两种部署：
# 1) /opt/qiaolian_dual_bots/v2/.env
# 2) 项目根目录 /opt/qiaolian_dual_bots/.env（本地也是这一种）
load_dotenv(BASE_DIR / ".env")
load_dotenv(BASE_DIR.parent / ".env")

_CHANNEL_URL_RE = re.compile(r"https?://t\.me/([A-Za-z0-9_]+)")


def _normalize_username(raw: str) -> str:
    return str(raw or "").strip().lstrip("@")


def _parse_admin_ids(raw: str) -> list[int]:
    result: list[int] = []
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        result.append(int(item))
    return result


@dataclass(frozen=True)
class Settings:
    publisher_bot_token: str
    user_bot_username: str
    channel_id: str
    channel_url: str
    channel_username: str
    discussion_group_link: str
    admin_ids: list[int]
    sqlite_path: Path
    default_contact_handle: str


def get_settings() -> Settings:
    token = os.getenv("PUBLISHER_BOT_TOKEN", "").strip()
    if not token:
        raise RuntimeError("PUBLISHER_BOT_TOKEN 未配置")

    admin_ids = _parse_admin_ids(os.getenv("ADMIN_IDS", ""))
    if not admin_ids:
        raise RuntimeError("ADMIN_IDS 未配置")

    sqlite_path = Path(os.getenv("SQLITE_PATH", "data/qiaolian_dual_bot.db"))
    if not sqlite_path.is_absolute():
        sqlite_path = BASE_DIR / sqlite_path

    deep_link_user = (
        _normalize_username(os.getenv("DEEPLINK_BOT_USERNAME", ""))
        or _normalize_username(os.getenv("USER_BOT_USERNAME", ""))
        or _normalize_username(os.getenv("PUBLISHER_BOT_USERNAME", ""))
    )

    # 频道用户名：优先读 CHANNEL_USERNAME，否则从 CHANNEL_URL 推导
    raw_ch_user = _normalize_username(os.getenv("CHANNEL_USERNAME", ""))
    if not raw_ch_user:
        m = _CHANNEL_URL_RE.match(os.getenv("CHANNEL_URL", "").strip())
        if m:
            raw_ch_user = m.group(1)

    return Settings(
        publisher_bot_token=token,
        user_bot_username=deep_link_user,
        channel_id=os.getenv("CHANNEL_ID", "").strip(),
        channel_url=os.getenv("CHANNEL_URL", "").strip(),
        channel_username=raw_ch_user,
        discussion_group_link=os.getenv("DISCUSSION_GROUP_LINK", "").strip(),
        admin_ids=admin_ids,
        sqlite_path=sqlite_path,
        default_contact_handle=os.getenv("DEFAULT_CONTACT_HANDLE", "@qiaolian_advisor").strip(),
    )
