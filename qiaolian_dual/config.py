from __future__ import annotations

import logging
import os
from pathlib import Path

from dotenv import load_dotenv

PROJECT_DIR = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_DIR / ".env")

DATA_DIR = Path(os.getenv("DATA_DIR", PROJECT_DIR / "data")).resolve()
DATA_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = Path(os.getenv("DB_PATH", DATA_DIR / "qiaolian_dual_bot.db")).resolve()
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=getattr(logging, LOG_LEVEL, logging.INFO),
)
logger = logging.getLogger("qiaolian_dual")

USER_BOT_TOKEN = os.getenv("USER_BOT_TOKEN", "")
PUBLISHER_BOT_TOKEN = os.getenv("PUBLISHER_BOT_TOKEN", "")
USER_BOT_USERNAME = os.getenv("USER_BOT_USERNAME", "qiaolian_rent_bot").lstrip("@")
CHANNEL_ID = os.getenv("CHANNEL_ID", "@your_channel_username")
CHANNEL_URL = os.getenv("CHANNEL_URL", "https://t.me/your_channel_username")
BRAND_NAME = os.getenv("BRAND_NAME", "侨联地产")
ADVISOR_TG = os.getenv("ADVISOR_TG", "@qiaolian_advisor")
ADVISOR_WECHAT = os.getenv("ADVISOR_WECHAT", "qiaolian_service")
ADVISOR_PHONE = os.getenv("ADVISOR_PHONE", "+855 XX XXX XXX")

ADMIN_IDS = {
    int(item.strip())
    for item in os.getenv("ADMIN_IDS", "").split(",")
    if item.strip().isdigit()
}

USER_BASE_URL = f"https://t.me/{USER_BOT_USERNAME}"

# 频道用户名（不含 @ 前缀），用于生成评论区链接
# https://t.me/{CHANNEL_USERNAME}/{channel_message_id}?comment=1
def _derive_channel_username() -> str:
    raw = os.getenv("CHANNEL_USERNAME", "").strip().lstrip("@")
    if raw:
        return raw
    # 尝试从 CHANNEL_URL 推导，例如 https://t.me/my_channel → my_channel
    _url = (CHANNEL_URL or "").strip()
    import re as _re
    m = _re.match(r"https?://t\.me/([A-Za-z0-9_]+)", _url)
    if m:
        return m.group(1)
    return ""

CHANNEL_USERNAME: str = _derive_channel_username()
DISCUSSION_GROUP_LINK: str = os.getenv("DISCUSSION_GROUP_LINK", "").strip()
SUPPORT_USERNAME: str = os.getenv("SUPPORT_USERNAME", "").strip().lstrip("@")
