from __future__ import annotations

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from .config import ADVISOR_PHONE, ADVISOR_TG, ADVISOR_WECHAT, BRAND_NAME, CHANNEL_URL
from .utils import compact_join, e


def public_brand_name() -> str:
    """用户可见品牌名不携开发环境标记。"""
    return (BRAND_NAME or "侨联地产").replace("测试", "").strip() or "侨联地产"
