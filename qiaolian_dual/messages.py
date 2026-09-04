from __future__ import annotations

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from .config import ADVISOR_PHONE, ADVISOR_TG, ADVISOR_WECHAT, BRAND_NAME, CHANNEL_URL
from .utils import compact_join, e
from .messages_more import (
    advisor_notify_ok_text,
    advisor_response_notice_text,
    appoint_entry_text,
    appoint_success_text,
    consult_submit_ok_text,
    handoff_find_ok_text,
    legacy_callback_degraded_text,
    local_life_text,
    merchant_join_text,
    repair_progress_text,
    rfcity_bbq_text,
    rfcity_drinks_text,
    rfcity_hotel_text,
    rfcity_logistics_text,
    rfcity_property_text,
    rfcity_recreation_text,
    rfcity_restaurant_text,
    rfcity_supermarket_text,
    rfcity_text,
    smart_search_text,
)


def public_brand_name() -> str:
    """用户可见品牌名不携开发环境标记。"""
    return (BRAND_NAME or "侨联地产").replace("测试", "").strip() or "侨联地产"
