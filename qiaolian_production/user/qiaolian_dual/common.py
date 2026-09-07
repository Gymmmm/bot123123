"""侨联用户 Bot：承接频道深链，记录留资、预约和找房偏好。"""
from __future__ import annotations
from html import escape as he
import asyncio
import json
import logging
import os
import re
import sqlite3
from datetime import datetime, time as dt_time, timedelta
from zoneinfo import ZoneInfo
from urllib.parse import quote
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes, ConversationHandler, MessageHandler, filters
try:
    from .config import ADVISOR_TG, ADMIN_IDS, CHANNEL_URL, DB_PATH, USER_BOT_TOKEN, USER_BOT_USERNAME
    from .db import Database
    from .messages import about_text as copy_about_text, appointment_hub_text as copy_appointment_hub_text, advisor_text as copy_advisor_text, advisor_notify_ok_text as copy_advisor_notify_ok_text, appoint_entry_text as copy_appoint_entry_text, appoint_success_text as copy_appoint_success_text, brand_text as copy_brand_text, channel_welcome_text as copy_channel_welcome_text, consult_submit_ok_text as copy_consult_submit_ok_text, deposit_text as copy_deposit_text, discussion_entry_welcome_text as copy_discussion_entry_welcome_text, find_area_budget_hint_text, find_no_match_text, handoff_find_ok_text as copy_handoff_find_ok_text, help_repeat_keyboard, help_text as copy_help_text, home_text as copy_home_text, lead_capture_text as copy_lead_capture_text, legacy_callback_degraded_text as copy_legacy_callback_degraded_text, listing_detail as copy_listing_detail, listing_match_footer_text, listing_match_intro_text, local_life_text as copy_local_life_text, merchant_join_text as copy_merchant_join_text, rfcity_bbq_text as copy_rfcity_bbq_text, rfcity_drinks_text as copy_rfcity_drinks_text, rfcity_hotel_text as copy_rfcity_hotel_text, rfcity_logistics_text as copy_rfcity_logistics_text, rfcity_property_text as copy_rfcity_property_text, rfcity_recreation_text as copy_rfcity_recreation_text, rfcity_restaurant_text as copy_rfcity_restaurant_text, rfcity_supermarket_text as copy_rfcity_supermarket_text, rfcity_text as copy_rfcity_text, search_entry_intro_text, service_promise_text as copy_service_promise_text, service_hub_text as copy_service_hub_text, smart_find_guided_header_text, smart_find_play_footer_hint_text, smart_find_play_prompt_text, smart_search_text as copy_smart_search_text, want_home_ack_text as copy_want_home_ack_text, want_home_text as copy_want_home_text
except ImportError:
    from qiaolian_dual.config import ADVISOR_TG, ADMIN_IDS, CHANNEL_URL, DB_PATH, USER_BOT_TOKEN, USER_BOT_USERNAME
    from qiaolian_dual.db import Database
    from qiaolian_dual.messages import about_text as copy_about_text, appointment_hub_text as copy_appointment_hub_text, advisor_text as copy_advisor_text, advisor_notify_ok_text as copy_advisor_notify_ok_text, appoint_entry_text as copy_appoint_entry_text, appoint_success_text as copy_appoint_success_text, brand_text as copy_brand_text, channel_welcome_text as copy_channel_welcome_text, consult_submit_ok_text as copy_consult_submit_ok_text, deposit_text as copy_deposit_text, discussion_entry_welcome_text as copy_discussion_entry_welcome_text, find_area_budget_hint_text, find_no_match_text, handoff_find_ok_text as copy_handoff_find_ok_text, help_repeat_keyboard, help_text as copy_help_text, home_text as copy_home_text, lead_capture_text as copy_lead_capture_text, legacy_callback_degraded_text as copy_legacy_callback_degraded_text, listing_detail as copy_listing_detail, listing_match_footer_text, listing_match_intro_text, local_life_text as copy_local_life_text, merchant_join_text as copy_merchant_join_text, rfcity_bbq_text as copy_rfcity_bbq_text, rfcity_drinks_text as copy_rfcity_drinks_text, rfcity_hotel_text as copy_rfcity_hotel_text, rfcity_logistics_text as copy_rfcity_logistics_text, rfcity_property_text as copy_rfcity_property_text, rfcity_recreation_text as copy_rfcity_recreation_text, rfcity_restaurant_text as copy_rfcity_restaurant_text, rfcity_supermarket_text as copy_rfcity_supermarket_text, rfcity_text as copy_rfcity_text, search_entry_intro_text, service_promise_text as copy_service_promise_text, service_hub_text as copy_service_hub_text, smart_find_guided_header_text, smart_find_play_footer_hint_text, smart_find_play_prompt_text, smart_search_text as copy_smart_search_text, want_home_ack_text as copy_want_home_ack_text, want_home_text as copy_want_home_text
from .location_mapping import get_display_location
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logging.getLogger('httpx').setLevel(logging.WARNING)
logger = logging.getLogger(__name__)
MAIN, FIND_AREA, FIND_BUDGET, APPT_MODE, APPT_FOCUS, APPT_DATE, APPT_TIME, APPT_CONFIRM = range(8)
_AREA_HINT_KEYS = ('BKK1', 'BKK2', 'BKK3', '森速', 'TK/7月区', '俄罗斯市场', '钻石岛')
AREA_HINTS = [get_display_location(key) for key in _AREA_HINT_KEYS] + ['不限']
ROOM_TYPE_HINTS = {'studio': ['studio', '开间', '单间'], '1房': ['1房', '一房', '1br', '1 bed', '一居'], '2房': ['2房', '二房', '2br', '2 bed', '两居'], '3房': ['3房', '三房', '3br', '3 bed', '三居']}
START_ACTIONS = ('consult', 'appoint', 'fav', 'more')
START_ACTION_ALIASES = {'a': 'appoint', 'f': 'fav', 'm': 'more', 'q': 'consult'}
START_ACTION_CODES = {action: alias for alias, action in START_ACTION_ALIASES.items()}
APPOINTMENT_MODE_LABELS = {'offline': '实地看房', 'video': '实时视频看房'}
APPOINTMENT_TIME_LABELS = {'am': '上午 09:00–12:00', 'pm': '下午 14:00–17:00', 'evening': '晚上 17:00–19:00'}
APPOINTMENT_FOCUS_LABELS = {'ac': '空调型号和老旧程度', 'appliances': '冰箱/洗衣机/家具使用痕迹', 'light_noise': '采光、噪音、窗外环境', 'water': '水压、热水、排水', 'fee_contract': '费用和合同细节'}
APPOINTMENT_FOCUS_ORDER = ['ac', 'appliances', 'light_noise', 'water', 'fee_contract']
APPOINTMENT_STATUS_LABELS = {'pending': '待确认', 'assigned': '顾问联系中', 'contacted': '顾问联系中', 'confirmed': '已确认', 'done': '已完成', 'cancelled': '已取消'}
LEASE_REMINDER_DAYS = (7,)
SERVICE_REQUEST_LABELS = {'repair_ac': '空调', 'repair_water': '热水器 / 漏水排水', 'repair_power': '灯具 / 电路', 'repair_door': '门锁 / 门禁', 'repair_washer': '洗衣机', 'repair_fridge': '冰箱', 'repair_network': '网络', 'repair_furniture': '家具损坏', 'repair_other': '其他设备', 'property': '物业协调'}
PREF_CONDITION_LABELS = {'budget': '预算优先', 'area': '区域优先', 'utility': '必须民水民电', 'parking': '停车方便', 'quiet': '安静不吵', 'sunlight': '采光好', 'pet': '可养宠物', 'furnished': '拎包入住', 'chinese_owner': '中国房东', 'amenity': '电梯/泳池'}
FIND_AREA_CODE_MAP = {
    'rf': '富力城', 'pp': '炳发城', 'ph': '太子幸福广场',
    'bkk1': 'BKK1', 'bkk23': 'BKK2 / BKK3', 'tk': 'TK/7月区', 'koh': '钻石岛',
    'aeon1': '永旺1', 'russian': '俄罗斯市场', 'chroy': '水净华', 'sen': '森速',
    'a4': 'BKK1', 'a8': '森速', 'a6': 'TK/7月区', 'a7': '洪森大道',
    'a41': 'BKK2', 'a42': 'BKK3', 'a0': '不限',
}
_FIND_AREA_ICONS = {'rf': '🏙', 'pp': '🌆', 'ph': '🌟', 'bkk1': '📍', 'tk': '🗺', 'koh': '💎'}
FIND_AREA_OPTIONS: list[tuple[str, str]] = [
    (code, f"{_FIND_AREA_ICONS[code]} {get_display_location(FIND_AREA_CODE_MAP[code])}")
    for code in ('rf', 'pp', 'ph', 'bkk1', 'tk', 'koh')
]
FIND_BUDGET_OPTIONS: dict[str, list[tuple[str, str, int | None, int | None]]] = {'住宅': [('r1', '$300以下', None, 300), ('r2', '$300-500', 300, 500), ('r3', '$500-800', 500, 800), ('r4', '$800-1200', 800, 1200), ('r5', '$1200-2000', 1200, 2000), ('r6', '$2000+', 2000, None), ('rn', '不限预算', None, None)], '别墅/排屋': [('v1', '$800-1500', 800, 1500), ('v2', '$1500-2500', 1500, 2500), ('v3', '$2500+', 2500, None), ('vn', '不限预算', None, None)], '商铺/办公': [('o1', '$500以下', None, 500), ('o2', '$500-1000', 500, 1000), ('o3', '$1000-2000', 1000, 2000), ('o4', '$2000+', 2000, None), ('on', '不限预算', None, None)], 'any': [('n1', '$300以下', None, 300), ('n2', '$300-500', 300, 500), ('n3', '$500-800', 500, 800), ('n4', '$800-1200', 800, 1200), ('n5', '$1200-2000', 1200, 2000), ('n6', '$2000+', 2000, None), ('nn', '不限预算', None, None)]}
db = Database(DB_PATH)
PANEL_ANCHOR_KEY = '_panel_anchor'


def _title_layout_label(title: str, layout: str, separator: str='｜') -> str:
    """跨预约模块共用的标题/户型去重 helper，兼容旧回调。"""
    title_text = str(title or '').strip()
    layout_text = str(layout or '').strip()
    if not layout_text or layout_text.lower() in title_text.lower():
        return title_text
    return f'{title_text}{separator}{layout_text}'


from weakref import WeakSet
_CALLBACK_ANSWERED_IDS: set[str] = set()
_CALLBACK_ANSWERED_ORDER: list[str] = []
_CALLBACK_ANSWERED_OBJECTS = WeakSet()

async def answer_callback_once(query, text: str | None=None, *, show_alert: bool=False) -> bool:
    query_id = str(getattr(query, 'id', '') or '')
    if query_id:
        if query_id in _CALLBACK_ANSWERED_IDS:
            return False
    else:
        try:
            if query in _CALLBACK_ANSWERED_OBJECTS:
                return False
        except TypeError:
            if bool(getattr(query, '_qiaolian_answered', False)):
                return False
    await query.answer(text=text, show_alert=show_alert)
    if query_id:
        _CALLBACK_ANSWERED_IDS.add(query_id)
        _CALLBACK_ANSWERED_ORDER.append(query_id)
        if len(_CALLBACK_ANSWERED_ORDER) > 2048:
            stale = _CALLBACK_ANSWERED_ORDER.pop(0)
            _CALLBACK_ANSWERED_IDS.discard(stale)
    else:
        try:
            _CALLBACK_ANSWERED_OBJECTS.add(query)
        except TypeError:
            try:
                setattr(query, '_qiaolian_answered', True)
            except Exception:
                pass
    return True

__all__ = [name for name in globals() if not name.startswith('__')]

_MAIN_CB_PATTERN = r"^"
_APPT_CB_PATTERN = r"^(apmode:|apfocus:|apdate:|aptime:|apconfirm:|apedit:|appoint_back_mode|appoint_back_date|appoint_back_time|home$)"

__all__ = [name for name in globals() if not name.startswith('__')]
