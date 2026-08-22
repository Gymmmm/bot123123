"""从 user_bot.py 拆分出的职责模块。"""
from __future__ import annotations

from .common import *

def parse_budget_range(text: str) -> tuple[int | None, int | None]:
    values = [int(x) for x in re.findall('\\d{2,5}', text or '')]
    if not values:
        return (None, None)
    if len(values) == 1:
        value = values[0]
        return (max(0, value - 200), value + 200)
    low, high = (min(values[0], values[1]), max(values[0], values[1]))
    return (low, high)

def detect_area(text: str) -> str:
    """从用户输入中识别位置，使用位置映射系统"""
    from .location_mapping import normalize_user_input, LOCATION_MAP
    if not text:
        return ''
    matched_areas = normalize_user_input(text)
    if matched_areas:
        return matched_areas[0]
    raw = text.strip()
    lowered = raw.lower()
    for db_area in LOCATION_MAP.keys():
        if db_area.lower() in lowered:
            return db_area
    if '不限' in text:
        return '不限'
    return raw[:40]

def detect_room_type(text: str) -> str:
    lowered = (text or '').lower()
    for label, variants in ROOM_TYPE_HINTS.items():
        if any((v in lowered for v in variants)):
            return label
    return ''

def detect_property_type(text: str) -> str:
    """仅提取可用于 listings.property_type 精确过滤的类型。"""
    lowered = (text or '').lower()
    mapping = (('别墅', ('别墅', 'villa')), ('排屋', ('排屋', 'townhouse')), ('商铺', ('商铺', '店铺', 'shophouse', 'shop')), ('办公室', ('办公室', 'office')), ('公寓', ('公寓', 'apartment', 'studio')), ('住宅', ('住宅',)))
    for canonical, keys in mapping:
        if any((k in lowered for k in keys)):
            return canonical
    return ''

def search_listings_with_fallback(*, property_type: str | None, area: str | None, budget_min: int | None, budget_max: int | None, text_fragment: str='', limit: int=3) -> tuple[list[dict], str]:
    """分层放宽条件，避免“有房但全空结果”。

    返回：(matches, mode)
    mode: strict / no_type / no_area / budget_only / fuzzy / fallback_recent
    """
    if area and area != '不限':
        from .location_mapping import get_all_location_aliases
        area_arg = get_all_location_aliases(area)
    else:
        area_arg = None
    matches = db.search_listings(property_type=property_type or None, areas=area_arg, budget_min=budget_min, budget_max=budget_max, limit=limit)
    if matches:
        return (matches, 'strict')
    if property_type:
        matches = db.search_listings(areas=area_arg, budget_min=budget_min, budget_max=budget_max, limit=limit)
        if matches:
            return (matches, 'no_type')
    if area and area != '不限':
        matches = db.search_listings(property_type=property_type or None, budget_min=budget_min, budget_max=budget_max, limit=limit)
        if matches:
            return (matches, 'no_area')
    matches = db.search_listings(budget_min=budget_min, budget_max=budget_max, limit=limit)
    if matches:
        return (matches, 'budget_only')
    frag = (text_fragment or '').strip()
    if frag:
        matches = db.search_listings(ilike_fragment=frag, limit=limit)
        if matches:
            return (matches, 'fuzzy')
    return (db.list_recent_listings(limit), 'fallback_recent')

def upsert_user_profile(user) -> None:
    """仅对真实 Telegram User 写入资料；频道/系统更新可能没有 from_user。"""
    from .session_deeplink import now_ts
    if user is None or not getattr(user, 'id', None):
        return
    db.upsert_user(int(user.id), getattr(user, 'username', '') or '', getattr(user, 'first_name', '') or '', getattr(user, 'last_name', '') or '', now_ts())

def create_lead(user, *, action: str, source: str, listing_id: str='', area: str='', property_type: str='', budget_min: int | None=None, budget_max: int | None=None, payload: dict | None=None) -> int | None:
    from .session_deeplink import _normalize_variant, now_ts, user_display_name
    if user is None or not getattr(user, 'id', None):
        logger.warning('跳过无用户身份的线索写入: action=%s listing=%s', action, listing_id)
        return None
    lead_payload = payload or {}
    raw_message_id = lead_payload.get('channel_message_id', lead_payload.get('message_id'))
    try:
        message_id = int(raw_message_id) if raw_message_id not in (None, '', 0) else None
    except (TypeError, ValueError):
        message_id = None
    caption_variant = _normalize_variant(lead_payload.get('caption_variant')) or ''
    post_token = str(lead_payload.get('post_token') or '').strip()
    agent_id = str(lead_payload.get('agent_id') or '').strip()
    response_at = str(lead_payload.get('response_at') or '').strip()
    try:
        return db.create_lead({'user_id': user.id, 'username': getattr(user, 'username', '') or '', 'display_name': user_display_name(user), 'source': source, 'action': action, 'listing_id': listing_id, 'area': area, 'property_type': property_type, 'budget_min': budget_min, 'budget_max': budget_max, 'payload': lead_payload, 'message_id': message_id, 'post_token': post_token, 'caption_variant': caption_variant, 'agent_id': agent_id, 'response_at': response_at, 'created_at': now_ts()})
    except Exception:
        logger.exception('写入 leads 失败: action=%s listing=%s', action, listing_id)
        return None
