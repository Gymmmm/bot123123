"""从 user_bot.py 拆分出的职责模块。"""
from __future__ import annotations

import json

from .common import *


def parse_budget_range(text: str) -> tuple[int | None, int | None]:
    raw = str(text or '')
    values = [int(x) for x in re.findall(r'\d{2,5}', raw)]
    if not values:
        return (None, None)
    if len(values) == 1:
        value = values[0]
        if re.search(r'(以内|以下|不超过|最多)', raw):
            return (None, value)
        if re.search(r'(以上|起|至少)', raw):
            return (value, None)
        return (max(0, value - 200), value + 200)
    low, high = (min(values[0], values[1]), max(values[0], values[1]))
    return (low, high)


def detect_area(text: str) -> str:
    """从用户输入中识别位置，使用位置映射系统。"""
    from .location_mapping import LOCATION_MAP, normalize_user_input

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
        if any(v in lowered for v in variants):
            return label
    return ''


def detect_property_type(text: str) -> str:
    """仅提取可用于 listings.property_type 精确过滤的类型。"""
    lowered = (text or '').lower()
    mapping = (
        ('别墅', ('别墅', 'villa')),
        ('排屋', ('排屋', 'townhouse')),
        ('商铺', ('商铺', '店铺', 'shophouse', 'shop')),
        ('办公室', ('办公室', 'office')),
        ('公寓', ('公寓', 'apartment', 'studio')),
        ('住宅', ('住宅',)),
    )
    for canonical, keys in mapping:
        if any(k in lowered for k in keys):
            return canonical
    return ''


def _public_search_listings(
    *,
    property_type: str | None = None,
    areas: list[str] | tuple[str, ...] | None = None,
    budget_min: int | None = None,
    budget_max: int | None = None,
    ilike_fragment: str | None = None,
    limit: int = 6,
) -> list[dict]:
    """面向租客的公开房源查询，只返回真实发布且可预约的房源。"""
    table_names = db._table_names()
    if not {'drafts', 'posts'}.issubset(table_names):
        return []

    clauses = [
        "listings.status IN ('active','reserved')",
        """EXISTS (
            SELECT 1 FROM drafts d JOIN posts p ON p.draft_id=d.draft_id
            WHERE d.listing_id=listings.listing_id AND d.review_status='published'
              AND p.platform='telegram' AND p.publish_status IN ('published','success','ok')
        )""",
    ]
    params: list[object] = []

    if property_type:
        clauses.append('listings.property_type=?')
        params.append(property_type)

    cleaned_areas = [str(area).strip() for area in (areas or []) if str(area or '').strip() and str(area).strip() != '不限']
    if cleaned_areas:
        placeholders = ','.join('?' for _ in cleaned_areas)
        clauses.append(f'listings.area IN ({placeholders})')
        params.extend(cleaned_areas)

    if budget_min is not None:
        clauses.append('listings.price>=?')
        params.append(int(budget_min))
    if budget_max is not None:
        clauses.append('listings.price<=?')
        params.append(int(budget_max))

    fragment = str(ilike_fragment or '').strip()
    if fragment:
        token = f'%{fragment}%'
        clauses.append(
            "(listings.title LIKE ? OR listings.area LIKE ? OR listings.community LIKE ? "
            "OR listings.layout LIKE ? OR listings.property_type LIKE ? OR listings.highlights LIKE ?)"
        )
        params.extend([token] * 6)

    sql = (
        'SELECT listings.* FROM listings WHERE '
        + ' AND '.join(clauses)
        + ' ORDER BY CASE listings.status WHEN \'active\' THEN 0 ELSE 1 END, '
          'listings.updated_at DESC, listings.created_at DESC LIMIT ?'
    )
    params.append(max(1, int(limit)))

    with db.connect() as conn:
        rows = conn.execute(sql, params).fetchall()

    result: list[dict] = []
    for row in rows:
        item = {key: row[key] for key in row.keys()}
        raw_tags = item.pop('tags_json', '[]') or '[]'
        try:
            item['tags'] = json.loads(raw_tags)
        except (TypeError, ValueError, json.JSONDecodeError):
            item['tags'] = []
        result.append(item)
    return result


def _area_aliases(area: str | None) -> list[str] | None:
    """把客户侧组合区域投影成数据库可查询的 canonical aliases。"""
    raw = str(area or '').strip()
    if not raw or raw == '不限':
        return None
    from .location_mapping import get_all_location_aliases
    if raw in {'BKK2 / BKK3', 'BKK2/BKK3'}:
        return list(dict.fromkeys([
            *get_all_location_aliases('BKK2'),
            *get_all_location_aliases('BKK3'),
        ]))
    return get_all_location_aliases(raw)


def search_listings_with_fallback(
    *,
    property_type: str | None,
    area: str | None,
    budget_min: int | None,
    budget_max: int | None,
    text_fragment: str = '',
    limit: int = 3,
) -> tuple[list[dict], str]:
    """执行严格公开搜索；没有严格匹配时不自动放宽条件。"""
    del text_fragment
    matches = _public_search_listings(
        property_type=property_type or None,
        areas=_area_aliases(area),
        budget_min=budget_min,
        budget_max=budget_max,
        limit=limit,
    )
    if matches:
        return (matches, 'strict')
    return ([], 'no_match')


def search_similar_listings(
    *,
    property_type: str | None,
    area: str | None,
    budget_min: int | None,
    budget_max: int | None,
    limit: int = 3,
) -> tuple[list[dict], str]:
    """仅供用户主动点“看类似房源”后进行显式放宽。"""
    area_arg = _area_aliases(area)
    attempts = [
        ('no_type', dict(areas=area_arg, budget_min=budget_min, budget_max=budget_max)),
        ('no_area', dict(property_type=property_type or None, budget_min=budget_min, budget_max=budget_max)),
        ('budget_only', dict(budget_min=budget_min, budget_max=budget_max)),
    ]
    for mode, kwargs in attempts:
        matches = _public_search_listings(limit=limit, **kwargs)
        if matches:
            return (matches, mode)
    return ([], 'no_match')


def upsert_user_profile(user) -> None:
    """仅对真实 Telegram User 写入资料；频道/系统更新可能没有 from_user。"""
    from .session_deeplink import now_ts

    if user is None or not getattr(user, 'id', None):
        return
    db.upsert_user(
        int(user.id),
        getattr(user, 'username', '') or '',
        getattr(user, 'first_name', '') or '',
        getattr(user, 'last_name', '') or '',
        now_ts(),
    )


def create_lead(
    user,
    *,
    action: str,
    source: str,
    listing_id: str = '',
    area: str = '',
    property_type: str = '',
    budget_min: int | None = None,
    budget_max: int | None = None,
    payload: dict | None = None,
) -> int | None:
    from .attribution import attach_to_lead_payload, remember_touch
    from .attribution_store import apply_lead_attribution_columns
    from .session_deeplink import _normalize_variant, now_ts, user_display_name

    if user is None or not getattr(user, 'id', None):
        logger.warning('跳过无用户身份的线索写入: action=%s listing=%s', action, listing_id)
        return None
    touch = remember_touch(
        user,
        action=action,
        source=source,
        listing_id=listing_id,
        payload=payload,
    )
    lead_payload = attach_to_lead_payload(payload, touch)
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
        lead_id = db.create_lead({
            'user_id': user.id,
            'username': getattr(user, 'username', '') or '',
            'display_name': user_display_name(user),
            'source': source,
            'action': action,
            'listing_id': listing_id,
            'area': area,
            'property_type': property_type,
            'budget_min': budget_min,
            'budget_max': budget_max,
            'payload': lead_payload,
            'message_id': message_id,
            'post_token': post_token,
            'caption_variant': caption_variant,
            'agent_id': agent_id,
            'response_at': response_at,
            'created_at': now_ts(),
        })
        apply_lead_attribution_columns(lead_id, touch)
        return lead_id
    except Exception:
        logger.exception('写入 leads 失败: action=%s listing=%s', action, listing_id)
        return None
