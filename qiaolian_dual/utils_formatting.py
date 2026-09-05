"""从 user_bot.py 拆分出的职责模块。"""
from __future__ import annotations

from .common import *


def _fmt_price(price: object) -> str:
    if isinstance(price, (int, float)):
        return f'${int(price):,}/月' if price > 0 else '价格待确认'
    s = str(price or '').strip()
    if not s:
        return '价格待确认'
    digits = re.sub('[^\\d]', '', s)
    if digits:
        try:
            value = int(digits)
        except ValueError:
            return '价格待确认'
        if value > 0:
            return f'${value:,}/月'
    return '价格待确认'


def _display_listing_id(listing_id: object) -> str:
    """客户统一看到永久随机 QL 编号；内部键不变。"""
    from .public_listing_id import public_listing_id
    return public_listing_id(listing_id)


def _internal_listing_id(listing_id: object) -> str:
    """新 QL、旧 QC/QJ 和内部编号统一解析到内部 listing_id。"""
    from .public_listing_id import resolve_listing_id
    return resolve_listing_id(listing_id)


def _display_layout(layout: object, property_type: object='') -> str:
    """把内部户型写法转换成客户容易读的展示文案，不修改底层事实。"""
    raw = re.sub(r'\s+', '', str(layout or '').strip())
    if not raw:
        return ''
    kind = str(property_type or '').strip()
    is_commercial = any(word in kind for word in ('办公室', '办公楼', '写字楼', '商铺', '商业'))
    if not is_commercial:
        match = re.fullmatch(r'(\d+)房(\d+)办公(\d+)卫', raw)
        if match:
            rooms, studies, baths = match.groups()
            study_text = '书房' if studies == '1' else f'{studies}书房'
            return f'{rooms}房＋{study_text}｜{baths}卫'
        match = re.fullmatch(r'(\d+)房(\d+)厅(\d+)卫', raw)
        if match:
            rooms, halls, baths = match.groups()
            return f'{rooms}房{halls}厅｜{baths}卫'
    return raw.replace('|', '｜')


def _display_floor(floor: object) -> str:
    """楼层未知就隐藏；已知楼层只在展示层规范化。"""
    raw = str(floor or '').strip()
    if not raw or raw in {'未知', '待确认', '暂无', '[暂无]', '-', '--'}:
        return ''
    compact = re.sub(r'\s+', '', raw)
    if re.fullmatch(r'\d+', compact):
        return f'{int(compact)}楼'
    if re.fullmatch(r'\d+楼', compact):
        return compact
    return raw.replace('|', '｜')
