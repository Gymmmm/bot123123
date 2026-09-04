"""Final public channel listing caption contract."""
from __future__ import annotations

import html
import re
from typing import Any, Iterable

from .channel_links import public_qc_code
from .utils_formatting import _display_floor, _display_layout

_STATUS_LABELS = {
    'active': '🟢 当前可预约',
    'reserved': '🟡 已有预约 · 仍可预约',
    'pending': '🔵 房态待确认',
    'rented': '🔴 已租出',
    'inactive': '⚫ 已下架',
    'offline': '⚫ 已下架',
}
_GENERIC_HEADINGS = {'侨联地产', '侨联精选', '精选房源', '优质房源', '房源', '金边房源'}
_EMPTY_FACTS = {'', '—', '-', '--', '暂无', '[暂无]', '未知', '待确认', '待定', '面议', '租金面议', '售价面议', '价格待确认', '随时入住', '即起', '现在', '立即'}


def _clean(value: Any, limit: int = 32) -> str:
    text = re.sub(r'\s+', ' ', str(value or '').strip()).replace('|', '｜')
    if text in _EMPTY_FACTS or text in _GENERIC_HEADINGS:
        return ''
    return text[:limit]


def _display_size(value: Any) -> str:
    raw = _clean(value, 18)
    if not raw:
        return ''
    normalized = raw.replace('平方米', '㎡').replace('平米', '㎡')
    normalized = re.sub(r'(?<=\d)平$', '㎡', normalized)
    if re.fullmatch(r'\d+(?:\.\d+)?', normalized):
        normalized += '㎡'
    return normalized


def _price_bucket(price: Any, deal_type: str) -> str:
    if deal_type != 'rent':
        return ''
    try:
        amount = int(float(price))
    except (TypeError, ValueError):
        return ''
    if amount <= 0:
        return ''
    if amount < 500:
        return '#租金500以下'
    if amount < 1000:
        return '#租金500至1000'
    if amount < 1500:
        return '#租金1000至1500'
    if amount < 2000:
        return '#租金1500至2000'
    if amount < 3000:
        return '#租金2000至3000'
    return '#租金3000以上'


def _safe_hashtag(value: str) -> str:
    token = re.sub(r'[^0-9A-Za-z\u4e00-\u9fff]+', '', str(value or ''))
    if not token or token in {'公寓', '金边'}:
        return ''
    return f'#{token}'


def _dedupe(values: Iterable[str]) -> list[str]:
    out: list[str] = []
    for value in values:
        value = str(value or '').strip()
        if value and value not in out:
            out.append(value)
    return out


def _layout_tag(layout: str) -> str:
    clean = str(layout or '').strip()
    if not clean:
        return ''
    if re.search(r'(单间|开间|studio)', clean, flags=re.I):
        return '#单间'
    match = re.search(r'(\d+)\s*房', clean)
    if match:
        number = int(match.group(1))
        cn = {1: '一', 2: '两', 3: '三', 4: '四', 5: '五'}.get(number, str(number))
        return f'#{cn}房'
    for cn in ('一', '两', '二', '三', '四', '五'):
        if f'{cn}房' in clean:
            return f"#{'两' if cn == '二' else cn}房"
    return ''


def _factual_tags(d: dict, *, heading: str, layout: str, deal_type: str) -> list[str]:
    """固定公开检索标签：区域 / 户型 / 价格段。"""
    values = [_safe_hashtag(heading), _layout_tag(layout), _price_bucket(d.get('price'), deal_type)]
    return [tag for tag in _dedupe(values) if tag]


def _normalize_contract(value: Any) -> str:
    text = _clean(value, 14)
    if not text:
        return ''
    return re.sub(r'^租期\s*', '', text)


def format_channel_listing_post(
    d: dict,
    listing_id: str = '',
    *,
    status: str | None = None,
    appointment_count: int = 0,
    extra_tags: Iterable[str] | None = None,
) -> str:
    """频道主帖：一眼判断房子、租金、基础条件和房态。"""
    del appointment_count, extra_tags  # 后台预约数量和营销 tag 不公开。
    project = _clean(d.get('project') or d.get('project_name'), 24)
    area = _clean(d.get('public_location_display') or d.get('area'), 24)
    heading = project if project and project not in _GENERIC_HEADINGS else (area or '金边房源')
    property_type = _clean(d.get('property_type'), 16)
    layout = _clean(_display_layout(d.get('layout') or d.get('room_type') or property_type or '整租', property_type), 18)

    deal_type = str(d.get('deal_type') or 'rent').strip().lower()
    try:
        amount = int(float(d.get('price')))
    except (TypeError, ValueError):
        amount = 0
    price_text = f'${amount:,}' + ('' if deal_type == 'sale' else '/月') if amount > 0 else ''

    size = _display_size(d.get('size') or d.get('size_sqm'))
    floor = _display_floor(_clean(d.get('floor'), 16))
    property_line = '｜'.join(value for value in (property_type, size, floor) if value)
    deposit = _clean(d.get('payment_terms') or d.get('deposit') or d.get('deposit_rule'), 18)
    contract = _normalize_contract(d.get('contract_term'))
    rental = '｜'.join(value for value in (deposit, f'租期{contract}' if contract else '') if value)

    effective_status = str(status if status is not None else d.get('status') or 'active').strip().lower()
    status_text = _STATUS_LABELS.get(effective_status, '🔵 房态待确认')
    qc_code = public_qc_code(listing_id or d.get('listing_id') or d.get('property_id') or '')

    lines = [f"🏠 <b>{html.escape('｜'.join(part for part in (heading, layout) if part) or '金边租房')}</b>"]
    if price_text:
        lines.append(f'💰 <b>{html.escape(price_text)}</b>')
    if property_line:
        lines.extend(['', f'🏢 {html.escape(property_line)}'])
    if rental:
        lines.append(f'🔑 {html.escape(rental)}')
    lines.extend(['', f'{status_text}　{html.escape(qc_code)}' if qc_code else status_text])
    tags = _factual_tags(d, heading=heading, layout=layout, deal_type=deal_type)
    if tags:
        lines.extend(['', ' '.join(tags)])
    return '\n'.join(lines).strip()[:1024]


def format_button_post_text(d: dict, listing_id: str = '', tag_lines: Iterable[str] | None = None, caption_variant: str = 'a') -> str:
    del tag_lines, caption_variant
    return format_channel_listing_post(d, listing_id=listing_id or (d.get('listing_id') if isinstance(d, dict) else '') or '', status=d.get('status') if isinstance(d, dict) else None)


__all__ = ['format_channel_listing_post', 'format_button_post_text']
