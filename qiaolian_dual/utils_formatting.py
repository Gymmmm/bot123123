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
    """客户统一看到 QCxxxx；内部数据库和深链继续使用 l_xxxx。"""
    raw = str(listing_id or '').strip()
    match = re.fullmatch('(?i)l[_-]?(\\d+)', raw)
    return f'QC{int(match.group(1)):04d}' if match else raw.upper()
