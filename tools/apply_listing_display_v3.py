from pathlib import Path
import re


def replace_once(text: str, pattern: str, replacement: str, label: str) -> str:
    out, n = re.subn(pattern, lambda _m: replacement, text, count=1, flags=re.S)
    if n != 1:
        raise SystemExit(f'{label}: replacement count={n}')
    return out

# User Bot detail
p = Path('qiaolian_dual/listing.py')
s = p.read_text()
new_detail = '''def listing_cost_text(listing_id: str) -> str:
    """房源详情：有事实才显示；信息少也保持紧凑，不用“待确认”撑版面。"""
    from .talk_engine import generate_talk
    from .utils_formatting import _display_floor, _display_layout, _display_listing_id, _fmt_price

    item = listing_context(listing_id)
    normalized = _normalized_facts(item)
    area = _known_value(item.get('area'), item.get('community'))
    layout = _known_value(_display_layout(item.get('layout') or item.get('property_type'), item.get('property_type')))
    price = _fmt_price(item.get('price')) if item.get('price') not in (None, '', 0, '0') else ''
    size = _known_value(item.get('size_sqm'), item.get('size'))
    if size and '㎡' not in size:
        size += '㎡'
    floor = _display_floor(item.get('floor'))
    deposit = _known_value(item.get('deposit'), item.get('deposit_rule'), normalized.get('deposit_payment_terms'))
    contract = _known_value(normalized.get('contract_term_display'), normalized.get('contract_term'), item.get('contract_term'))
    lease = ' · '.join(v for v in (deposit, contract) if v)
    qc = _display_listing_id(listing_id)
    status = str(item.get('status') or 'pending').strip().lower()
    status_text = {
        'active': '当前可预约', 'reserved': '已有预约 · 仍可预约', 'pending': '房态确认中',
        'rented': '已租出', 'inactive': '已下架', 'offline': '已下架',
    }.get(status, '房态确认中')
    status_icon = {'active':'🟢','reserved':'🟡','pending':'🔵','rented':'🔴','inactive':'⚫','offline':'⚫'}.get(status, '🔵')

    lines = ['🏠 <b>房源详情</b>', '']
    if area: lines.append(f'🏠 区域：{he(area)}')
    if layout: lines.append(f'🛏 户型：{he(layout)}')
    if price: lines.append(f'💰 租金：{he(price)}')
    if size: lines.append(f'📐 面积：{he(size)}')
    if floor: lines.append(f'🏢 楼层：{he(floor)}')
    if lease: lines.append(f'🔑 租约：{he(lease)}')
    lines.append(f'{status_icon} 房态：{he(status_text)}')
    if qc: lines.append(f'📸 实拍：{he(qc)}')

    talk = generate_talk(item, max_points=2, allow_empty=True).strip()
    if talk:
        safe_talk = '\\n'.join(he(line) for line in talk.splitlines() if line.strip())
        lines.extend(['', '💬 <b>侨联说</b>', '', safe_talk])
    return '\\n'.join(lines)
'''
s = replace_once(s, r'def listing_cost_text\(listing_id: str\) -> str:\n.*?(?=\ndef listing_cost_keyboard\()', new_detail, 'listing_cost_text')
s = s.replace("'📋 租赁详情'", "'🏠 房源详情'").replace('"📋 租赁详情"', '"🏠 房源详情"')
p.write_text(s)

# Qiaolian talk: service/living facts first; fees only fill a spare slot.
p = Path('qiaolian_dual/talk_engine.py')
s = p.read_text()
old = '''    lines: list[str] = []\n\n    cost_line = _cost_talk_line(listing)\n    if cost_line:\n        lines.append(cost_line)\n\n    remaining = max(0, max_points - len(lines))\n    selected = choose_talk_tags(detect_talk_tags(listing), max_points=remaining)\n    for tag in selected:\n        if TALK_LIBRARY.get(tag):\n            lines.append(_stable_choice(TALK_LIBRARY[tag], listing_id, tag))\n'''
new = '''    lines: list[str] = []\n\n    selected = choose_talk_tags(detect_talk_tags(listing), max_points=max_points)\n    for tag in selected:\n        if TALK_LIBRARY.get(tag):\n            lines.append(_stable_choice(TALK_LIBRARY[tag], listing_id, tag))\n\n    if len(lines) < max_points:\n        cost_line = _cost_talk_line(listing)\n        if cost_line:\n            lines.append(cost_line)\n'''
if old in s:
    s = s.replace(old, new)
p.write_text(s)

# Channel caption: keep existing hashtag generation, rebuild visible facts with compact fallback.
p = Path('meihua_publisher.py')
s = p.read_text()
if 'def _build_channel_caption_legacy(' not in s:
    s = s.replace('def build_channel_caption(', 'def _build_channel_caption_legacy(', 1)
    marker = 'def build_rich_album_caption('
    if marker not in s:
        raise SystemExit('build_rich_album_caption marker missing')
    wrapper = '''def build_channel_caption(d: dict, album_paths: list[str], caption_variant: str | None = "a") -> str:\n    """频道主帖 V3：字段多分层，字段少自动压缩；标签继续沿用生产规则。"""\n    from qiaolian_dual.channel_links import public_qc_code\n\n    legacy = _build_channel_caption_legacy(d, album_paths, caption_variant=caption_variant)\n    hashtag_lines = [line.strip() for line in str(legacy or '').splitlines() if line.strip().startswith('#')]\n\n    def val(*keys):\n        for key in keys:\n            value = str(d.get(key) or '').strip()\n            if value and value not in {'待确认','暂无','未知','-','--'}:\n                return value\n        return ''\n\n    project = val('project','community','area') or '金边房源'\n    layout = _display_layout(val('layout','room_type','property_type'), val('property_type'))\n    title = '｜'.join(x for x in (project, layout) if x)\n    raw_price = d.get('price') or d.get('rent') or d.get('monthly_rent')\n    try:\n        number = float(str(raw_price).replace('$','').replace(',',''))\n        price = f"${number:,.0f}/月"\n    except Exception:\n        price = f"${raw_price}/月" if raw_price else ''\n\n    property_type = val('property_type')\n    size = val('size_sqm','size','area_sqm')\n    if size and '㎡' not in size: size += '㎡'\n    floor = val('floor')\n    floor_display = floor if not floor or '楼' in floor else floor + '楼'\n    deposit = val('deposit','deposit_rule')\n    lease = val('contract_term','lease_term','lease')\n\n    basic = [x for x in (property_type, size, floor_display) if x]\n    rental = [x for x in (deposit, (f'租期{lease}' if lease else '')) if x]\n    lines = [f'🏠 {title}']\n    if price: lines.append(f'💰 {price}')\n\n    if len(basic) + len(rental) <= 2:\n        compact = basic + rental\n        if compact: lines.extend(['', '🏢 ' + '｜'.join(compact)])\n    else:\n        if basic: lines.extend(['', '🏢 ' + '｜'.join(basic)])\n        if rental: lines.append('🔑 ' + '｜'.join(rental))\n\n    status = val('status').lower()\n    status_map = {\n        'active': ('🟢','当前可预约'), 'reserved': ('🟡','已有预约 · 仍可预约'),\n        'pending': ('🔵','房态确认中'), 'rented': ('🔴','已租出'),\n        'inactive': ('⚫','已下架'), 'offline': ('⚫','已下架'),\n    }\n    icon, status_text = status_map.get(status, ('🟢', val('status_text') or '当前可预约'))\n    code = public_qc_code(val('listing_id','id','code'))\n    lines.extend(['', f'{icon} {status_text}' + (f'　{code}' if code else '')])\n\n    if hashtag_lines:\n        lines.extend(['', hashtag_lines[-1]])\n    return '\\n'.join(lines)[:1024]\n\n\n'''
    s = s.replace(marker, wrapper + marker, 1)
s = s.replace('📋 租赁详情', '🏠 房源详情').replace('租赁详情', '房源详情')
p.write_text(s)

# Existing factual tests: service priority now intentionally wins over fee recital.
p = Path('tests/test_talk_engine_factual_copy.py')
s = p.read_text()
s = s.replace('''    assert "物业包了" in lines[0]\n    assert "电费$0.25/度" in lines[0]\n    assert "灭虫" in lines[1]\n''', '''    assert "灭虫" in lines[0]\n    assert "保洁" in lines[1]\n    assert "电费$0.25/度" not in lines[0]\n''')
p.write_text(s)

print('LISTING_DISPLAY_V3_APPLIED')
