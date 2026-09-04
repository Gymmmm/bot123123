"""从 user_bot.py 拆分出的职责模块。"""
from __future__ import annotations

from .common import *

def listing_context(listing_id: str) -> dict:
    from .session_deeplink import _extract_caption_variant, _latest_draft_context
    listing_id = str(listing_id or '').strip()
    if not listing_id:
        return {}
    merged: dict = {}
    try:
        listing = db.get_listing(listing_id)
        if listing:
            merged.update(dict(listing))
    except Exception:
        logger.debug('用户 Bot 读取 listings 失败: %s', listing_id, exc_info=True)
    draft_ctx = _latest_draft_context(listing_id)
    if draft_ctx:
        for key in ('listing_id', 'area', 'layout', 'property_type', 'price', 'floor', 'size', 'title', 'project', 'deposit', 'available_date', 'cost_notes', 'normalized_data', 'water_rate', 'electric_rate'):
            if key not in merged or merged.get(key) in (None, '', 0, '0'):
                merged[key] = draft_ctx.get(key, merged.get(key))
        merged['caption_variant'] = _extract_caption_variant(draft_ctx.get('review_note'))
    if 'caption_variant' not in merged:
        merged['caption_variant'] = 'a'
    if not merged:
        return {'listing_id': listing_id, 'caption_variant': 'a'}
    merged.setdefault('listing_id', listing_id)
    # “完整实拍”必须读取整个冻结发布包：频道主图 + 评论区补充图。
    # 只读并按原顺序去重，不改动任何冻结文件。
    try:
        with sqlite3.connect(DB_PATH) as conn:
            row = conn.execute(
                "SELECT main_images_json, discussion_images_json FROM publication_packages WHERE property_id=? AND status IN ('published','approved','package_ready') ORDER BY id DESC LIMIT 1",
                (listing_id,),
            ).fetchone()
        if row:
            images: list[str] = []
            for raw_images in row:
                parsed = json.loads(raw_images) if isinstance(raw_images, str) and raw_images else raw_images
                if isinstance(parsed, list):
                    images.extend(str(path) for path in parsed if isinstance(path, str) and path.strip())
            merged['media_files'] = list(dict.fromkeys(images))
        else:
            with sqlite3.connect(DB_PATH) as conn:
                post_row = conn.execute(
                    "SELECT publication_package_id FROM posts WHERE listing_id=? AND platform='telegram' AND publish_status IN ('published','success','ok') AND COALESCE(publication_package_id,'')<>'' ORDER BY id DESC LIMIT 1",
                    (listing_id,),
                ).fetchone()
            package_id = str(post_row[0] or '').strip() if post_row else ''
            package_root = os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(DB_PATH))),
                'media', 'publication_packages', package_id,
            )
            if package_id and os.path.isdir(package_root):
                names = sorted(
                    name for name in os.listdir(package_root)
                    if re.fullmatch(r'image_\d+\.(?:jpg|jpeg|png|webp)', name, flags=re.I)
                )
                merged['media_files'] = [os.path.join(package_root, name) for name in names]
    except Exception:
        logger.debug('读取房源多图包失败: %s', listing_id, exc_info=True)
    return merged

# 详情页顾问核对语义保持一致：点“联系中文顾问”逐项核对。
def listing_cost_text(listing_id: str) -> str:
    """租赁详情只展示已有决策事实；缺失字段整行隐藏。"""
    from .utils_formatting import _fmt_price
    item = listing_context(listing_id)
    normalized: dict = {}
    raw_normalized = item.get('normalized_data')
    if isinstance(raw_normalized, dict):
        normalized = raw_normalized
    elif raw_normalized:
        try:
            normalized = json.loads(str(raw_normalized))
        except (TypeError, ValueError, json.JSONDecodeError):
            normalized = {}

    def fact(*values) -> str:
        for value in values:
            value = str(value or '').strip()
            if value and value not in {'待确认', '暂无', '[暂无]', '未知', '--', '-'}:
                return value
        return ''

    price_raw = item.get('price')
    price = _fmt_price(price_raw) if price_raw not in (None, '', 0, '0') else ''
    deposit = fact(item.get('deposit'), item.get('deposit_rule'), normalized.get('deposit_payment_terms'))
    contract = fact(normalized.get('contract_term_display'), normalized.get('contract_term'), item.get('contract_term'))
    management = fact(normalized.get('management_fee'))
    internet = fact(normalized.get('internet_fee'))
    water = fact(item.get('water_rate'), normalized.get('water_rate'))
    electric = fact(item.get('electric_rate'), normalized.get('electric_rate'))
    parking = fact(normalized.get('parking_fee'))

    amenities_raw = normalized.get('special_tags') or item.get('highlights') or []
    if isinstance(amenities_raw, str):
        amenities = [x.strip() for x in re.split(r'[、, ，| ｜·]', amenities_raw) if x.strip()]
    elif isinstance(amenities_raw, list):
        amenities = [str(x).strip() for x in amenities_raw if str(x).strip()]
    else:
        amenities = []
    amenities = list(dict.fromkeys(amenities))[:6]
    project = fact(item.get('project'), item.get('community'), item.get('area'))
    from .utils_formatting import _display_layout, _display_listing_id
    layout = fact(_display_layout(item.get('layout') or item.get('property_type'), item.get('property_type')))
    title = '｜'.join(value for value in (project, layout) if value)
    size = fact(item.get('size_sqm'), item.get('size'))
    floor = fact(item.get('floor'))
    lines = ['📋 <b>租赁详情</b>']
    if title:
        lines.extend(['', f'🏠 <b>{he(title)}</b>'])
    if price:
        lines.append(f'💰 <b>租金：{he(price)}</b>')
    fields = (
        ('🔑', '押付', deposit), ('📅', '租期', contract), ('📐', '面积', size),
        ('🏢', '楼层', floor), ('⚡', '电费', electric), ('💧', '水费', water),
        ('🏢', '物业费', management), ('🌐', '网络费', internet), ('🚗', '停车费', parking),
    )
    field_lines = [f'{emoji} <b>{label}：</b> {he(value)}' for emoji, label, value in fields if value]
    if field_lines:
        lines.extend(['', *field_lines])
    if amenities:
        lines.extend(['', '✨ <b>房源亮点</b>', he(' · '.join(amenities))])
    public_id = fact(_display_listing_id(listing_id))
    if public_id:
        lines.extend(['', f'<b>房源编号：</b> {he(public_id)}'])
    status = str(item.get('status') or '').strip().lower()
    status_bits = {
        'active': ('🟢', '当前可预约'),
        'reserved': ('🟡', '已有预约 · 仍可预约'),
        'pending': ('🔵', '房态待确认'),
        'rented': ('🔴', '已租出'),
        'inactive': ('⚫', '已下架'),
        'offline': ('⚫', '已下架'),
    }.get(status)
    if status_bits:
        emoji, status_text = status_bits
        lines.append(f'{emoji} <b>房态：</b> {status_text}')
    from .talk_engine import generate_talk
    talk = generate_talk(item, max_points=2, allow_empty=True).strip()
    if talk:
        safe_talk = '\n'.join(he(line) for line in talk.splitlines() if line.strip())
        lines.extend(['', '💬 <b>侨联说</b>', safe_talk])
    lines.extend([
        '',
        '📅 <b>想看这套？</b>',
        '点「预约看房」选时间。',
        '没时间到现场，也可以在预约里选择实时视频看房。',
    ])
    return '\n'.join(lines)
