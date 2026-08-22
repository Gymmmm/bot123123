"""从 user_bot.py 拆分出的职责模块。"""
from __future__ import annotations

from .common import *

def user_display_name(user) -> str:
    return (getattr(user, 'full_name', '') or getattr(user, 'first_name', '') or '').strip()

def now_ts() -> str:
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def clear_main_flags(context: ContextTypes.DEFAULT_TYPE) -> None:
    for key in ('awaiting_consult', 'awaiting_want_home', 'awaiting_service_request', 'awaiting_old_customer', 'service_request_detail', 'awaiting_general_appointment', 'awaiting_keyword_find', 'search_pref'):
        context.user_data.pop(key, None)

def clear_session_for_fresh_entry(context: ContextTypes.DEFAULT_TYPE) -> None:
    """新 /start 或作为入口的斜杠命令：清咨询/找房标志，并丢弃未完成的预约草稿，避免与新的深链打架。"""
    clear_main_flags(context)
    context.user_data.pop('appt', None)
    context.user_data.pop('contact_touch_payload', None)

def _remember_video_pref(context: ContextTypes.DEFAULT_TYPE, *, area: str | None=None, budget_min: int | None=None, budget_max: int | None=None, layout: str | None=None) -> None:
    snap = context.user_data.get('video_pref')
    if not isinstance(snap, dict):
        snap = {}
    if area is not None:
        snap['area'] = str(area or '').strip()
    if budget_min is not None:
        snap['budget_min'] = budget_min
    if budget_max is not None:
        snap['budget_max'] = budget_max
    if layout is not None:
        snap['layout'] = str(layout or '').strip()
    context.user_data['video_pref'] = snap

def _base36_decode(token: str) -> int | None:
    try:
        return int((token or '').lower(), 36)
    except ValueError:
        return None

def parse_start_arg_payload(arg: str) -> dict | None:
    from .texts import _channel_index_action
    index_payload = _channel_index_action(arg)
    if index_payload is not None:
        return index_payload
    _static_actions = {'brand', 'about', 'want_home', 'ask', 'find_home', 'area_index', 'latest', 'cooperate', 'consult_general'}
    if arg in _static_actions:
        return {'action': arg, 'target': '', 'post_token': '', 'channel_message_id': None}
    if arg == 'more':
        return {'action': 'more', 'target': '', 'post_token': '', 'channel_message_id': None}
    if arg.startswith('t_bind_'):
        return {'action': 'tenant_bind', 'target': arg[len('t_bind_'):], 'post_token': '', 'channel_message_id': None}
    if arg.startswith('ch__'):
        return {'action': 'channel_topic', 'target': arg[len('ch__'):], 'post_token': '', 'channel_message_id': None}
    if arg.startswith('ch_'):
        return {'action': 'channel_topic', 'target': arg[len('ch_'):], 'post_token': '', 'channel_message_id': None}
    if arg.startswith('book__'):
        token = arg[len('book__'):].split('__', 1)[0]
        if token.startswith('ql'):
            return {'action': 'appoint', 'target': '', 'post_token': token, 'opaque_token': token, 'channel_message_id': None}
    if arg.startswith('consult__'):
        token = arg[len('consult__'):].split('__', 1)[0]
        if token.startswith('ql'):
            return {'action': 'consult', 'target': '', 'post_token': token, 'opaque_token': token, 'channel_message_id': None}
    if arg.startswith('discussion_entry__'):
        parts = arg.split('__', 2)
        if len(parts) == 3:
            post_token, listing_id = (parts[1], parts[2])
            return {'action': 'discussion_entry', 'target': listing_id, 'post_token': post_token, 'channel_message_id': _base36_decode(post_token)}
        return {'action': 'discussion_entry', 'target': '', 'post_token': '', 'channel_message_id': None}
    _old_suffix_map = {'_appoint': 'book', '_consult': 'consult', '_video': 'video'}
    for suffix, mapped_action in _old_suffix_map.items():
        if arg.endswith(suffix):
            target = arg[:-len(suffix)]
            if target:
                return {'action': mapped_action, 'target': target, 'post_token': '', 'channel_message_id': None}
    _new_prefix_actions = ('book', 'ask', 'similar', 'video')
    for action in _new_prefix_actions:
        prefix = f'{action}_'
        if arg.startswith(prefix):
            return {'action': 'consult' if action == 'ask' else action, 'target': arg[len(prefix):], 'post_token': '', 'channel_message_id': None}
    if re.match('^l_\\d+$', arg):
        return {'action': 'consult', 'target': arg, 'post_token': '', 'channel_message_id': None}
    for alias, action in START_ACTION_ALIASES.items():
        prefix = f'{alias}__'
        if arg.startswith(prefix):
            parts = arg.split('__', 2)
            if len(parts) == 2 and parts[1].startswith('ql'):
                return {'action': action, 'target': '', 'post_token': parts[1], 'opaque_token': parts[1], 'channel_message_id': None}
            if len(parts) == 3:
                post_token, target = (parts[1], parts[2])
                return {'action': action, 'target': target, 'post_token': post_token, 'channel_message_id': _base36_decode(post_token)}
        legacy_prefix = f'{alias}_'
        if arg.startswith(legacy_prefix):
            return {'action': action, 'target': arg[len(legacy_prefix):], 'post_token': '', 'channel_message_id': None}
    for action in START_ACTIONS:
        prefix = f'{action}__'
        if arg.startswith(prefix):
            parts = arg.split('__', 2)
            if len(parts) == 2 and parts[1].startswith('ql'):
                return {'action': action, 'target': '', 'post_token': parts[1], 'opaque_token': parts[1], 'channel_message_id': None}
            if len(parts) == 3:
                post_token, target = (parts[1], parts[2])
                return {'action': action, 'target': target, 'post_token': post_token, 'channel_message_id': _base36_decode(post_token)}
        legacy_prefix = f'{action}_'
        if arg.startswith(legacy_prefix):
            return {'action': action, 'target': arg[len(legacy_prefix):], 'post_token': '', 'channel_message_id': None}
    return None

def build_source_label(post_token: str) -> str:
    return f'channel_post:{post_token}' if post_token else 'channel_deeplink'

def _deep_link(payload: str) -> str:
    return f'https://t.me/{USER_BOT_USERNAME}?start={quote(payload)}'

def _build_start_payload(action: str, target: str, **meta: str) -> str:
    action_code = START_ACTION_CODES.get(action, action)
    payload = str(target or '').strip()
    meta_parts = [f'{key}={value}' for key, value in meta.items() if str(value or '').strip()]
    if meta_parts:
        payload = '|'.join([payload, *meta_parts])
    return f'{action_code}_{payload}'

def _extract_caption_variant(review_note: str | None) -> str:
    m = re.search('caption_variant:(a|b|c)', str(review_note or ''), flags=re.IGNORECASE)
    return m.group(1).lower() if m else 'a'

def _normalize_variant(raw: str | None) -> str:
    v = str(raw or '').strip().lower()
    return v if v in {'a', 'b', 'c'} else ''

def _split_target_meta(raw_target: str | None) -> tuple[str, dict[str, str]]:
    raw = str(raw_target or '').strip()
    if not raw:
        return ('', {})
    parts = [p.strip() for p in raw.split('|') if p.strip()]
    if not parts:
        return ('', {})
    target = parts[0]
    meta: dict[str, str] = {}
    for item in parts[1:]:
        if '=' not in item:
            continue
        key, value = item.split('=', 1)
        key = key.strip().lower()
        value = value.strip()
        if key:
            meta[key] = value
    return (target, meta)

def _latest_draft_context(listing_id: str) -> dict:
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute('SELECT listing_id, title, project, area, layout, property_type, price, floor, size,\n                          deposit, available_date, cost_notes, normalized_data, water_rate, electric_rate,\n                          review_note\n                   FROM drafts\n                   WHERE listing_id=?\n                   ORDER BY id DESC\n                   LIMIT 1', (listing_id,)).fetchone()
            return dict(row) if row else {}
    except Exception:
        logger.debug('读取 drafts 上下文失败: %s', listing_id, exc_info=True)
        return {}

def _latest_draft_review_status(listing_id: str) -> str:
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute('\n                SELECT review_status\n                FROM drafts\n                WHERE listing_id=?\n                ORDER BY id DESC\n                LIMIT 1\n                ', (listing_id,)).fetchone()
            return str((dict(row) if row else {}).get('review_status') or '').strip().lower()
    except Exception:
        logger.debug('读取 drafts 状态失败: %s', listing_id, exc_info=True)
        return ''


def resolve_public_token(token: str) -> str:
    token = str(token or '').strip()
    if not token:
        return ''
    try:
        with sqlite3.connect(DB_PATH) as conn:
            row = conn.execute("""SELECT pp.property_id
                FROM publication_packages pp
                JOIN listings l ON l.listing_id=pp.property_id AND l.status='active'
                JOIN drafts d ON d.listing_id=l.listing_id AND d.review_status='published'
                JOIN posts p ON p.listing_id=l.listing_id
                    AND p.platform='telegram' AND p.publish_status IN ('published','success','ok')
                WHERE pp.public_token=? AND pp.status IN ('approved','published')
                ORDER BY pp.package_version DESC LIMIT 1""", (token,)).fetchone()
            return str(row[0] or '').strip() if row else ''
    except Exception:
        logger.debug('解析 public token 失败: %s', token, exc_info=True)
        return ''
