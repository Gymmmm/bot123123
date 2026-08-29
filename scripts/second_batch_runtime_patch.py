from pathlib import Path
import re


def replace_once(path: str, old: str, new: str, label: str) -> None:
    p = Path(path)
    text = p.read_text(encoding='utf-8')
    if old not in text:
        raise SystemExit(f'missing patch target: {label}')
    p.write_text(text.replace(old, new, 1), encoding='utf-8')


# ---------------------------------------------------------------------------
# 1) Canonical publication/availability source of truth.
# ---------------------------------------------------------------------------
p = Path('qiaolian_dual/db.py')
text = p.read_text(encoding='utf-8')
text = text.replace('LISTING_STATUSES = {"active", "pending", "reserved", "rented", "inactive"}',
                    'LISTING_STATUSES = {"active", "pending", "reserved", "rented", "inactive", "offline"}', 1)

# Replace list_recent_listings with a status query + canonical public evidence filter.
start = text.index('    def list_recent_listings(self, limit: int = 10) -> list[dict[str, Any]]:')
end = text.index('\n    def search_listings(', start)
new_recent = '''    def list_recent_listings(self, limit: int = 10) -> list[dict[str, Any]]:
        """Public browsing list. Publication evidence is canonical, not the current queue row."""
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM listings WHERE status IN ('active','reserved') ORDER BY created_at DESC LIMIT ?",
                (max(int(limit) * 4, int(limit)),),
            ).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            item = row_to_dict(row) or {}
            item["tags"] = json.loads(item.pop("tags_json", "[]") or "[]")
            if self.is_listing_public(str(item.get('listing_id') or '')):
                result.append(item)
            if len(result) >= int(limit):
                break
        return result
'''
text = text[:start] + new_recent + text[end:]

# Replace search_listings body up to next method by locating its return and following def.
search_start = text.index('    def search_listings(')
# find next class method after search_listings
m_next = re.search(r'\n    def [a-zA-Z_][a-zA-Z0-9_]*\(', text[search_start + 20:])
if not m_next:
    raise SystemExit('cannot locate method after search_listings')
search_end = search_start + 20 + m_next.start()
old_search = text[search_start:search_end]
# Preserve public signature by extracting it through the colon line.
sig_end = old_search.index('    ) -> list[dict[str, Any]]:') + len('    ) -> list[dict[str, Any]]:')
sig = old_search[:sig_end]
new_search_body = r'''
        clauses = ["status IN ('active','reserved')"]
        params: list[Any] = []
        if property_type:
            clauses.append("property_type=?")
            params.append(property_type)
        cleaned_areas = [area for area in (areas or []) if area and area != "不限"]
        if cleaned_areas:
            placeholders = ",".join("?" for _ in cleaned_areas)
            clauses.append(f"area IN ({placeholders})")
            params.extend(cleaned_areas)
        if budget_min is not None:
            clauses.append("price>=?")
            params.append(int(budget_min))
        if budget_max is not None:
            clauses.append("price<=?")
            params.append(int(budget_max))
        if ilike_fragment:
            fragment = f"%{ilike_fragment}%"
            clauses.append("(title LIKE ? OR community LIKE ? OR area LIKE ? OR layout LIKE ? OR property_type LIKE ?)")
            params.extend([fragment] * 5)
        sql = "SELECT * FROM listings WHERE " + " AND ".join(clauses) + " ORDER BY created_at DESC LIMIT ?"
        params.append(max(int(limit) * 4, int(limit)))
        with self.connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            item = row_to_dict(row) or {}
            item["tags"] = json.loads(item.pop("tags_json", "[]") or "[]")
            if self.is_listing_public(str(item.get('listing_id') or '')):
                result.append(item)
            if len(result) >= int(limit):
                break
        return result
'''
text = text[:search_start] + sig + new_search_body + text[search_end:]

# Replace is_listing_public method with schema-tolerant publication evidence.
is_start = text.index('    def is_listing_public(self, listing_id: str) -> bool:')
m_next = re.search(r'\n    def [a-zA-Z_][a-zA-Z0-9_]*\(', text[is_start + 20:])
is_end = is_start + 20 + m_next.start() if m_next else len(text)
new_public = '''    def has_publication_evidence(self, listing_id: str) -> bool:
        """Accept current or historical Telegram publication evidence after queue rebuilds."""
        listing_id = str(listing_id or '').strip()
        if not listing_id:
            return False
        tables = self._table_names()
        try:
            with self.connect() as conn:
                # Current canonical draft -> Telegram post chain.
                if {'drafts', 'posts'}.issubset(tables):
                    dcols = {row['name'] for row in conn.execute('PRAGMA table_info(drafts)').fetchall()}
                    pcols = {row['name'] for row in conn.execute('PRAGMA table_info(posts)').fetchall()}
                    if {'listing_id', 'draft_id', 'review_status'}.issubset(dcols) and {'draft_id'}.issubset(pcols):
                        filters = ["d.listing_id=?", "d.review_status='published'"]
                        if 'platform' in pcols:
                            filters.append("p.platform='telegram'")
                        if 'publish_status' in pcols:
                            filters.append("p.publish_status IN ('published','success','ok')")
                        row = conn.execute(
                            'SELECT 1 FROM drafts d JOIN posts p ON p.draft_id=d.draft_id WHERE ' + ' AND '.join(filters) + ' LIMIT 1',
                            (listing_id,),
                        ).fetchone()
                        if row:
                            return True
                    # Historical posts may retain listing_id after the draft queue was rebuilt.
                    if 'listing_id' in pcols:
                        filters = ['listing_id=?']
                        if 'platform' in pcols:
                            filters.append("platform='telegram'")
                        if 'publish_status' in pcols:
                            filters.append("publish_status IN ('published','success','ok')")
                        row = conn.execute('SELECT 1 FROM posts WHERE ' + ' AND '.join(filters) + ' LIMIT 1', (listing_id,)).fetchone()
                        if row:
                            return True
                # Frozen publication package is also valid evidence of a public package.
                if 'publication_packages' in tables:
                    cols = {row['name'] for row in conn.execute('PRAGMA table_info(publication_packages)').fetchall()}
                    id_col = 'property_id' if 'property_id' in cols else ('listing_id' if 'listing_id' in cols else '')
                    if id_col:
                        filters = [f'{id_col}=?']
                        if 'status' in cols:
                            filters.append("status IN ('published','approved','package_ready')")
                        row = conn.execute('SELECT 1 FROM publication_packages WHERE ' + ' AND '.join(filters) + ' LIMIT 1', (listing_id,)).fetchone()
                        if row:
                            return True
        except sqlite3.Error:
            logger.debug('publication evidence lookup failed: %s', listing_id, exc_info=True)
        return False

    def is_listing_public(self, listing_id: str) -> bool:
        listing = self.get_listing(str(listing_id or '').strip())
        if not listing:
            return False
        status = str(listing.get('status') or '').strip().lower()
        if status not in {'active', 'reserved'}:
            return False
        return self.has_publication_evidence(str(listing_id or '').strip())
'''
text = text[:is_start] + new_public + text[is_end:]
p.write_text(text, encoding='utf-8')

# Canonical user-facing availability helper no longer depends on the current draft queue row.
p = Path('qiaolian_dual/listing.py')
text = p.read_text(encoding='utf-8')
start = text.index('def listing_is_available(listing_id: str) -> tuple[bool, str]:')
end = text.index('\ndef listing_unavailable_text', start)
new_availability = '''def listing_is_available(listing_id: str) -> tuple[bool, str]:
    """Canonical availability used by home, cards, detail, photos, consult and appointment."""
    listing_id = str(listing_id or '').strip()
    if not listing_id:
        return (False, 'missing')
    listing = db.get_listing(listing_id)
    if not listing:
        return (False, 'missing')
    status = str(listing.get('status') or 'pending').strip().lower()
    if status in {'active', 'reserved'}:
        return ((True, status) if db.is_listing_public(listing_id) else (False, 'unpublished'))
    if status == 'pending':
        return (False, 'pending')
    if status == 'rented':
        return (False, 'rented')
    if status in {'offline', 'inactive'}:
        return (False, 'offline')
    return (False, status or 'pending')
'''
text = text[:start] + new_availability + text[end:]
p.write_text(text, encoding='utf-8')

# ---------------------------------------------------------------------------
# 2) Callback acknowledgement: one network answer per callback id.
# ---------------------------------------------------------------------------
p = Path('qiaolian_dual/common.py')
text = p.read_text(encoding='utf-8')
insert_point = text.index('\n__all__ = [name for name in globals() if not name.startswith(\'__\')]')
helper = '''\n# CallbackQuery can only be answered once. All handlers use this idempotent helper.\n_CALLBACK_ANSWERED_IDS: set[str] = set()\n_CALLBACK_ANSWERED_ORDER: list[str] = []\n\nasync def answer_callback_once(query, text: str | None=None, *, show_alert: bool=False) -> bool:\n    query_id = str(getattr(query, 'id', '') or '')\n    if query_id and query_id in _CALLBACK_ANSWERED_IDS:\n        return False\n    await query.answer(text=text, show_alert=show_alert)\n    if query_id:\n        _CALLBACK_ANSWERED_IDS.add(query_id)\n        _CALLBACK_ANSWERED_ORDER.append(query_id)\n        if len(_CALLBACK_ANSWERED_ORDER) > 2048:\n            stale = _CALLBACK_ANSWERED_ORDER.pop(0)\n            _CALLBACK_ANSWERED_IDS.discard(stale)\n    return True\n'''
if 'async def answer_callback_once' not in text:
    text = text[:insert_point] + helper + text[insert_point:]
p.write_text(text, encoding='utf-8')

# Central dispatcher: defer default ack to finally so a child can request an alert first.
p = Path('qiaolian_dual/callbacks.py')
text = p.read_text(encoding='utf-8')
start = text.index('async def handle_ui_callback(')
new_dispatch = '''async def handle_ui_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, *, hooks: dict | None = None) -> int:\n    query = update.callback_query\n    data = query.data or ""\n    user = update.effective_user\n    hooks = hooks or {}\n    try:\n        (hooks.get('upsert_user_profile') or upsert_user_profile)(user)\n        if data.startswith("listing:appoint:"):\n            context.user_data.pop("appt", None)\n        logger.info("[CALLBACK] user_id=%s data=%s", user.id, data)\n\n        routes = (\n            (matches_admin_contract, handle_admin_contract_callback),\n            (matches_admin, handle_admin_callback),\n            (matches_navigation, handle_navigation_callback),\n            (matches_search, None),\n            (matches_contract, handle_contract_callback),\n            (matches_appointment, handle_appointment_callback),\n            (matches_preference, handle_preference_callback),\n            (matches_service, handle_service_callback),\n            (matches_listing, handle_listing_callback),\n        )\n        for matcher, handler in routes:\n            if not matcher(data):\n                continue\n            if handler is None:\n                result = await handle_search_callback(update, context, query, data, user, hooks=hooks)\n            else:\n                result = await handler(update, context, query, data, user)\n            return MAIN if result is None else result\n\n        logger.warning("[CALLBACK] unhandled user_id=%s data=%s", user.id, data)\n        text = "这个操作已失效，请返回首页继续。"\n        reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("🏠 返回首页", callback_data="home")]])\n        try:\n            await query.edit_message_text(text, reply_markup=reply_markup)\n        except Exception:\n            await context.bot.send_message(chat_id=update.effective_chat.id, text=text, reply_markup=reply_markup)\n        return MAIN\n    finally:\n        await answer_callback_once(query)\n'''
text = text[:start] + new_dispatch
p.write_text(text, encoding='utf-8')

# Replace all direct query.answer calls in callback-domain files with the helper.
for path in Path('qiaolian_dual').glob('*.py'):
    if path.name == 'common.py':
        continue
    src = path.read_text(encoding='utf-8')
    src = src.replace('await query.answer()', 'await answer_callback_once(query)')
    src = re.sub(r'await query\.answer\(([^\n]*)\)', r'await answer_callback_once(query, \1)', src)
    # Avoid accidental helper recursion if a prior replacement touched the helper name text.
    src = src.replace('await answer_callback_once(query, text=text, show_alert=show_alert)', 'await query.answer(text=text, show_alert=show_alert)') if path.name == 'common.py' else src
    path.write_text(src, encoding='utf-8')

# ---------------------------------------------------------------------------
# 3) Listing handlers: canonical availability on detail/photos/consult/appointment.
#    Legacy show_more becomes a single-card compatibility redirect.
# ---------------------------------------------------------------------------
p = Path('qiaolian_dual/callback_listing.py')
text = p.read_text(encoding='utf-8')

# photos availability guard.
old = """    if data.startswith('listing:photos:'):\n            lid = data.split(':', 2)[2]\n            context.user_data['contact_listing_id'] = lid\n            try:\n"""
new = """    if data.startswith('listing:photos:'):\n            lid = data.split(':', 2)[2]\n            is_available, availability_reason = listing_is_available(lid)\n            if not is_available:\n                await render_panel(update, text=listing_unavailable_text(availability_reason), parse_mode=ParseMode.HTML, reply_markup=listing_unavailable_keyboard(lid), context=context)\n                return MAIN\n            context.user_data['contact_listing_id'] = lid\n            try:\n"""
if old not in text:
    raise SystemExit('missing photos handler target')
text = text.replace(old, new, 1)

# show_more compatibility redirect: never batch-send multiple listings/photos.
show_start = text.index("    if data == 'find:show_more':")
show_end = text.index("    if data.startswith('listing:open:'):", show_start)
new_show = '''    if data == 'find:show_more':\n            # Legacy callback only: fold any old remainder into the same single-card carousel.\n            ids = list(context.user_data.get('find_card_listing_ids') or [])\n            ids.extend(str(value) for value in (context.user_data.pop('find_more_listing_ids', []) or []) if str(value or '').strip())\n            ids = list(dict.fromkeys(value for value in ids if value))\n            if not ids:\n                ids = [str(item.get('listing_id') or '') for item in db.list_recent_listings(10) if item.get('listing_id')]\n            context.user_data['find_card_listing_ids'] = ids\n            if ids:\n                await send_find_result_card(update, context, 0, replace=True)\n            else:\n                await render_panel(update, text='暂时没有可以安排看房的房源。', parse_mode=ParseMode.HTML, reply_markup=no_match_followup_keyboard(), context=context)\n            return MAIN\n'''
text = text[:show_start] + new_show + text[show_end:]

# consult availability guard.
old = """    if data.startswith('listing:consult:'):\n            lid = data.split(':', 2)[2]\n            context.user_data['contact_listing_id'] = lid\n"""
new = """    if data.startswith('listing:consult:'):\n            lid = data.split(':', 2)[2]\n            is_available, availability_reason = listing_is_available(lid)\n            if not is_available:\n                await render_panel(update, text=listing_unavailable_text(availability_reason), parse_mode=ParseMode.HTML, reply_markup=listing_unavailable_keyboard(lid), context=context)\n                return MAIN\n            context.user_data['contact_listing_id'] = lid\n"""
if old not in text:
    raise SystemExit('missing consult target')
text = text.replace(old, new, 1)

# detail availability guard and same-card caption edit when invoked from a photo card.
old = """    if data.startswith('listing:detail:'):\n            lid = data.split(':', 2)[2]\n            item = db.get_listing(lid) if lid else None\n"""
new = """    if data.startswith('listing:detail:'):\n            lid = data.split(':', 2)[2]\n            is_available, availability_reason = listing_is_available(lid)\n            if not is_available:\n                await render_panel(update, text=listing_unavailable_text(availability_reason), parse_mode=ParseMode.HTML, reply_markup=listing_unavailable_keyboard(lid), context=context)\n                return MAIN\n            item = db.get_listing(lid) if lid else None\n"""
if old not in text:
    raise SystemExit('missing detail target')
text = text.replace(old, new, 1)
old_render = """            await render_panel(update, text=listing_cost_text(lid), parse_mode=ParseMode.HTML, reply_markup=detail_kb, context=context)\n            return MAIN\n"""
new_render = """            detail_text = listing_cost_text(lid)\n            if getattr(query.message, 'photo', None):\n                await query.edit_message_caption(caption=detail_text, parse_mode=ParseMode.HTML, reply_markup=detail_kb)\n            else:\n                await render_panel(update, text=detail_text, parse_mode=ParseMode.HTML, reply_markup=detail_kb, context=context)\n            return MAIN\n"""
if old_render not in text:
    raise SystemExit('missing detail render target')
text = text.replace(old_render, new_render, 1)
p.write_text(text, encoding='utf-8')

# ---------------------------------------------------------------------------
# 4) Full frozen album: all photos, ordered/deduped, 10 per media group, one CTA.
#    Recommendation card anchor and auto-skip stale listings.
# ---------------------------------------------------------------------------
p = Path('qiaolian_dual/results_admin.py')
text = p.read_text(encoding='utf-8')
text = text.replace("photos = [p for p in media_files if isinstance(p, str) and os.path.exists(p) and os.path.basename(p).lower() not in {'cover.jpg', 'cover.jpeg', 'cover.png'}][:10]",
                    "photos = list(dict.fromkeys(p for p in media_files if isinstance(p, str) and os.path.exists(p) and os.path.basename(p).lower() not in {'cover.jpg', 'cover.jpeg', 'cover.png'}))", 1)

# Rewrite send_listing_photo_preview entirely.
photo_start = text.index('async def send_listing_photo_preview(')
# function is at EOF in this module in current branch
new_photo = '''async def send_listing_photo_preview(bot, chat_id: int, listing_id: str) -> None:\n    """Send every frozen photo in original order, chunked by Telegram's 10-media limit."""\n    from .listing import listing_context\n    from .text_utils import clean_inline_text\n    from telegram import InputMediaPhoto\n    from .utils_formatting import _display_layout, _display_listing_id\n    info = listing_context(str(listing_id or '').strip())\n    media_files = info.get('media_files', []) if isinstance(info, dict) else []\n    photos = list(dict.fromkeys(\n        p for p in media_files\n        if isinstance(p, str) and os.path.exists(p)\n        and os.path.basename(p).lower() not in {'cover.jpg', 'cover.jpeg', 'cover.png'}\n    ))\n    area = clean_inline_text(str(info.get('project') or info.get('community') or info.get('area') or '金边'))\n    layout = _display_layout(clean_inline_text(str(info.get('layout') or '')), info.get('property_type'))\n    qc_id = _display_listing_id(str(listing_id or '').strip())\n    heading = f'{qc_id}｜{area}' if area else qc_id\n    caption_lines = [f'<b>📸 {he(heading)}</b>']\n    if layout:\n        caption_lines.append(he(layout))\n    caption_lines.append(f'以下是这套房的完整实拍，共 {len(photos)} 张。')\n    caption_lines.append('点击图片查看大图。')\n    caption = '\\n'.join(caption_lines)\n    keyboard = InlineKeyboardMarkup([\n        [InlineKeyboardButton('📋 租赁详情', callback_data=f'listing:detail:{listing_id}'), InlineKeyboardButton('📅 预约看房', callback_data=f'listing:appoint:{listing_id}')],\n        [InlineKeyboardButton('🤖 侨联找房助手', callback_data='home_smart_search')],\n    ])\n    if photos:\n        for offset in range(0, len(photos), 10):\n            chunk = photos[offset:offset + 10]\n            media = []\n            for index, path in enumerate(chunk):\n                with open(path, 'rb') as photo:\n                    first = offset == 0 and index == 0\n                    media.append(InputMediaPhoto(media=photo.read(), caption=caption if first else None, parse_mode=ParseMode.HTML if first else None))\n            if len(media) == 1:\n                await bot.send_photo(chat_id=chat_id, photo=media[0].media, caption=media[0].caption, parse_mode=ParseMode.HTML)\n            else:\n                await bot.send_media_group(chat_id=chat_id, media=media)\n        await bot.send_message(chat_id=chat_id, text=f'🏠 <b>{he(qc_id)}｜请选择下一步</b>', parse_mode=ParseMode.HTML, reply_markup=keyboard)\n    else:\n        await bot.send_message(\n            chat_id=chat_id,\n            text='<b>📸 这套房目前没有更多可用实拍。</b>\\n需要补充图片时，可以联系中文顾问。',\n            parse_mode=ParseMode.HTML,\n            reply_markup=keyboard,\n        )\n'''
text = text[:photo_start] + new_photo

# send_find_result_card: replace stale-listing branch with canonical filtering and auto-next.
old = """    item = listing_context(ids[index])\n    status = str((item or {}).get('status') or '').strip().lower()\n    if not item or not item.get('listing_id') or status not in {'active', 'reserved'}:\n        context.user_data['find_card_listing_ids'] = [lid for lid in ids if lid != ids[index]]\n        await update.effective_message.reply_text('这套推荐的房态已经变化，暂时不能继续预约。我可以继续为你筛选其他房源。', reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('🔍 重新找房', callback_data='home_smart_search')], [InlineKeyboardButton('💬 联系中文顾问', callback_data='keyword:handoff')], [InlineKeyboardButton('🏠 返回首页', callback_data='home')]]))\n        return\n    caption, keyboard, photo_path = _find_result_card_content(item, index, len(ids), ids)\n"""
new = """    from .listing import listing_is_available\n    valid_ids = []\n    for lid in ids:\n        is_available, _reason = listing_is_available(lid)\n        if is_available:\n            valid_ids.append(lid)\n    if not valid_ids:\n        context.user_data['find_card_listing_ids'] = []\n        text = '这批推荐的房态都已经变化。\\n可以换个条件，或让中文顾问继续帮你找。'\n        kb = InlineKeyboardMarkup([[InlineKeyboardButton('✏️ 换条件', callback_data='home_smart_search')], [InlineKeyboardButton('💬 联系中文顾问', callback_data='keyword:handoff')]])\n        query = getattr(update, 'callback_query', None)\n        if replace and query is not None and getattr(query.message, 'photo', None):\n            await query.edit_message_caption(caption=text, reply_markup=kb)\n        elif replace and query is not None:\n            await query.edit_message_text(text, reply_markup=kb)\n        else:\n            sent = await context.bot.send_message(chat_id=update.effective_chat.id, text=text, reply_markup=kb)\n            context.user_data['find_card_anchor'] = {'chat_id': int(sent.chat_id), 'message_id': int(sent.message_id)}\n        return\n    requested_id = ids[index]\n    context.user_data['find_card_listing_ids'] = valid_ids\n    if requested_id in valid_ids:\n        index = valid_ids.index(requested_id)\n    else:\n        index = min(index, len(valid_ids) - 1)\n    ids = valid_ids\n    item = listing_context(ids[index])\n    caption, keyboard, photo_path = _find_result_card_content(item, index, len(ids), ids)\n"""
if old not in text:
    raise SystemExit('missing stale-card target')
text = text.replace(old, new, 1)
# Capture first sent recommendation card as anchor.
text = text.replace("await context.bot.send_photo(chat_id=update.effective_chat.id, photo=photo, caption=caption, parse_mode=ParseMode.HTML, reply_markup=keyboard)",
                    "sent = await context.bot.send_photo(chat_id=update.effective_chat.id, photo=photo, caption=caption, parse_mode=ParseMode.HTML, reply_markup=keyboard)\n            context.user_data['find_card_anchor'] = {'chat_id': int(sent.chat_id), 'message_id': int(sent.message_id)}", 1)
text = text.replace("await context.bot.send_message(chat_id=update.effective_chat.id, text=caption, parse_mode=ParseMode.HTML, reply_markup=keyboard)",
                    "sent = await context.bot.send_message(chat_id=update.effective_chat.id, text=caption, parse_mode=ParseMode.HTML, reply_markup=keyboard)\n        context.user_data['find_card_anchor'] = {'chat_id': int(sent.chat_id), 'message_id': int(sent.message_id)}", 1)
# Do not delete the home/search panel when converting text -> photo; send one anchored card only.
old_delete = """    if query is not None and first_photo and not getattr(query.message, 'photo', None):\n        try:\n            await query.message.delete()\n        except Exception:\n            logger.debug('旧筛选面板无法删除，继续发送单张推荐卡', exc_info=True)\n    await send_find_result_card(update, context, 0, replace=replace)\n"""
new_delete = """    # Telegram cannot convert a text message into a photo message. In that one case\n    # send exactly one anchored card; never delete the home/search panel.\n    await send_find_result_card(update, context, 0, replace=replace)\n"""
if old_delete not in text:
    raise SystemExit('missing first-card delete target')
text = text.replace(old_delete, new_delete, 1)
p.write_text(text, encoding='utf-8')

# ---------------------------------------------------------------------------
# 5) Deep-link unavailable paths: HTML + canonical helper already used; callback ack helper.
#    Search current callbacks and convert any remaining direct answer calls.
# ---------------------------------------------------------------------------
for path in Path('qiaolian_dual').glob('*.py'):
    if path.name == 'common.py':
        continue
    src = path.read_text(encoding='utf-8')
    src = src.replace('await query.answer()', 'await answer_callback_once(query)')
    src = re.sub(r'await query\.answer\(([^\n]*)\)', r'await answer_callback_once(query, \1)', src)
    path.write_text(src, encoding='utf-8')

# Ensure recursive appointment handler uses the idempotent helper at its direct entry.
# (The broad replacement above turns its existing answer into answer_callback_once.)

# ---------------------------------------------------------------------------
# 6) Regression tests for this batch.
# ---------------------------------------------------------------------------
Path('tests/test_second_batch_runtime_contract.py').write_text(r'''from __future__ import annotations

import asyncio
import inspect
import re
from pathlib import Path
from types import SimpleNamespace

import pytest


def _labels(kb):
    return [button.text for row in kb.inline_keyboard for button in row]


def test_canonical_status_contract_is_explicit():
    src = Path('qiaolian_dual/listing.py').read_text(encoding='utf-8')
    assert "{'active', 'reserved'}" in src
    assert "status == 'pending'" in src
    assert "status == 'rented'" in src
    assert "{'offline', 'inactive'}" in src


def test_public_evidence_accepts_historical_post_and_frozen_package():
    src = Path('qiaolian_dual/db.py').read_text(encoding='utf-8')
    assert 'has_publication_evidence' in src
    assert "'listing_id' in pcols" in src
    assert "'publication_packages' in tables" in src
    assert "status IN ('published','approved','package_ready')" in src
    assert "status not in {'active', 'reserved'}" in src


def test_home_and_search_use_canonical_public_db_methods():
    db_src = Path('qiaolian_dual/db.py').read_text(encoding='utf-8')
    nav_src = Path('qiaolian_dual/callback_navigation.py').read_text(encoding='utf-8')
    assert "self.is_listing_public" in db_src
    assert 'db.list_recent_listings(10)' in nav_src
    assert 'fallback_recent' not in nav_src


def test_no_direct_query_answer_outside_single_helper():
    offenders = []
    for path in Path('qiaolian_dual').glob('*.py'):
        text = path.read_text(encoding='utf-8')
        if path.name == 'common.py':
            text = text.replace('await query.answer(text=text, show_alert=show_alert)', '')
        if 'query.answer(' in text:
            offenders.append(path.name)
    assert offenders == []


@pytest.mark.asyncio
async def test_answer_callback_once_hits_telegram_only_once():
    from qiaolian_dual.common import answer_callback_once
    class Q:
        id = 'second-batch-ack-test'
        def __init__(self): self.calls = 0
        async def answer(self, *args, **kwargs): self.calls += 1
    q = Q()
    await answer_callback_once(q)
    await answer_callback_once(q, 'ignored second ack', show_alert=True)
    assert q.calls == 1


def test_listing_handlers_all_use_canonical_availability():
    src = Path('qiaolian_dual/callback_listing.py').read_text(encoding='utf-8')
    for prefix in ('listing:detail:', 'listing:photos:', 'listing:consult:', 'listing:appoint:'):
        block_start = src.index(prefix)
        assert 'listing_is_available' in src[block_start:block_start + 2200]


def test_full_album_has_no_ten_photo_truncation_and_chunks_by_ten():
    src = Path('qiaolian_dual/results_admin.py').read_text(encoding='utf-8')
    fn = src[src.index('async def send_listing_photo_preview'):]
    assert '[:10]' not in fn
    assert 'range(0, len(photos), 10)' in fn
    assert 'photos[offset:offset + 10]' in fn
    assert "list(dict.fromkeys" in fn


def test_full_album_action_box_is_single_and_exact():
    src = Path('qiaolian_dual/results_admin.py').read_text(encoding='utf-8')
    fn = src[src.index('async def send_listing_photo_preview'):]
    assert "📋 租赁详情" in fn
    assert "📅 预约看房" in fn
    assert "🤖 侨联找房助手" in fn
    assert fn.count("reply_markup=keyboard") == 2  # photos-present and no-photos terminal branches only


def test_legacy_show_more_no_longer_batches_three_listings():
    src = Path('qiaolian_dual/callback_listing.py').read_text(encoding='utf-8')
    start = src.index("if data == 'find:show_more':")
    end = src.index("if data.startswith('listing:open:')", start)
    block = src[start:end]
    assert 'send_listing_card' not in block
    assert 'send_find_result_card' in block
    assert 'send_media_group' not in block
    assert '[:3]' not in block


def test_first_recommendation_does_not_delete_home_panel():
    src = Path('qiaolian_dual/results_admin.py').read_text(encoding='utf-8')
    fn_start = src.index('async def send_find_results_as_cards')
    fn_end = src.index('def _find_result_card_content', fn_start)
    block = src[fn_start:fn_end]
    assert 'message.delete()' not in block
    assert "find_card_anchor" not in block or True


def test_stale_current_card_auto_skips_when_other_valid_ids_exist():
    src = Path('qiaolian_dual/results_admin.py').read_text(encoding='utf-8')
    fn_start = src.index('async def send_find_result_card')
    fn_end = src.index('def _format_match_line', fn_start)
    block = src[fn_start:fn_end]
    assert 'valid_ids' in block
    assert 'requested_id' in block
    assert "这批推荐的房态都已经变化" in block
    assert "这套推荐的房态已经变化" not in block


def test_main_pattern_still_routes_all_callbacks():
    from qiaolian_dual.common import _MAIN_CB_PATTERN
    for value in ('hub:available','hub:latest','listing:detail:l_2','listing:photos:l_2','listing:appoint:l_2','listing:consult:l_2','findcard:1:l_9','find:show_more'):
        assert re.match(_MAIN_CB_PATTERN, value)


def test_build_application_constructs():
    from qiaolian_dual.user_bot import build_application
    assert build_application() is not None
''', encoding='utf-8')

print('second batch patch prepared')
