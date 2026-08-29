from pathlib import Path

# Make callback ack helper idempotent without leaking object-id reuse across callbacks.
p = Path('qiaolian_dual/common.py')
text = p.read_text(encoding='utf-8')
old = '''# CallbackQuery can only be answered once. All handlers use this idempotent helper.
_CALLBACK_ANSWERED_IDS: set[str] = set()
_CALLBACK_ANSWERED_ORDER: list[str] = []

async def answer_callback_once(query, text: str | None=None, *, show_alert: bool=False) -> bool:
    query_id = str(getattr(query, 'id', '') or '')
    if query_id and query_id in _CALLBACK_ANSWERED_IDS:
        return False
    await query.answer(text=text, show_alert=show_alert)
    if query_id:
        _CALLBACK_ANSWERED_IDS.add(query_id)
        _CALLBACK_ANSWERED_ORDER.append(query_id)
        if len(_CALLBACK_ANSWERED_ORDER) > 2048:
            stale = _CALLBACK_ANSWERED_ORDER.pop(0)
            _CALLBACK_ANSWERED_IDS.discard(stale)
    return True
'''
new = '''# CallbackQuery can only be answered once. All handlers use this idempotent helper.
# Real Telegram callbacks have a stable query.id. A weak object set is only for local
# test/fallback query objects that do not expose an id, avoiding Python id() reuse.
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
'''
if old not in text:
    raise SystemExit('missing callback helper target')
text = text.replace(old, new, 1)
p.write_text(text, encoding='utf-8')

# Existing route tests are route-contract tests, not publication-evidence tests. Keep them
# focused by explicitly marking their fixture listing as canonically available.
p = Path('tests/test_callback_route_repair.py')
text = p.read_text(encoding='utf-8')
needle = "    monkeypatch.setattr(listing_cb.db, 'get_listing', lambda lid: {'listing_id': lid, 'status': 'active'})\n"
text = text.replace(needle, needle + "    monkeypatch.setattr(listing_mod, 'listing_is_available', lambda lid: (True, 'active'))\n", 1)

needle = "async def test_listing_photos_calls_complete_album_handler(monkeypatch):\n    import qiaolian_dual.results_admin as results\n"
replacement = needle + "    import qiaolian_dual.listing as listing_mod\n    monkeypatch.setattr(listing_mod, 'listing_is_available', lambda lid: (True, 'active'))\n"
if needle not in text:
    raise SystemExit('missing photos route test target')
text = text.replace(needle, replacement, 1)

needle = "async def test_listing_consult_preserves_listing_context(monkeypatch):\n    import qiaolian_dual.flows as flows\n"
replacement = needle + "    import qiaolian_dual.listing as listing_mod\n    monkeypatch.setattr(listing_mod, 'listing_is_available', lambda lid: (True, 'active'))\n"
if needle not in text:
    raise SystemExit('missing consult route test target')
text = text.replace(needle, replacement, 1)
p.write_text(text, encoding='utf-8')

# Old static assertion now checks the canonical helper rather than duplicated status logic.
p = Path('tests/test_user_ui_cleanup.py')
text = p.read_text(encoding='utf-8')
old = '''def test_find_card_rechecks_listing_status_before_rendering():\n    source = Path('qiaolian_dual/results_admin.py').read_text(encoding='utf-8')\n    assert "status not in {'active', 'reserved'}" in source\n    assert '房态已经变化' in source\n'''
new = '''def test_find_card_rechecks_listing_status_before_rendering():\n    source = Path('qiaolian_dual/results_admin.py').read_text(encoding='utf-8')\n    assert 'listing_is_available' in source\n    assert 'valid_ids' in source\n    assert '房态都已经变化' in source\n'''
if old not in text:
    raise SystemExit('missing old find-card status test')
text = text.replace(old, new, 1)
p.write_text(text, encoding='utf-8')

# Add a real >10 photo runtime test to the second-batch suite.
p = Path('tests/test_second_batch_runtime_contract.py')
text = p.read_text(encoding='utf-8')
insert = r'''

@pytest.mark.asyncio
async def test_full_album_12_photos_sends_10_plus_2_and_one_action_box(monkeypatch, tmp_path):
    import qiaolian_dual.listing as listing_mod
    from qiaolian_dual.results_admin import send_listing_photo_preview

    photos = []
    for index in range(12):
        path = tmp_path / f'image_{index:02d}.jpg'
        path.write_bytes(b'not-a-real-jpeg-but-bytes-are-enough-for-bot-mock')
        photos.append(str(path))
    monkeypatch.setattr(listing_mod, 'listing_context', lambda lid: {
        'listing_id': lid,
        'project': 'BKK1',
        'layout': '2房',
        'property_type': '公寓',
        'media_files': photos,
    })

    class Bot:
        def __init__(self):
            self.groups = []
            self.messages = []
            self.photos = []
        async def send_media_group(self, **kwargs):
            self.groups.append(kwargs['media'])
        async def send_message(self, **kwargs):
            self.messages.append(kwargs)
        async def send_photo(self, **kwargs):
            self.photos.append(kwargs)

    bot = Bot()
    await send_listing_photo_preview(bot, 123, 'l_2')
    assert [len(group) for group in bot.groups] == [10, 2]
    assert len(bot.messages) == 1
    assert len(bot.photos) == 0
    labels = [button.text for row in bot.messages[0]['reply_markup'].inline_keyboard for button in row]
    assert labels == ['📋 租赁详情', '📅 预约看房', '🤖 侨联找房助手']
'''
if 'test_full_album_12_photos_sends_10_plus_2_and_one_action_box' not in text:
    text += insert
p.write_text(text, encoding='utf-8')

print('second batch regression fixes prepared')
