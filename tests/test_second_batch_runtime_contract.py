from __future__ import annotations

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


def _create_listing(db, listing_id: str, status: str) -> None:
    db.create_listing({
        'listing_id': listing_id,
        'title': listing_id,
        'property_type': '公寓',
        'area': 'BKK1',
        'community': '测试公寓',
        'price': 800,
        'status': status,
        'created_at': '2026-08-29 00:00:00',
        'updated_at': '2026-08-29 00:00:00',
    })


def test_reserved_historical_post_stays_public_without_current_draft_row(tmp_path):
    from qiaolian_dual.db import Database
    db = Database(tmp_path / 'historical-post.db')
    _create_listing(db, 'l_history', 'reserved')
    with db.connect() as conn:
        conn.execute('CREATE TABLE drafts (draft_id TEXT, listing_id TEXT, review_status TEXT)')
        conn.execute('CREATE TABLE posts (draft_id TEXT, listing_id TEXT, platform TEXT, publish_status TEXT)')
        conn.execute(
            "INSERT INTO posts (draft_id, listing_id, platform, publish_status) VALUES ('old_draft','l_history','telegram','published')"
        )
    assert db.has_publication_evidence('l_history') is True
    assert db.is_listing_public('l_history') is True
    assert [item['listing_id'] for item in db.list_recent_listings(10)] == ['l_history']


def test_active_frozen_package_stays_public_without_draft_or_post_row(tmp_path):
    from qiaolian_dual.db import Database
    db = Database(tmp_path / 'frozen-package.db')
    _create_listing(db, 'l_package', 'active')
    with db.connect() as conn:
        conn.execute('CREATE TABLE publication_packages (property_id TEXT, status TEXT)')
        conn.execute("INSERT INTO publication_packages (property_id, status) VALUES ('l_package','published')")
    assert db.has_publication_evidence('l_package') is True
    assert db.is_listing_public('l_package') is True
    assert [item['listing_id'] for item in db.list_recent_listings(10)] == ['l_package']


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
    for prefix in ('listing:photos:', 'listing:appoint:'):
        block_start = src.index(prefix)
        assert 'listing_is_available' in src[block_start:block_start + 2200]
    for prefix in ('listing:detail:', 'listing:consult:'):
        block_start = src.index(prefix)
        assert 'listing_action_allowed' in src[block_start:block_start + 2200]


def test_pending_allows_detail_and_consult_but_not_album_or_appointment(monkeypatch):
    import qiaolian_dual.listing as listing_mod

    monkeypatch.setattr(listing_mod.db, 'get_listing', lambda _lid: {'listing_id': 'l_pending', 'status': 'pending'})
    assert listing_mod.listing_action_allowed('l_pending', 'detail') == (True, 'pending')
    assert listing_mod.listing_action_allowed('l_pending', 'consult') == (True, 'pending')
    assert listing_mod.listing_action_allowed('l_pending', 'photos') == (False, 'pending')
    assert listing_mod.listing_action_allowed('l_pending', 'appoint') == (False, 'pending')


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


@pytest.mark.asyncio
async def test_first_recommendation_resolves_listing_context_at_runtime(monkeypatch):
    """Regression for the real hub:available click path (NameError in production)."""
    from types import SimpleNamespace
    import qiaolian_dual.listing as listing_mod
    import qiaolian_dual.results_admin as results

    monkeypatch.setattr(listing_mod, 'listing_context', lambda lid: {
        'listing_id': lid,
        'media_files': [],
        'media_file_id': '',
    })
    calls = []

    async def fake_send(*args, **kwargs):
        calls.append((args, kwargs))

    monkeypatch.setattr(results, 'send_find_result_card', fake_send)
    update = SimpleNamespace(callback_query=SimpleNamespace(message=SimpleNamespace(photo=None)))
    context = SimpleNamespace(user_data={})

    await results.send_find_results_as_cards(
        update,
        context,
        [{'listing_id': 'l_72'}],
        'strict',
    )

    assert context.user_data['find_card_listing_ids'] == ['l_72']
    assert len(calls) == 1


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
    from qiaolian_dual.app import build_application
    assert build_application(token='123456:TEST_CALLBACK_ROUTE_TOKEN') is not None


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


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('photo_count', 'expected_groups', 'expected_single_photos'),
    [
        (0, [], 0),
        (1, [], 1),
        (4, [4], 0),
        (10, [10], 0),
        (11, [10], 1),
        (23, [10, 10, 3], 0),
    ],
)
async def test_full_album_required_photo_counts(
    monkeypatch, tmp_path, photo_count, expected_groups, expected_single_photos
):
    import qiaolian_dual.listing as listing_mod
    from qiaolian_dual.results_admin import send_listing_photo_preview

    photos = []
    for index in range(photo_count):
        path = tmp_path / f'album_{index:02d}.jpg'
        path.write_bytes(f'photo-{index}'.encode())
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
    await send_listing_photo_preview(bot, 123, 'l_required_counts')
    assert [len(group) for group in bot.groups] == expected_groups
    assert len(bot.photos) == expected_single_photos
    assert len(bot.messages) == 1
    terminal = bot.messages[0]
    labels = [button.text for row in terminal['reply_markup'].inline_keyboard for button in row]
    assert labels == ['📋 租赁详情', '📅 预约看房', '🤖 侨联找房助手']
    if photo_count == 0:
        assert '没有更多可用实拍' in terminal['text']
