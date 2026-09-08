from __future__ import annotations

import sqlite3

import pytest

from qiaolian_v3.admin_bot import ADMIN_ROUTES, home_screen
from qiaolian_v3.admin_bot.broadcast import BroadcastBlocked, BroadcastService
from qiaolian_v3.admin_bot.manual_intake import AdminPublicationFields
from qiaolian_v3.admin_bot.router import AdminBotHandler, AdminBotRouter, AdminUpdate
from qiaolian_v3.db.migration_runner import MigrationRunner
from qiaolian_v3.ingest.manual_intake import ManualIntakeService
from qiaolian_v3.parser.canonical import canonicalize_source
from tests.v3.fakes import FakeTelegramGateway


def db() -> sqlite3.Connection:
    conn = sqlite3.connect(':memory:')
    conn.row_factory = sqlite3.Row
    MigrationRunner(conn).migrate()
    return conn


def images() -> list[bytes]:
    return [f'admin-image-{n}'.encode() for n in range(4)]


def test_admin_home_has_exact_seven_reachable_entries():
    screen = home_screen()
    assert screen['title'] == '🏠 侨联发布后台'
    assert set(ADMIN_ROUTES) == {
        'manual_intake', 'listing_management', 'review', 'collector_management',
        'broadcast', 'publication_history', 'settings',
    }
    assert len(ADMIN_ROUTES) == 7


def test_admin_handler_dispatches_home_and_all_seven_callbacks():
    conn = db()
    fake = FakeTelegramGateway()
    handler = AdminBotHandler(AdminBotRouter(conn, gateway=fake, dry_run=True))
    assert handler.handle(AdminUpdate('home')).route == 'home'
    for callback in ADMIN_ROUTES:
        result = handler.handle(AdminUpdate(callback))
        assert result.route == callback
    assert fake.write_count == 0


def test_admin_import_uses_same_parser_never_auto_publishes_and_creates_review():
    conn = db()
    assert ManualIntakeService.parser is canonicalize_source
    fake = FakeTelegramGateway()
    handler = AdminBotHandler(AdminBotRouter(conn, gateway=fake, dry_run=True))
    preview_result = handler.handle(AdminUpdate('manual_intake', {
        'action':'preview',
        'text':'区域：BKK1\n公寓出租\n1房1卫\n租金：$800/月',
        'images':images(),
        'operator_id':'admin-1',
    }))
    preview = preview_result.payload['preview']
    assert preview_result.payload['action'] == 'preview'
    assert preview.deal_type == 'rent'
    assert preview.quality_decision == 'NEEDS_REVIEW'
    assert 'admin_import_requires_review' in preview.blocking_reasons
    assert fake.write_count == 0
    assert conn.execute('SELECT COUNT(*) FROM v3_publication_packages').fetchone()[0] == 0
    review = conn.execute('SELECT * FROM review_items WHERE id=?', (preview.review_item_id,)).fetchone()
    assert review['review_type'] == 'admin_import' and review['status'] == 'open'

    fields = AdminPublicationFields(
        target_channel_id='-100123', cover_path='/tmp/admin-cover.jpg',
        gallery=('1.jpg', '2.jpg', '3.jpg', '4.jpg'), caption_html='ok', bot_username='QiaoLianBot',
    )
    confirm_result = handler.handle(AdminUpdate('manual_intake', {
        'action':'confirm', 'preview':preview, 'operator_id':'admin-1', 'fields':fields,
    }))
    result = confirm_result.payload['result']
    assert confirm_result.payload['action'] == 'confirm'
    assert result['explicit_confirm'] is True
    assert result['publish_result']['writes'] == 0
    assert fake.write_count == 0
    assert conn.execute('SELECT status FROM review_items WHERE id=?', (preview.review_item_id,)).fetchone()[0] == 'approved'
    assert conn.execute('SELECT status FROM v3_publication_packages').fetchone()[0] == 'frozen'


def test_preview_is_not_approve_and_confirm_is_explicit():
    conn = db()
    fake = FakeTelegramGateway()
    handler = AdminBotHandler(AdminBotRouter(conn, gateway=fake, dry_run=True))
    preview = handler.handle(AdminUpdate('manual_intake', {
        'action':'preview', 'text':'区域：BKK1\n公寓出租\n1房1卫\n租金：$800/月',
        'images':images(), 'operator_id':'admin-2',
    })).payload['preview']
    assert preview.confirmed is False
    assert conn.execute('SELECT COUNT(*) FROM v3_publication_packages').fetchone()[0] == 0
    assert conn.execute('SELECT status FROM review_items WHERE id=?', (preview.review_item_id,)).fetchone()[0] == 'open'
    assert fake.write_count == 0


def test_sale_import_is_store_only_and_zero_telegram_writes():
    conn = db()
    fake = FakeTelegramGateway()
    handler = AdminBotHandler(AdminBotRouter(conn, gateway=fake, dry_run=True))
    preview = handler.handle(AdminUpdate('manual_intake', {
        'action':'preview',
        'text':'区域：BKK1\n商铺出售\n售价：$90,000\n面积：120㎡',
        'images':images(), 'operator_id':'admin-sale',
    })).payload['preview']
    assert preview.deal_type == 'sale'
    assert preview.publication_policy == 'store_only'
    offer = conn.execute('SELECT * FROM listing_offers WHERE id=?', (preview.offer_id,)).fetchone()
    assert offer['offer_type'] == 'sale' and offer['publication_policy'] == 'store_only'
    assert fake.write_count == 0
    assert conn.execute('SELECT COUNT(*) FROM v3_publication_packages').fetchone()[0] == 0


def test_unknown_and_conflict_create_readable_review_items_without_listing():
    conn = db()
    fake = FakeTelegramGateway()
    handler = AdminBotHandler(AdminBotRouter(conn, gateway=fake, dry_run=True))
    unknown = handler.handle(AdminUpdate('manual_intake', {
        'action':'preview','text':'区域：BKK1\n公寓\n1房1卫','images':images(),'operator_id':'admin-u',
    })).payload['preview']
    conflict = handler.handle(AdminUpdate('manual_intake', {
        'action':'preview',
        'text':'区域：BKK1\n公寓可出租，也可出售\n2房2卫\n租金：$680/月\n售价：$100,000',
        'images':images(),'operator_id':'admin-c',
    })).payload['preview']
    assert unknown.deal_type == conflict.deal_type == 'unknown'
    assert unknown.listing_id is None and conflict.listing_id is None
    ids = {row['id'] for row in conn.execute("SELECT id FROM review_items WHERE status='open'").fetchall()}
    assert {unknown.review_item_id, conflict.review_item_id} <= ids
    assert fake.write_count == 0


def test_broadcast_handler_preview_confirm_send_are_separate_with_zero_real_writes():
    class Gateway:
        def __init__(self): self.calls = 0
        def broadcast(self, **kwargs): self.calls += 1; return {'ok': True}
    gateway = Gateway()
    handler = AdminBotHandler(AdminBotRouter(db(), gateway=gateway, dry_run=True))
    preview_result = handler.handle(AdminUpdate('broadcast', {'action':'preview','text':'测试广播'}))
    preview = preview_result.payload['preview']
    assert preview_result.payload['action'] == 'preview' and gateway.calls == 0
    with pytest.raises(BroadcastBlocked, match='explicit_confirm_required'):
        BroadcastService(gateway=gateway, dry_run=True).send(preview)
    sent = handler.handle(AdminUpdate('broadcast', {'action':'confirm','preview':preview}))
    assert sent.payload['action'] == 'send'
    assert sent.payload['result']['writes'] == 0 and gateway.calls == 0
