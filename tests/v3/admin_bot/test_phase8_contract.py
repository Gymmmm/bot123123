from __future__ import annotations

import sqlite3

import pytest

from qiaolian_v3.admin_bot import ADMIN_ROUTES, home_screen
from qiaolian_v3.admin_bot.broadcast import BroadcastBlocked, BroadcastService
from qiaolian_v3.admin_bot.manual_intake import AdminManualIntakeController, AdminPublicationFields
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


def test_admin_import_uses_same_parser_never_auto_publishes_and_creates_review():
    conn = db()
    assert ManualIntakeService.parser is canonicalize_source
    controller = AdminManualIntakeController(conn)
    fake = FakeTelegramGateway()
    preview = controller.preview(
        text='区域：BKK1\n公寓出租\n1房1卫\n租金：$800/月',
        images=images(),
        operator_id='admin-1',
    )
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
    result = controller.confirm(
        preview=preview, operator_id='admin-1', fields=fields, gateway=fake, dry_run=True,
    )
    assert result['explicit_confirm'] is True
    assert result['publish_result']['writes'] == 0
    assert fake.write_count == 0
    assert conn.execute('SELECT status FROM review_items WHERE id=?', (preview.review_item_id,)).fetchone()[0] == 'approved'
    assert conn.execute('SELECT status FROM v3_publication_packages').fetchone()[0] == 'frozen'


def test_preview_is_not_approve_and_confirm_is_explicit():
    conn = db()
    controller = AdminManualIntakeController(conn)
    preview = controller.preview(
        text='区域：BKK1\n公寓出租\n1房1卫\n租金：$800/月', images=images(), operator_id='admin-2'
    )
    assert preview.confirmed is False
    assert conn.execute('SELECT COUNT(*) FROM v3_publication_packages').fetchone()[0] == 0
    assert conn.execute('SELECT status FROM review_items WHERE id=?', (preview.review_item_id,)).fetchone()[0] == 'open'


def test_sale_import_is_store_only_and_zero_telegram_writes():
    conn = db()
    controller = AdminManualIntakeController(conn)
    fake = FakeTelegramGateway()
    preview = controller.preview(
        text='区域：BKK1\n商铺出售\n售价：$90,000\n面积：120㎡', images=images(), operator_id='admin-sale'
    )
    assert preview.deal_type == 'sale'
    assert preview.publication_policy == 'store_only'
    offer = conn.execute('SELECT * FROM listing_offers WHERE id=?', (preview.offer_id,)).fetchone()
    assert offer['offer_type'] == 'sale' and offer['publication_policy'] == 'store_only'
    assert fake.write_count == 0
    assert conn.execute('SELECT COUNT(*) FROM v3_publication_packages').fetchone()[0] == 0


def test_unknown_and_conflict_create_readable_review_items_without_listing():
    conn = db()
    controller = AdminManualIntakeController(conn)
    unknown = controller.preview(text='区域：BKK1\n公寓\n1房1卫', images=images(), operator_id='admin-u')
    conflict = controller.preview(
        text='区域：BKK1\n公寓可出租，也可出售\n2房2卫\n租金：$680/月\n售价：$100,000',
        images=images(), operator_id='admin-c',
    )
    assert unknown.deal_type == conflict.deal_type == 'unknown'
    assert unknown.listing_id is None and conflict.listing_id is None
    ids = {row['id'] for row in conn.execute("SELECT id FROM review_items WHERE status='open'").fetchall()}
    assert {unknown.review_item_id, conflict.review_item_id} <= ids


def test_broadcast_preview_and_send_are_separate_and_preview_has_zero_writes():
    class Gateway:
        def __init__(self): self.calls = 0
        def broadcast(self, **kwargs): self.calls += 1; return {'ok': True}
    gateway = Gateway()
    service = BroadcastService(gateway=gateway, dry_run=True)
    preview = service.preview(text='测试广播')
    assert gateway.calls == 0
    with pytest.raises(BroadcastBlocked, match='explicit_confirm_required'):
        service.send(preview)
    result = service.send(preview, confirm=True)
    assert result['writes'] == 0 and gateway.calls == 0
