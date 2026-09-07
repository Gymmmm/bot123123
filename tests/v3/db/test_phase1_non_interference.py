from __future__ import annotations

from pathlib import Path

from qiaolian_v3.db.repositories import CanonicalRecordRepository, ListingOfferRepository, SourceRepository
from tests.v3.db._helpers import insert_source, migrated_connection


ROOT = Path(__file__).resolve().parents[3]


def test_phase1_does_not_wire_v3_into_production_runtime_modules():
    for rel in (
        'ai_parser.py',
        'collector_bot.py',
        'publication_package.py',
        'publication_delivery.py',
        'autopilot_publish_bot.py',
        'qiaolian_dual/user_bot.py',
    ):
        text = (ROOT / rel).read_text(encoding='utf-8')
        assert 'qiaolian_v3' not in text
        assert 'v3_shadow_store' not in text


def test_v3_source_repository_never_requires_or_writes_legacy_source_posts():
    conn = migrated_connection()
    legacy_id = insert_source(conn, external_id='legacy-1')
    before = [tuple(row) for row in conn.execute(
        'SELECT id,source_type,source_name,source_post_id,raw_text,parse_status FROM source_posts ORDER BY id'
    )]

    sources = SourceRepository(conn)
    source_post_id = sources.register_source_post(
        source_mode='collector', source_type='telegram_channel',
        source_name='v3-only', external_post_id='v3-1',
    )
    sources.append_revision(
        source_post_id=source_post_id, source_content_hash='v3-hash', raw_text='v3 evidence'
    )

    after = [tuple(row) for row in conn.execute(
        'SELECT id,source_type,source_name,source_post_id,raw_text,parse_status FROM source_posts ORDER BY id'
    )]
    assert after == before
    assert conn.execute('SELECT COUNT(*) FROM source_posts').fetchone()[0] == 1
    assert conn.execute('SELECT id FROM source_posts WHERE id=?', (legacy_id,)).fetchone() is not None


def test_v3_repository_activity_never_writes_legacy_listings():
    conn = migrated_connection()
    conn.execute(
        '''INSERT INTO listings(
           listing_id,title,property_type,area,community,price,created_at,updated_at
           ) VALUES ('LEGACY-1','legacy','公寓','area','community',800,'2026-01-01','2026-01-01')'''
    )
    before = [tuple(row) for row in conn.execute('SELECT listing_id,price,status FROM listings ORDER BY listing_id')]

    sources = SourceRepository(conn)
    source_post_id = sources.register_source_post(
        source_mode='collector', source_type='telegram_channel',
        source_name='fixture', external_post_id='100',
    )
    revision = sources.append_revision(
        source_post_id=source_post_id, source_content_hash='h1', raw_text='sale evidence'
    )
    canonical = CanonicalRecordRepository(conn).append(
        source_post_id=source_post_id, source_post_revision_id=revision.id,
        schema_version='v3', parser_revision='r1', facts={'deal_type': 'sale'},
        facts_hash='f1', deal_type='sale', processing_status='STORED',
    )
    offers = ListingOfferRepository(conn)
    listing_id = offers.create_listing(
        property_identity_key='semantic-sale-1', current_canonical_record_id=canonical.id,
        property_type='apartment', listing_status='active',
    )
    offers.link_source(listing_id=listing_id, source_post_id=source_post_id)
    offers.append_offer(
        listing_id=listing_id, canonical_record_id=canonical.id,
        offer_type='sale', sale_price_usd=150000,
        publication_policy='store_only', publish_block_reason='sale_not_enabled_for_rent_channel',
    )

    after = [tuple(row) for row in conn.execute('SELECT listing_id,price,status FROM listings ORDER BY listing_id')]
    assert after == before
    assert conn.execute("SELECT COUNT(*) FROM listings WHERE listing_id LIKE 'V3SRC_%'").fetchone()[0] == 0
