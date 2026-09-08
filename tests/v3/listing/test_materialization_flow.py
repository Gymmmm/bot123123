from __future__ import annotations

from qiaolian_v3.ingest.source_service import SourceIngestService
from qiaolian_v3.listing.materializer import CanonicalListingMaterializer
from qiaolian_v3.media.source_media import SourceMedia
from qiaolian_v3.parser.service import CanonicalParserService
from tests.v3.db._helpers import migrated_connection


def _source(conn, text: str, external_post_id: str = '100'):
    result = SourceIngestService(conn).ingest_telegram(
        source_name='fixture', source_external_identity='-100123', external_post_id=external_post_id,
        raw_text=text, media=[SourceMedia.from_bytes(b'photo', sort_order=0, message_id=1)],
        source_created_at='2026-09-01T10:00:00+00:00', fetched_at='2026-09-01T10:01:00+00:00',
    )
    return result


def test_source_to_canonical_to_listing_to_rent_offer_without_drafts_or_package():
    conn = migrated_connection()
    source = _source(conn, '区域：BKK1\n公寓出租\n2房2卫\n租金：$800/月\n押1付1')
    parsed = CanonicalParserService(conn).parse_revision(source.revision_id)
    materialized = CanonicalListingMaterializer(conn).materialize(parsed.canonical_record_id)

    listing = conn.execute('SELECT * FROM v3_listings WHERE id=?', (materialized.listing_id,)).fetchone()
    offer = conn.execute('SELECT * FROM listing_offers WHERE id=?', (materialized.offer_ids[0],)).fetchone()
    assert listing['current_canonical_record_id'] == parsed.canonical_record_id
    assert listing['public_listing_id'].startswith('QL-')
    assert offer['offer_type'] == 'rent'
    assert offer['monthly_rent_usd'] == 800
    assert offer['sale_price_usd'] is None
    assert conn.execute('SELECT COUNT(*) FROM v3_publication_packages').fetchone()[0] == 0
    assert conn.execute('SELECT COUNT(*) FROM drafts').fetchone()[0] == 0


def test_sale_materializes_directly_as_store_only_offer_not_skipped_non_rental():
    conn = migrated_connection()
    source = _source(conn, '区域：BKK1\n商铺出售\n售价：$90,000\n面积：120㎡', 'sale-100')
    parsed = CanonicalParserService(conn).parse_revision(source.revision_id)
    materialized = CanonicalListingMaterializer(conn).materialize(parsed.canonical_record_id)

    offer = conn.execute('SELECT * FROM listing_offers WHERE id=?', (materialized.offer_ids[0],)).fetchone()
    assert offer['offer_type'] == 'sale'
    assert offer['sale_price_usd'] == 90000
    assert offer['monthly_rent_usd'] is None
    assert offer['publication_policy'] == 'store_only'
    assert offer['publish_block_reason'] == 'sale_not_enabled_for_rent_channel'
    assert conn.execute('SELECT COUNT(*) FROM v3_publication_packages').fetchone()[0] == 0


def test_ambiguous_rent_and_sale_can_materialize_both_explicit_offers_without_fake_deal_type():
    conn = migrated_connection()
    source = _source(conn, '区域：BKK1\n公寓可出租，也可出售\n2房2卫\n租金：$680/月\n售价：$100,000', 'mixed-evidence')
    parsed = CanonicalParserService(conn).parse_revision(source.revision_id)
    assert parsed.deal_type == 'unknown'
    materialized = CanonicalListingMaterializer(conn).materialize(parsed.canonical_record_id)
    offers = conn.execute('SELECT * FROM listing_offers WHERE listing_id=? ORDER BY offer_type', (materialized.listing_id,)).fetchall()
    assert [(r['offer_type'], r['monthly_rent_usd'], r['sale_price_usd']) for r in offers] == [
        ('rent', 680, None), ('sale', None, 100000)
    ]
    assert next(r for r in offers if r['offer_type'] == 'sale')['publication_policy'] == 'store_only'
