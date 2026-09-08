from __future__ import annotations

import pytest

from qiaolian_v3.ingest.source_service import SourceIngestService
from qiaolian_v3.listing.materializer import CanonicalListingMaterializer
from qiaolian_v3.media.source_media import SourceMedia
from qiaolian_v3.parser.service import CanonicalParserService
from tests.v3.db._helpers import migrated_connection


def _source(conn, text: str, external_post_id: str = '100'):
    return SourceIngestService(conn).ingest_telegram(
        source_name='fixture', source_external_identity='-100123', external_post_id=external_post_id,
        raw_text=text, media=[SourceMedia.from_bytes(b'photo', sort_order=0, message_id=1)],
        source_created_at='2026-09-01T10:00:00+00:00', fetched_at='2026-09-01T10:01:00+00:00',
    )


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
    assert conn.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='drafts'").fetchone()[0] == 0


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


def test_unresolved_rent_sale_unknown_stops_before_listing_and_offer_materialization():
    conn = migrated_connection()
    source = _source(conn, '区域：BKK1\n公寓可出租，也可出售\n2房2卫\n租金：$680/月\n售价：$100,000', 'mixed-evidence')
    parsed = CanonicalParserService(conn).parse_revision(source.revision_id)
    assert parsed.deal_type == 'unknown'

    with pytest.raises(ValueError, match='unresolved_deal_type'):
        CanonicalListingMaterializer(conn).materialize(parsed.canonical_record_id)

    assert conn.execute('SELECT COUNT(*) FROM v3_listings').fetchone()[0] == 0
    assert conn.execute('SELECT COUNT(*) FROM listing_offers').fetchone()[0] == 0


def test_different_source_posts_with_same_weak_property_facts_remain_distinct_listings():
    conn = migrated_connection()
    text = '区域：BKK1\n项目：同一项目\n公寓出租\n2房2卫\n面积：88㎡\n楼层：19楼\n租金：$800/月'
    first = _source(conn, text, 'source-a')
    second = _source(conn, text, 'source-b')

    first_parsed = CanonicalParserService(conn).parse_revision(first.revision_id)
    second_parsed = CanonicalParserService(conn).parse_revision(second.revision_id)
    first_listing = CanonicalListingMaterializer(conn).materialize(first_parsed.canonical_record_id)
    second_listing = CanonicalListingMaterializer(conn).materialize(second_parsed.canonical_record_id)

    assert first_listing.listing_id != second_listing.listing_id
    assert first_listing.public_listing_id != second_listing.public_listing_id


def test_same_source_post_changed_revision_keeps_same_listing_id():
    conn = migrated_connection()
    first = _source(conn, '区域：BKK1\n公寓出租\n2房2卫\n租金：$800/月', 'stable-source')
    first_parsed = CanonicalParserService(conn).parse_revision(first.revision_id)
    first_listing = CanonicalListingMaterializer(conn).materialize(first_parsed.canonical_record_id)

    second = _source(conn, '区域：BKK1\n公寓出租\n2房2卫\n租金：$850/月', 'stable-source')
    second_parsed = CanonicalParserService(conn).parse_revision(second.revision_id)
    second_listing = CanonicalListingMaterializer(conn).materialize(second_parsed.canonical_record_id)

    assert second.revision_id != first.revision_id
    assert second_listing.listing_id == first_listing.listing_id
    assert second_listing.public_listing_id == first_listing.public_listing_id
    current = conn.execute(
        "SELECT * FROM listing_offers WHERE listing_id=? AND offer_type='rent' AND is_current=1",
        (first_listing.listing_id,),
    ).fetchone()
    assert current['monthly_rent_usd'] == 850
