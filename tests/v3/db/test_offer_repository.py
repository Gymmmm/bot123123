from __future__ import annotations

import sqlite3

import pytest

from qiaolian_v3.db.repositories import CanonicalRecordRepository, ListingOfferRepository, SourceRepository
from tests.v3.db._helpers import insert_source, migrated_connection


def _canonical(conn, deal_type: str = 'rent'):
    legacy_id = insert_source(conn)
    sources = SourceRepository(conn)
    identity_id = sources.register_identity(
        legacy_source_post_id=legacy_id,
        source_type='telegram_channel', source_name='fixture', external_post_id='100',
    )
    revision = sources.append_revision(source_identity_id=identity_id, content_hash='h1', raw_text='evidence')
    canonical = CanonicalRecordRepository(conn).append(
        source_identity_id=identity_id, source_post_revision_id=revision.id,
        schema_version='v3', parser_revision='r1', facts={'deal_type': deal_type},
        facts_hash=f'f-{deal_type}', deal_type=deal_type,
    )
    return identity_id, canonical.id


def test_sale_is_db_forced_store_only():
    conn = migrated_connection()
    identity_id, canonical_id = _canonical(conn, 'sale')
    repo = ListingOfferRepository(conn)
    listing_id = repo.create_listing(semantic_key='listing-1')
    repo.link_source(listing_id=listing_id, source_identity_id=identity_id)
    offer = repo.append_offer(
        listing_id=listing_id, canonical_record_id=canonical_id,
        offer_type='sale', sale_price_usd=150000,
        publication_policy='store_only', publish_block_reason='sale_not_enabled_for_rent_channel',
    )
    assert offer.publication_policy == 'store_only'
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            '''INSERT INTO listing_offers(
               listing_id,canonical_record_id,offer_type,sale_price_usd,publication_policy,is_current
               ) VALUES (?,?,?,?,?,0)''',
            (listing_id, canonical_id, 'sale', 160000, 'telegram_rent'),
        )


def test_rent_and_sale_price_constraints_are_db_enforced():
    conn = migrated_connection()
    _, canonical_id = _canonical(conn, 'rent')
    listing_id = ListingOfferRepository(conn).create_listing(semantic_key='listing-1')
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            '''INSERT INTO listing_offers(
               listing_id,canonical_record_id,offer_type,monthly_rent_usd,sale_price_usd
               ) VALUES (?,?,?,?,?)''',
            (listing_id, canonical_id, 'rent', None, None),
        )
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            '''INSERT INTO listing_offers(
               listing_id,canonical_record_id,offer_type,monthly_rent_usd,sale_price_usd
               ) VALUES (?,?,?,?,?)''',
            (listing_id, canonical_id, 'rent', 800, 100000),
        )


def test_offer_versions_supersede_and_keep_one_current_per_type():
    conn = migrated_connection()
    identity_id, canonical_id = _canonical(conn, 'rent')
    repo = ListingOfferRepository(conn)
    listing_id = repo.create_listing(semantic_key='listing-1')
    repo.link_source(listing_id=listing_id, source_identity_id=identity_id)
    first = repo.append_offer(
        listing_id=listing_id, canonical_record_id=canonical_id,
        offer_type='rent', monthly_rent_usd=800,
    )
    second = repo.append_offer(
        listing_id=listing_id, canonical_record_id=canonical_id,
        offer_type='rent', monthly_rent_usd=750,
    )
    rows = conn.execute(
        'SELECT id,is_current,supersedes_id,monthly_rent_usd FROM listing_offers ORDER BY id'
    ).fetchall()
    assert [(r['id'], r['is_current']) for r in rows] == [(first.id, 0), (second.id, 1)]
    assert rows[1]['supersedes_id'] == first.id
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            '''INSERT INTO listing_offers(
               listing_id,canonical_record_id,offer_type,monthly_rent_usd,is_current
               ) VALUES (?,?,?,?,1)''',
            (listing_id, canonical_id, 'rent', 700),
        )
