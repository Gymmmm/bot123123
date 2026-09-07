from __future__ import annotations

import sqlite3

import pytest

from qiaolian_v3.db.repositories import CanonicalRecordRepository, ListingOfferRepository, SourceRepository
from tests.v3.db._helpers import migrated_connection


def _graph(conn):
    sources = SourceRepository(conn)
    source_post_id = sources.register_source_post(
        source_mode='admin_import', source_type='admin', source_name='manual', external_post_id='A-1'
    )
    revision = sources.append_revision(
        source_post_id=source_post_id, source_content_hash='source-hash', raw_text='manual evidence'
    )
    canonical = CanonicalRecordRepository(conn).append(
        source_post_id=source_post_id, source_post_revision_id=revision.id,
        schema_version='v3', parser_revision='r1', facts={'deal_type': 'rent'},
        facts_hash='canonical-hash', deal_type='rent', processing_status='NEEDS_REVIEW',
    )
    listings = ListingOfferRepository(conn)
    listing_id = listings.create_listing(
        property_identity_key='property-1', public_listing_id='QL-123456',
        current_canonical_record_id=canonical.id,
        project_name='Project A', project_alias='PA', property_type='apartment',
        property_subtype='condo', city_key='phnom-penh', project_key='project-a',
        canonical_area_key='bkk1', public_location_key='bkk1-public',
        public_location_display='BKK1', layout='2房1厅', bedrooms=2, living_rooms=1,
        bathrooms=2, helper_rooms=0, size_sqm=95.0, floor='19', listing_status='pending',
    )
    listings.link_source(listing_id=listing_id, source_post_id=source_post_id)
    offer_id = listings.append_offer(
        listing_id=listing_id, canonical_record_id=canonical.id,
        offer_type='rent', monthly_rent_usd=800,
    ).id
    return source_post_id, canonical.id, listing_id, offer_id


def test_listing_persists_formal_identity_and_property_fields():
    conn = migrated_connection()
    _, canonical_id, listing_id, _ = _graph(conn)
    row = conn.execute('SELECT * FROM v3_listings WHERE id=?', (listing_id,)).fetchone()
    assert row['public_listing_id'] == 'QL-123456'
    assert row['current_canonical_record_id'] == canonical_id
    assert row['property_identity_key'] == 'property-1'
    assert row['project_name'] == 'Project A'
    assert row['project_alias'] == 'PA'
    assert row['property_type'] == 'apartment'
    assert row['property_subtype'] == 'condo'
    assert row['city_key'] == 'phnom-penh'
    assert row['project_key'] == 'project-a'
    assert row['canonical_area_key'] == 'bkk1'
    assert row['public_location_key'] == 'bkk1-public'
    assert row['public_location_display'] == 'BKK1'
    assert row['layout'] == '2房1厅'
    assert row['bedrooms'] == 2
    assert row['living_rooms'] == 1
    assert row['bathrooms'] == 2
    assert row['helper_rooms'] == 0
    assert row['size_sqm'] == 95.0
    assert row['floor'] == '19'
    assert row['listing_status'] == 'pending'


def test_listing_status_is_locked_to_formal_contract_and_public_id_is_unique():
    conn = migrated_connection()
    repo = ListingOfferRepository(conn)
    for idx, status in enumerate(('active','reserved','pending','rented','inactive'), start=1):
        repo.create_listing(property_identity_key=f'property-{idx}', listing_status=status)
    for bad in ('closed', 'archived'):
        with pytest.raises(sqlite3.IntegrityError):
            repo.create_listing(property_identity_key=f'bad-{bad}', listing_status=bad)

    repo.create_listing(property_identity_key='public-a', public_listing_id='QL-999999')
    with pytest.raises(sqlite3.IntegrityError):
        repo.create_listing(property_identity_key='public-b', public_listing_id='QL-999999')


def test_review_item_formal_references_json_source_mode_and_status_contract():
    conn = migrated_connection()
    _, canonical_id, listing_id, offer_id = _graph(conn)
    review_id = conn.execute(
        '''INSERT INTO review_items(
           review_type,canonical_record_id,listing_id,offer_id,reason_codes_json,
           status,source_mode,operator_id,resolution_json
           ) VALUES (?,?,?,?,?,?,?,?,?)''',
        ('admin_listing_review', canonical_id, listing_id, offer_id,
         '["MISSING_PAYMENT_TERMS"]', 'open', 'admin_import', 'admin:1', '{}'),
    ).lastrowid
    row = conn.execute('SELECT * FROM review_items WHERE id=?', (review_id,)).fetchone()
    assert row['canonical_record_id'] == canonical_id
    assert row['listing_id'] == listing_id
    assert row['offer_id'] == offer_id
    assert row['reason_codes_json'] == '["MISSING_PAYMENT_TERMS"]'
    assert row['source_mode'] == 'admin_import'
    assert row['resolution_json'] == '{}'

    for status in ('approved','rejected','resolved'):
        conn.execute('UPDATE review_items SET status=? WHERE id=?', (status, review_id))
        assert conn.execute('SELECT status FROM review_items WHERE id=?', (review_id,)).fetchone()[0] == status


def test_review_item_rejects_invalid_status_source_mode_json_and_missing_subject():
    conn = migrated_connection()
    _, canonical_id, _, _ = _graph(conn)
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            '''INSERT INTO review_items(
               review_type,canonical_record_id,reason_codes_json,status,source_mode,resolution_json
               ) VALUES ('x',?,'[]','pending','collector','{}')''',
            (canonical_id,),
        )
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            '''INSERT INTO review_items(
               review_type,canonical_record_id,reason_codes_json,status,source_mode,resolution_json
               ) VALUES ('x',?,'[]','open','csv_import','{}')''',
            (canonical_id,),
        )
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            '''INSERT INTO review_items(
               review_type,canonical_record_id,reason_codes_json,status,source_mode,resolution_json
               ) VALUES ('x',?,'not-json','open','collector','{}')''',
            (canonical_id,),
        )
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            '''INSERT INTO review_items(
               review_type,reason_codes_json,status,source_mode,resolution_json
               ) VALUES ('x','[]','open','collector','{}')'''
        )
