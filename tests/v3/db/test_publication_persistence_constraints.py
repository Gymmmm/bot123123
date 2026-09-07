from __future__ import annotations

import sqlite3

import pytest

from qiaolian_v3.db.repositories import CanonicalRecordRepository, ListingOfferRepository, SourceRepository
from tests.v3.db._helpers import migrated_connection


def _offer(conn, suffix: str = '1') -> tuple[int, int, int]:
    sources = SourceRepository(conn)
    source_post_id = sources.register_source_post(
        source_mode='collector', source_type='telegram_channel',
        source_name='fixture', external_post_id=f'100-{suffix}',
    )
    revision = sources.append_revision(
        source_post_id=source_post_id, source_content_hash=f'h-{suffix}', raw_text='evidence'
    )
    canonical = CanonicalRecordRepository(conn).append(
        source_post_id=source_post_id, source_post_revision_id=revision.id,
        schema_version='v3', parser_revision='r1', facts={'deal_type': 'rent'},
        facts_hash=f'f-{suffix}', deal_type='rent', processing_status='READY_TO_PUBLISH',
    )
    offers = ListingOfferRepository(conn)
    listing_id = offers.create_listing(
        property_identity_key=f'property-{suffix}', public_listing_id=f'QL-{suffix.zfill(6)}',
        current_canonical_record_id=canonical.id, property_type='apartment', listing_status='active',
    )
    offers.link_source(listing_id=listing_id, source_post_id=source_post_id)
    offer_id = offers.append_offer(
        listing_id=listing_id, canonical_record_id=canonical.id,
        offer_type='rent', monthly_rent_usd=800, publication_policy='telegram_rent',
    ).id
    return listing_id, offer_id, canonical.id


def _package(
    conn,
    *,
    listing_id: int,
    offer_id: int,
    canonical_id: int,
    version: int,
    package_id: str,
    channel_id: str = '-1001',
    status: str = 'prepared',
) -> int:
    return int(conn.execute(
        '''INSERT INTO v3_publication_packages(
           package_id,idempotency_key,listing_id,offer_id,canonical_record_id,package_version,
           target_kind,target_channel_id,status,approval_mode,approved_by,approved_at,
           cover_path,cover_hash,gallery_json,gallery_hash,caption_html,keyboard_json,
           content_hash,canonical_hash
           ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
        (
            package_id, f'idem-{package_id}', listing_id, offer_id, canonical_id, version,
            'telegram_rent', channel_id, status, 'auto', 'system:auto_publish', '2026-09-07T09:00:00Z',
            '/tmp/cover.jpg', f'cover-{package_id}', '["a.jpg"]', f'gallery-{package_id}',
            '<b>caption</b>', '{"buttons":[]}', f'content-{package_id}', f'canonical-{package_id}',
        ),
    ).lastrowid)


def test_publication_package_final_fields_and_frozen_content_are_immutable():
    conn = migrated_connection()
    listing_id, offer_id, canonical_id = _offer(conn)
    package_row_id = _package(
        conn, listing_id=listing_id, offer_id=offer_id, canonical_id=canonical_id,
        version=1, package_id='pkg-1',
    )
    row = conn.execute('SELECT * FROM v3_publication_packages WHERE id=?', (package_row_id,)).fetchone()
    assert row['package_version'] == 1
    assert row['target_kind'] == 'telegram_rent'
    assert row['approval_mode'] == 'auto'
    assert row['content_hash'] == 'content-pkg-1'
    assert row['canonical_hash'] == 'canonical-pkg-1'

    conn.execute("UPDATE v3_publication_packages SET status='frozen' WHERE id=?", (package_row_id,))
    with pytest.raises(sqlite3.IntegrityError, match='immutable'):
        conn.execute(
            "UPDATE v3_publication_packages SET caption_html='<b>changed</b>' WHERE id=?",
            (package_row_id,),
        )
    with pytest.raises(sqlite3.IntegrityError, match='immutable'):
        conn.execute(
            "UPDATE v3_publication_packages SET content_hash='changed' WHERE id=?",
            (package_row_id,),
        )


def test_publication_package_version_target_approval_and_idempotency_constraints():
    conn = migrated_connection()
    listing_id, offer_id, canonical_id = _offer(conn)
    _package(conn, listing_id=listing_id, offer_id=offer_id, canonical_id=canonical_id, version=1, package_id='pkg-1')

    with pytest.raises(sqlite3.IntegrityError):
        _package(conn, listing_id=listing_id, offer_id=offer_id, canonical_id=canonical_id, version=1, package_id='pkg-duplicate-version')
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            '''INSERT INTO v3_publication_packages(
               package_id,idempotency_key,listing_id,offer_id,canonical_record_id,package_version,
               target_kind,target_channel_id,approval_mode,content_hash,canonical_hash
               ) VALUES (?,?,?,?,?,?,?,?,?,?,?)''',
            ('bad-target','bad-target-idem',listing_id,offer_id,canonical_id,2,
             'telegram_sale','-1001','auto','content','canonical'),
        )
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            '''INSERT INTO v3_publication_packages(
               package_id,idempotency_key,listing_id,offer_id,canonical_record_id,package_version,
               target_channel_id,approval_mode,content_hash,canonical_hash
               ) VALUES (?,?,?,?,?,?,?,?,?,?)''',
            ('bad-approval','bad-approval-idem',listing_id,offer_id,canonical_id,2,
             '-1001','implicit','content','canonical'),
        )


def test_channel_post_final_mapping_and_unique_constraints():
    conn = migrated_connection()
    listing1, offer1, canonical1 = _offer(conn, '1')
    package1 = _package(
        conn, listing_id=listing1, offer_id=offer1, canonical_id=canonical1,
        version=1, package_id='pkg-1', channel_id='-1001', status='frozen',
    )
    conn.execute(
        '''INSERT INTO v3_channel_posts(
           idempotency_key,channel_id,message_id,listing_id,offer_id,current_package_id,
           publication_kind,content_hash,post_status,last_synced_at,published_at
           ) VALUES (?,?,?,?,?,?,?,?,?,?,?)''',
        ('post-1','-1001',10,listing1,offer1,package1,'telegram_rent','content-pkg-1','published',
         '2026-09-07T09:10:00Z','2026-09-07T09:05:00Z'),
    )

    # One current Telegram-rent channel mapping per listing.
    package1_v2 = _package(
        conn, listing_id=listing1, offer_id=offer1, canonical_id=canonical1,
        version=2, package_id='pkg-1-v2', channel_id='-1001', status='frozen',
    )
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            '''INSERT INTO v3_channel_posts(
               idempotency_key,channel_id,message_id,listing_id,offer_id,current_package_id,
               publication_kind,content_hash
               ) VALUES (?,?,?,?,?,?,?,?)''',
            ('post-1-v2','-1001',11,listing1,offer1,package1_v2,'telegram_rent','content-pkg-1-v2'),
        )

    # (channel_id, message_id) is unique independently of listing.
    listing2, offer2, canonical2 = _offer(conn, '2')
    package2 = _package(
        conn, listing_id=listing2, offer_id=offer2, canonical_id=canonical2,
        version=1, package_id='pkg-2', channel_id='-1001', status='frozen',
    )
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            '''INSERT INTO v3_channel_posts(
               idempotency_key,channel_id,message_id,listing_id,offer_id,current_package_id,
               publication_kind,content_hash
               ) VALUES (?,?,?,?,?,?,?,?)''',
            ('post-2','-1001',10,listing2,offer2,package2,'telegram_rent','content-pkg-2'),
        )


def test_channel_post_current_package_must_match_listing_offer_target():
    conn = migrated_connection()
    listing1, offer1, canonical1 = _offer(conn, '1')
    package1 = _package(
        conn, listing_id=listing1, offer_id=offer1, canonical_id=canonical1,
        version=1, package_id='pkg-1', channel_id='-1001', status='frozen',
    )
    listing2, offer2, _ = _offer(conn, '2')
    with pytest.raises(sqlite3.IntegrityError, match='mismatch'):
        conn.execute(
            '''INSERT INTO v3_channel_posts(
               idempotency_key,channel_id,message_id,listing_id,offer_id,current_package_id,
               publication_kind,content_hash
               ) VALUES (?,?,?,?,?,?,?,?)''',
            ('bad-map','-1001',99,listing2,offer2,package1,'telegram_rent','bad'),
        )
