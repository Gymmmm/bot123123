from __future__ import annotations

import sqlite3

import pytest

from qiaolian_v3.db.repositories import CanonicalRecordRepository, ListingOfferRepository, SourceRepository
from tests.v3.db._helpers import insert_source, migrated_connection


def _offer(conn) -> int:
    legacy_id = insert_source(conn)
    sources = SourceRepository(conn)
    identity_id = sources.register_identity(
        legacy_source_post_id=legacy_id,
        source_type='telegram_channel', source_name='fixture', external_post_id='100',
    )
    revision = sources.append_revision(source_identity_id=identity_id, content_hash='h1', raw_text='evidence')
    canonical = CanonicalRecordRepository(conn).append(
        source_identity_id=identity_id, source_post_revision_id=revision.id,
        schema_version='v3', parser_revision='r1', facts={'deal_type': 'rent'}, facts_hash='f1', deal_type='rent',
    )
    offers = ListingOfferRepository(conn)
    listing_id = offers.create_listing(semantic_key='listing-1')
    offers.link_source(listing_id=listing_id, source_identity_id=identity_id)
    return offers.append_offer(
        listing_id=listing_id, canonical_record_id=canonical.id,
        offer_type='rent', monthly_rent_usd=800,
    ).id


def test_publication_package_snapshot_fields_are_immutable():
    conn = migrated_connection()
    offer_id = _offer(conn)
    cur = conn.execute(
        '''INSERT INTO v3_publication_packages(
           package_key,listing_offer_id,snapshot_json,snapshot_hash
           ) VALUES (?,?,?,?)''',
        ('pkg-1', offer_id, '{"rent":800}', 'snap-1'),
    )
    package_id = int(cur.lastrowid)
    conn.execute("UPDATE v3_publication_packages SET state='approved' WHERE id=?", (package_id,))
    with pytest.raises(sqlite3.IntegrityError, match='immutable'):
        conn.execute(
            "UPDATE v3_publication_packages SET snapshot_json='{}' WHERE id=?",
            (package_id,),
        )


def test_channel_post_idempotency_and_message_mapping_are_unique():
    conn = migrated_connection()
    offer_id = _offer(conn)
    p1 = conn.execute(
        "INSERT INTO v3_publication_packages(package_key,listing_offer_id,snapshot_json,snapshot_hash) VALUES (?,?,?,?)",
        ('pkg-1', offer_id, '{}', 'snap-1'),
    ).lastrowid
    conn.execute(
        "INSERT INTO v3_channel_posts(publication_package_id,idempotency_key,channel_id,message_id) VALUES (?,?,?,?)",
        (p1, 'idem-1', '-1001', 10),
    )
    p2 = conn.execute(
        "INSERT INTO v3_publication_packages(package_key,listing_offer_id,snapshot_json,snapshot_hash) VALUES (?,?,?,?)",
        ('pkg-2', offer_id, '{}', 'snap-2'),
    ).lastrowid
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO v3_channel_posts(publication_package_id,idempotency_key,channel_id,message_id) VALUES (?,?,?,?)",
            (p2, 'idem-1', '-1001', 11),
        )
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO v3_channel_posts(publication_package_id,idempotency_key,channel_id,message_id) VALUES (?,?,?,?)",
            (p2, 'idem-2', '-1001', 10),
        )
