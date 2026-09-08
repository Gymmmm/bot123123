from __future__ import annotations

from qiaolian_v3.listing.public_id import assign_public_listing_id, normalize_public_id
from qiaolian_v3.legacy.public_id_compat import normalize_legacy_listing_reference
from tests.v3.db._helpers import migrated_connection


def test_public_id_is_ql_location_aware_and_stable_on_listing():
    conn = migrated_connection()
    listing_id = conn.execute(
        "INSERT INTO v3_listings(property_identity_key,canonical_area_key,listing_status) VALUES ('p1','BKK1','active')"
    ).lastrowid
    first = assign_public_listing_id(conn, int(listing_id))
    second = assign_public_listing_id(conn, int(listing_id))
    assert first == second
    assert first.startswith('QL-BK-')
    assert normalize_public_id(first.lower().replace('-', '')) == first
    assert conn.execute('SELECT public_listing_id FROM v3_listings WHERE id=?', (listing_id,)).fetchone()[0] == first


def test_public_id_falls_back_to_pp_and_legacy_qc_qj_are_compat_only():
    conn = migrated_connection()
    listing_id = conn.execute(
        "INSERT INTO v3_listings(property_identity_key,listing_status) VALUES ('p2','active')"
    ).lastrowid
    public_id = assign_public_listing_id(conn, int(listing_id))
    assert public_id.startswith('QL-PP-')
    assert normalize_legacy_listing_reference('QC0350') == 'l_350'
    assert normalize_legacy_listing_reference('QJ-12') == 'l_12'
    assert normalize_public_id('QC0350') is None
