import sqlite3

from v3_core.inventory.identity import IdentityService


def test_identity_continues_after_legacy_listing_numbers(tmp_path):
    db = tmp_path / "identity.sqlite3"
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE listings (listing_id TEXT PRIMARY KEY)")
        conn.executemany(
            "INSERT INTO listings(listing_id) VALUES (?)",
            [("l_349",), ("l_350",)],
        )
        conn.commit()

    service = IdentityService(str(db))
    identity = service.allocate(
        canonical_record_id="CAN_1",
        facts={"project_name": "富力城", "public_location_display": "富力城"},
    )

    assert identity.listing_id == "l_351"
    assert identity.public_listing_id.startswith("QL-RF-")


def test_identity_is_idempotent_for_same_canonical_record(tmp_path):
    service = IdentityService(str(tmp_path / "identity-idem.sqlite3"))
    first = service.allocate(
        canonical_record_id="CAN_SAME",
        facts={"public_location_display": "BKK1"},
    )
    second = service.allocate(
        canonical_record_id="CAN_SAME",
        facts={"public_location_display": "BKK1"},
    )

    assert second == first
    assert first.listing_id == "l_1"
    assert first.public_listing_id.startswith("QL-BK-")
