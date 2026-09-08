from __future__ import annotations

from qiaolian_v3.db.migration_runner import MigrationRunner
from tests.v3.db._helpers import baseline_connection


def test_forward_migration_from_locked_baseline_and_ledger():
    conn = baseline_connection()
    runner = MigrationRunner(conn)
    assert runner.migrate() == ['001', '002']
    row = conn.execute("SELECT version,name,checksum FROM schema_migrations WHERE version='001'").fetchone()
    assert row is not None
    assert row[1] == '001_phase1_core.sql'
    assert len(row[2]) == 64
    row2 = conn.execute("SELECT version,name,checksum FROM schema_migrations WHERE version='002'").fetchone()
    assert row2 is not None
    assert row2[1] == '002_workpack_b_sync_leases.sql'
    assert len(row2[2]) == 64
    assert runner.migrate() == []


def test_simulation_rolls_back_all_pending_schema_changes():
    conn = baseline_connection()
    runner = MigrationRunner(conn)
    assert runner.simulate_pending() == ['001', '002']
    names = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert 'canonical_records' not in names
    assert 'listing_offers' not in names
    assert 'source_post_revisions' not in names
    assert 'v3_sync_leases' not in names
    assert runner.migrate() == ['001', '002']
