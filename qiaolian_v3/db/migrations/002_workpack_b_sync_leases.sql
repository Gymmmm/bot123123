-- Workpack B additive runtime ownership only.
-- Phase-1 migration history is unchanged.
CREATE TABLE IF NOT EXISTS v3_sync_leases (
    lease_key TEXT PRIMARY KEY,
    owner TEXT NOT NULL,
    expected_content_hash TEXT NOT NULL DEFAULT '',
    acquired_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_v3_sync_leases_owner
ON v3_sync_leases(owner, updated_at);
