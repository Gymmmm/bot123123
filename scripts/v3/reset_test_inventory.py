"""Explicit test-inventory reset, preserving customers and channel edit slots."""
import argparse
import json
import sqlite3
from pathlib import Path

from v3_core.publishing.admin_bot import load_settings


def reset(db):
    db = Path(db).resolve()
    if str(db) != '/opt/qiaolian_dual_bots/data/qiaolian_dual_bot.db':
        raise ValueError('unexpected_production_database')
    with sqlite3.connect(db, timeout=30) as c:
        c.execute('PRAGMA foreign_keys=ON')
        c.execute('BEGIN IMMEDIATE')
        assert c.execute("SELECT count(*) FROM publication_delivery_attempts_v3 WHERE state IN ('sending','sent','unknown')").fetchone()[0] == 0
        assert c.execute("SELECT count(*) FROM publication_packages_v3 WHERE status='rebuild_hold'").fetchone()[0] == 0
        keep = set()
        protected = ['appointments_v3', 'appointments', 'leads_v3', 'leads',
                     'tenant_bindings_v3', 'tenant_bindings', 'repair_tickets_v3', 'repair_tickets']
        tables = {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        before = {t: c.execute(f'SELECT count(*) FROM {t}').fetchone()[0] for t in protected if t in tables}
        slots = list(c.execute('SELECT instance_id,channel_chat_id,channel_message_id FROM publication_instances ORDER BY id'))
        for table in protected:
            if table in tables and 'listing_id' in {r[1] for r in c.execute(f'PRAGMA table_info({table})')}:
                keep.update(str(r[0]) for r in c.execute(f'SELECT listing_id FROM {table} WHERE listing_id IS NOT NULL'))
        c.execute('CREATE TEMP TABLE retained_listings(listing_id TEXT PRIMARY KEY)')
        c.executemany('INSERT OR IGNORE INTO retained_listings VALUES (?)', ((i,) for i in keep))
        offer_keep = 'SELECT offer_id FROM listing_offers WHERE listing_id IN (SELECT listing_id FROM retained_listings)'
        canonical_keep = 'SELECT canonical_record_id FROM listings_v3 WHERE listing_id IN (SELECT listing_id FROM retained_listings)'
        source_keep = f'SELECT CAST(source_post_id AS INTEGER) FROM canonical_records WHERE canonical_record_id IN ({canonical_keep})'
        counts = {}
        for table in ['publisher_auto_items_v3','publisher_auto_validations_v3','publisher_auto_versions_v3',
                      'publisher_post_windows_v3','publisher_review_exceptions_v3','cover_render_jobs',
                      'publish_queue_v2','publish_logs','publish_analytics','discussion_map',
                      'publication_delivery_attempts','publication_packages','posts','drafts',
                      'excel_listing_rows','excel_intake_batches','collector_source_state_v3',
                      'channel_replacement_jobs_v3']:
            if table in tables:
                counts[table] = c.execute(f'DELETE FROM {table}').rowcount
        for table in ['publication_delivery_attempts_v3','publication_packages_v3','review_items']:
            counts[table] = c.execute(f'DELETE FROM {table} WHERE offer_id NOT IN ({offer_keep})').rowcount
        counts['media_assets'] = c.execute(f"DELETE FROM media_assets WHERE owner_type='source_post' AND CAST(owner_ref_id AS INTEGER) NOT IN ({source_keep})").rowcount
        counts['source_posts'] = c.execute(f'DELETE FROM source_posts WHERE id NOT IN ({source_keep})').rowcount
        c.execute(f"UPDATE source_posts SET parse_status='customer_history' WHERE id IN ({source_keep})")
        counts['canonical_overrides'] = c.execute(f'DELETE FROM canonical_overrides WHERE canonical_record_id NOT IN ({canonical_keep})').rowcount
        counts['canonical_records'] = c.execute(f'DELETE FROM canonical_records WHERE canonical_record_id NOT IN ({canonical_keep})').rowcount
        counts['listing_offers'] = c.execute('DELETE FROM listing_offers WHERE listing_id NOT IN (SELECT listing_id FROM retained_listings)').rowcount
        counts['listings_v3'] = c.execute('DELETE FROM listings_v3 WHERE listing_id NOT IN (SELECT listing_id FROM retained_listings)').rowcount
        # Legacy customer references may still use legacy listing ids.
        if 'listings' in tables and 'listing_id' in {r[1] for r in c.execute('PRAGMA table_info(listings)')}:
            counts['listings'] = c.execute('DELETE FROM listings WHERE listing_id NOT IN (SELECT listing_id FROM retained_listings)').rowcount
        assert before == {t: c.execute(f'SELECT count(*) FROM {t}').fetchone()[0] for t in before}
        assert slots == list(c.execute('SELECT instance_id,channel_chat_id,channel_message_id FROM publication_instances ORDER BY id'))
        assert not c.execute('PRAGMA foreign_key_check').fetchall()
        print(json.dumps({'deleted_rows': counts, 'protected_customer_tables': before,
                          'channel_slots_preserved': len(slots), 'customer_listing_ids_preserved': len(keep)},ensure_ascii=False), flush=True)
    return counts


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--execute', action='store_true', required=True)
    p.parse_args()
    reset(load_settings().db_path)
