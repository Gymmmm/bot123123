#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sqlite3
import sys
from pathlib import Path
from urllib.parse import urlparse

from dotenv import load_dotenv

ROOT = Path('/opt/qiaolian_v3/current').resolve()
load_dotenv('/opt/qiaolian_v3/runtime.env', override=True)
os.chdir(ROOT)
sys.path.insert(0, str(ROOT))

from v3_core.parser.worker import CanonicalWorker  # noqa: E402

DB = Path(os.getenv('DB_PATH', '/opt/qiaolian_dual_bots/data/qiaolian_dual_bot.db')).resolve()
SOURCE_NAME = os.getenv('REBUILD_SOURCE_NAME', 'zufang555').strip()
TARGET = max(1, int(os.getenv('REBUILD_TARGET_GROUPS', '50') or 50))
BATCH = os.getenv('REBUILD_BATCH_ID', 'zufang555_20260912_v2').strip()


def meta_anchor(row: sqlite3.Row) -> int:
    try:
        meta = json.loads(str(row['raw_meta_json'] or '{}'))
    except Exception:
        meta = {}
    try:
        anchor = int(meta.get('anchor_message_id') or 0)
    except Exception:
        anchor = 0
    if anchor > 0:
        return anchor
    try:
        path = urlparse(str(row['source_url'] or '')).path.rstrip('/')
        tail = path.rsplit('/', 1)[-1]
        return int(tail) if tail.isdigit() else 0
    except Exception:
        return 0


def main() -> None:
    conn = sqlite3.connect(str(DB), timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA busy_timeout=30000')

    rows = conn.execute(
        "SELECT id,source_post_id,source_url,parse_status,raw_meta_json FROM source_posts WHERE source_name=?",
        (SOURCE_NAME,),
    ).fetchall()
    ordered = sorted(rows, key=lambda row: (meta_anchor(row), int(row['id'])), reverse=True)
    selected = ordered[:TARGET]
    selected_ids = [int(r['id']) for r in selected]
    if not selected_ids:
        raise SystemExit('REBUILD_FATAL no_source_rows')

    worker = CanonicalWorker(str(DB))
    processed = failed = blocked_media = 0
    materialized_listing_ids: set[str] = set()

    for row in reversed(selected):
        source_id = int(row['id'])
        status = str(row['parse_status'] or '')
        if status == 'insufficient_media':
            blocked_media += 1
            continue
        conn.execute(
            "UPDATE source_posts SET parse_status='pending',updated_at=CURRENT_TIMESTAMP WHERE id=?",
            (source_id,),
        )
        conn.commit()
        result = worker.process_one(source_id)
        if result.status == 'materialized':
            processed += 1
            if result.listing_id:
                materialized_listing_ids.add(str(result.listing_id))
        else:
            failed += 1
            print(f'REBUILD_PARSE_ERROR source_pk={source_id} error={result.error}')

    if not materialized_listing_ids:
        print(
            f'REBUILD_RESULT source={SOURCE_NAME} selected={len(selected_ids)} processed={processed} '
            f'blocked_media={blocked_media} failed={failed} queued=0 archived_publications=0 superseded_packages=0'
        )
        return

    placeholders = ','.join('?' for _ in materialized_listing_ids)
    listing_ids = sorted(materialized_listing_ids)
    offer_rows = conn.execute(
        f"""SELECT o.offer_id,o.listing_id,o.offer_type,o.offer_status,o.publication_policy,o.publishable,
                    r.review_id,r.review_status
             FROM listing_offers o
             LEFT JOIN review_items r ON r.offer_id=o.offer_id
             WHERE o.listing_id IN ({placeholders})
               AND o.offer_type='rent' AND o.offer_status='active'
               AND o.publication_policy='telegram_rent'""",
        listing_ids,
    ).fetchall()

    queued = archived_publications = superseded_packages = 0
    for row in offer_rows:
        offer_id = str(row['offer_id'])

        # Rebuild is intentionally routed back through AutoPublishService. New rent
        # offers start publishable=0/review_required by design; package approval
        # flips publishable only after the normal strict eligibility checks pass.
        cur = conn.execute(
            """UPDATE publication_instances
               SET publish_status=?,updated_at=CURRENT_TIMESTAMP
               WHERE offer_id=? AND platform='telegram' AND publish_status='published'""",
            (f'archived_rebuild:{BATCH}', offer_id),
        )
        archived_publications += int(cur.rowcount or 0)

        cur = conn.execute(
            """UPDATE publication_packages_v3
               SET status=?,updated_at=CURRENT_TIMESTAMP
               WHERE offer_id=? AND status IN ('approved','published')""",
            (f'superseded_rebuild:{BATCH}', offer_id),
        )
        superseded_packages += int(cur.rowcount or 0)

        review_id = str(row['review_id'] or '')
        if not review_id:
            continue
        conn.execute(
            """UPDATE review_items SET review_status='pending',operator_user_id='',approved_at=NULL,
               review_note=CASE WHEN review_note='' THEN ? ELSE review_note || ' | ' || ? END,
               updated_at=CURRENT_TIMESTAMP WHERE review_id=?""",
            (f'rebuild:{BATCH}', f'rebuild:{BATCH}', review_id),
        )

        conn.execute(
            """INSERT INTO publisher_auto_items_v3
               (offer_id,listing_id,review_id,state,reason_code,reason_text,package_id,channel_message_id,origin,ignored,published_at)
               VALUES (?,?,?,'queued','','','','','rebuild',0,NULL)
               ON CONFLICT(offer_id) DO UPDATE SET
                 listing_id=excluded.listing_id,review_id=excluded.review_id,state='queued',
                 reason_code='',reason_text='',package_id='',channel_message_id='',origin='rebuild',ignored=0,
                 published_at=NULL,updated_at=CURRENT_TIMESTAMP""",
            (offer_id, str(row['listing_id']), review_id),
        )
        queued += 1

    conn.commit()
    qcount = int(conn.execute(
        "SELECT COUNT(*) FROM publisher_auto_items_v3 WHERE state='queued' AND ignored=0"
    ).fetchone()[0])
    anchors = [meta_anchor(r) for r in selected]
    print(
        f'REBUILD_RESULT source={SOURCE_NAME} selected={len(selected_ids)} processed={processed} '
        f'blocked_media={blocked_media} failed={failed} listings={len(materialized_listing_ids)} '
        f'offers={len(offer_rows)} queued={queued} queue_total={qcount} '
        f'archived_publications={archived_publications} superseded_packages={superseded_packages} '
        f'oldest_anchor={min(anchors)} newest_anchor={max(anchors)}'
    )


if __name__ == '__main__':
    main()
