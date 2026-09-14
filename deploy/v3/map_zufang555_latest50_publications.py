#!/usr/bin/env python3
from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from urllib.parse import urlparse

DB = Path('/opt/qiaolian_dual_bots/data/qiaolian_dual_bot.db')
SOURCE = 'zufang555'
TARGET = 50


def anchor(row: sqlite3.Row) -> int:
    try:
        meta = json.loads(str(row['raw_meta_json'] or '{}'))
    except Exception:
        meta = {}
    try:
        value = int(meta.get('anchor_message_id') or 0)
    except Exception:
        value = 0
    if value:
        return value
    try:
        tail = urlparse(str(row['source_url'] or '')).path.rstrip('/').rsplit('/', 1)[-1]
        return int(tail) if tail.isdigit() else 0
    except Exception:
        return 0


def identity_values(raw: str) -> tuple[str, str]:
    try:
        obj = json.loads(raw or '{}')
    except Exception:
        return '', ''
    name = str(obj.get('source_name') or obj.get('name') or '')
    post = str(obj.get('source_post_id') or obj.get('post_id') or obj.get('message_id') or '')
    return name, post


def main() -> None:
    c = sqlite3.connect(DB)
    c.row_factory = sqlite3.Row
    source_rows = c.execute(
        'SELECT id,source_post_id,source_url,raw_meta_json,parse_status FROM source_posts WHERE source_name=?',
        (SOURCE,),
    ).fetchall()
    selected = sorted(source_rows, key=lambda r: (anchor(r), int(r['id'])), reverse=True)[:TARGET]
    selected_posts = {str(r['source_post_id']) for r in selected}
    selected_anchors = {anchor(r) for r in selected}
    print('LATEST50 selected=', len(selected), 'oldest_anchor=', min(selected_anchors), 'newest_anchor=', max(selected_anchors))

    pubs = c.execute(
        '''SELECT i.instance_id,i.package_id,i.listing_id,i.offer_id,i.channel_message_id,i.publish_status,
                  p.source_identity_json,p.snapshot_json
           FROM publication_instances i
           JOIN publication_packages_v3 p ON p.package_id=i.package_id
           WHERE i.platform='telegram' AND CAST(COALESCE(i.channel_message_id,'0') AS INTEGER)>0'''
    ).fetchall()

    matched = []
    for p in pubs:
        name, post = identity_values(str(p['source_identity_json'] or ''))
        hit = name == SOURCE and post in selected_posts
        if not hit:
            try:
                snap = json.loads(str(p['snapshot_json'] or '{}'))
            except Exception:
                snap = {}
            candidates = [snap.get('source_identity'), snap.get('source')]
            for candidate in candidates:
                if isinstance(candidate, dict):
                    n = str(candidate.get('source_name') or candidate.get('name') or '')
                    q = str(candidate.get('source_post_id') or candidate.get('post_id') or candidate.get('message_id') or '')
                    if n == SOURCE and q in selected_posts:
                        name, post, hit = n, q, True
                        break
        if hit:
            matched.append((int(p['channel_message_id']), str(p['instance_id']), str(p['listing_id']), str(p['offer_id']), post, str(p['publish_status'])))

    matched.sort()
    print('MATCHED_PUBLICATIONS', len(matched))
    for item in matched:
        print('TARGET message_id=%s instance=%s listing=%s offer=%s source_post=%s status=%s' % item)

    unmatched = sorted(selected_posts - {m[4] for m in matched})
    print('UNMATCHED_SOURCE_GROUPS', len(unmatched))
    print('UNMATCHED_IDS', ','.join(unmatched))
    c.close()


if __name__ == '__main__':
    main()
