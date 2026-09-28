import os
import sys
import sqlite3
import json

sys.path = [p for p in sys.path if p not in ('/tmp', '')]
sys.path.insert(0, '/opt/qiaolian_dual_bots')

from publication_package import build_package, approve_package
from meihua_publisher import MeihuaPublisher

DB = os.getenv('DB_PATH', 'data/qiaolian_dual_bot.db')
DRAFT = 'DRF_480132d7-d4cc-4122-85f6-e0892f449e60'

with sqlite3.connect(DB) as conn:
    old = conn.execute("SELECT review_note FROM drafts WHERE draft_id=?", (DRAFT,)).fetchone()
    note = str(old[0] or '') if old else ''
    note = note.replace('publish_layout:links', '').replace('publish_layout:buttons', '').strip()
    note = (note + ' publish_layout:buttons').strip()
    conn.execute("UPDATE drafts SET review_note=?, review_status='pending', published_at=NULL, updated_at=CURRENT_TIMESTAMP WHERE draft_id=?", (note, DRAFT))
    conn.execute("UPDATE posts SET publish_status='archived', updated_at=CURRENT_TIMESTAMP WHERE listing_id='l_2' AND publish_status='published'")
    conn.execute("UPDATE publication_packages SET status='superseded', updated_at=CURRENT_TIMESTAMP WHERE property_id='l_2' AND status IN ('published','approved')")
    conn.commit()

package = build_package(DB, DRAFT)
approved = approve_package(DB, DRAFT, approved_by='two_step_cover_photos_test')
print('PACKAGE_APPROVED', approved.get('package_id'))
ok = MeihuaPublisher(DB).publish_draft(DRAFT)
print('PUBLISHED', DRAFT, ok)
if not ok:
    raise SystemExit(2)

with sqlite3.connect(DB) as conn:
    row = conn.execute("SELECT listing_id,channel_message_id,publication_package_id FROM posts WHERE listing_id='l_2' AND publish_status='published' ORDER BY id DESC LIMIT 1").fetchone()
    pkg = conn.execute("SELECT main_images_json,discussion_images_json,public_token FROM publication_packages WHERE package_id=?", (row[2],)).fetchone()
    print('POST', row)
    print('PACKAGE', 'buttons', len(json.loads(pkg[0] or '[]')), len(json.loads(pkg[1] or '[]')), pkg[2])
