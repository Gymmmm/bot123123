import os
import sys
import sqlite3

sys.path = [p for p in sys.path if p not in ('/tmp', '')]
sys.path.insert(0, '/opt/qiaolian_dual_bots')

from publication_package import build_package, approve_package
from meihua_publisher import MeihuaPublisher

DB = os.getenv('DB_PATH', 'data/qiaolian_dual_bot.db')
DRAFT = 'DRF_480132d7-d4cc-4122-85f6-e0892f449e60'

with sqlite3.connect(DB) as conn:
    conn.execute("UPDATE posts SET publish_status='archived', updated_at=CURRENT_TIMESTAMP WHERE listing_id='l_2' AND publish_status='published'")
    conn.execute("UPDATE publication_packages SET status='superseded', updated_at=CURRENT_TIMESTAMP WHERE property_id='l_2' AND status IN ('published','approved')")
    conn.execute("UPDATE drafts SET review_status='pending', published_at=NULL, updated_at=CURRENT_TIMESTAMP WHERE draft_id=?", (DRAFT,))
    conn.commit()

package = build_package(DB, DRAFT)
approved = approve_package(DB, DRAFT, approved_by='media_group_10_image_test')
print('PACKAGE_APPROVED', approved.get('package_id'))
ok = MeihuaPublisher(DB).publish_draft(DRAFT)
print('PUBLISHED', DRAFT, ok)
if not ok:
    raise SystemExit(2)

with sqlite3.connect(DB) as conn:
    row = conn.execute("SELECT channel_message_id, publication_package_id FROM posts WHERE listing_id='l_2' AND publish_status='published' ORDER BY id DESC LIMIT 1").fetchone()
    print('POST', row)
