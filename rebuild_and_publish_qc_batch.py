import os, sys
from pathlib import Path
sys.path.insert(0, '/opt/qiaolian_dual_bots')
sys.path = [p for p in sys.path if p not in ('/tmp', '')]
from publication_package import build_package, approve_package
from meihua_publisher import MeihuaPublisher
DB = os.getenv('DB_PATH', 'data/qiaolian_dual_bot.db')
DRAFTS = [
    'DRF_480132d7-d4cc-4122-85f6-e0892f449e60',
    'DRF_e713cf31-3aca-43ae-8ba2-213e11ac67fe',
]
for draft_id in DRAFTS:
    package = build_package(DB, draft_id)
    approved = approve_package(DB, draft_id, approved_by='reedit_batch_manual_authorization')
    print('PACKAGE_APPROVED', draft_id, approved.get('package_id'))
publisher = MeihuaPublisher(DB)
for draft_id in DRAFTS:
    ok = publisher.publish_draft(draft_id)
    print('PUBLISHED', draft_id, ok)
