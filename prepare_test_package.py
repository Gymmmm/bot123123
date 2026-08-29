import sqlite3
from ai_parser import AIParserModule
from qiaolian_dual.canonical_facts import canonicalize_source
from qiaolian_dual.canonical_listing_materializer import materialize_draft_facts
from publication_package import build_package

DB = "data/qiaolian_dual_bot.db"
SOURCE_POST_ID = 70

parser = AIParserModule(DB)
row = parser.db_manager._fetch_one(
    "SELECT id,source_id,source_type,source_post_id,raw_text,raw_meta_json,raw_images_json,raw_videos_json FROM source_posts WHERE id=?",
    (SOURCE_POST_ID,),
)
if not row:
    raise SystemExit("source_not_found")
source = parser._as_mapping(row, parser._SOURCE_COLUMNS)
_, _, raw_text, sanitized_text, identity, media_summary = parser._inputs(source)
base = canonicalize_source(
    raw_text=raw_text,
    sanitized_text=sanitized_text,
    source_identity=identity,
    media_summary=media_summary,
)
flags = [f for f in (base.get("candidate_flags") or []) if f != "mixed_sale_rent_terms"]
facts = canonicalize_source(
    raw_text=raw_text,
    sanitized_text=sanitized_text,
    source_identity=identity,
    media_summary=media_summary,
    manual_overrides={
        "deal_type": "rent",
        "market_location_keys": ["一号路"],
        "market_location_displays": ["一号路附近"],
        "candidate_flags": flags,
    },
)
draft_row = parser.db_manager._fetch_one(
    "SELECT draft_id FROM drafts WHERE source_post_id=? ORDER BY id DESC LIMIT 1",
    (SOURCE_POST_ID,),
)
if not draft_row:
    raise SystemExit("draft_not_found")
draft_id = draft_row[0]
conn = sqlite3.connect(DB)
materialize_draft_facts(conn, draft_id=draft_id, facts=facts)
quality = facts.get("quality") or {}
review_note = (
    "manual_rental_selection: source explicitly contains rental offer; "
    "sale offer excluded from public copy | " + parser._review_note(facts)
)
conn.execute(
    "UPDATE drafts SET review_status=?, review_note=?, updated_at=CURRENT_TIMESTAMP WHERE draft_id=?",
    ("pending", review_note[:500], draft_id),
)
conn.commit()
conn.close()
pkg = build_package(DB, draft_id)
print("DRAFT", draft_id)
print("PACKAGE", pkg["package_id"], pkg["property_id"], pkg["cover_template"])
print("LOCATION", facts.get("public_location_display"), facts.get("publication_location_level"))
print("QUALITY", quality)
print("CAPTION\n" + pkg["post_text"])
print("MEDIA", len(pkg["main_images"]), len(pkg["discussion_images"]))
