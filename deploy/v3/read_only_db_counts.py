import sqlite3
from collections import OrderedDict

DB = "/opt/qiaolian_dual_bots/data/qiaolian_dual_bot.db"
conn = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

tables = {r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table'")}

wanted = [
    "source_posts",
    "source_media",
    "canonical_records",
    "listings_v3",
    "listing_offers",
    "review_items",
    "publisher_auto_items_v3",
    "publication_instances",
    "publication_packages_v3",
    "collector_source_state_v3",
    "v3_component_status",
]

print("DB_COUNTS_BEGIN")
for table in wanted:
    if table in tables:
        count = cur.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"{table}={count}")
print("DB_COUNTS_END")

if "publisher_auto_items_v3" in tables:
    print("AUTO_STATES_BEGIN")
    for row in cur.execute("SELECT state, COUNT(*) AS n FROM publisher_auto_items_v3 GROUP BY state ORDER BY state"):
        print(f"{row['state']}={row['n']}")
    print("AUTO_STATES_END")

if "collector_source_state_v3" in tables:
    print("SOURCES_BEGIN")
    rows = cur.execute("SELECT source_name,enabled,last_collected_at,today_count,today_duplicates,last_error FROM collector_source_state_v3 ORDER BY source_name").fetchall()
    for row in rows:
        err = (row['last_error'] or '').replace('\n',' ')[:120]
        print(f"source={row['source_name']} enabled={row['enabled']} last={row['last_collected_at']} today={row['today_count']} dup={row['today_duplicates']} err={err}")
    print("SOURCES_END")

if "publisher_auto_items_v3" in tables:
    print("QUEUE_HEAD_BEGIN")
    rows = cur.execute("SELECT offer_id,listing_id,state,created_at,updated_at,published_at,reason_code FROM publisher_auto_items_v3 WHERE ignored=0 ORDER BY created_at DESC LIMIT 12").fetchall()
    for row in rows:
        print(dict(row))
    print("QUEUE_HEAD_END")

conn.close()
