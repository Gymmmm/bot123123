#!/usr/bin/env python3
import json, sqlite3
from pathlib import Path

sources_path = Path('/opt/qiaolian_v3/sources.json')
if not sources_path.exists():
    sources_path = Path('/opt/qiaolian_v3/runtime/sources.json')
obj = json.loads(sources_path.read_text(encoding='utf-8'))
items = obj.get('sources', obj) if isinstance(obj, dict) else obj
print('SOURCES_FILE', sources_path)
for s in items:
    if isinstance(s, dict):
        print('SOURCE', s.get('source_name'), 'entity_id=', s.get('entity_id'), 'enabled=', s.get('enabled', True))

db = '/opt/qiaolian_dual_bots/data/qiaolian_dual_bot.db'
c = sqlite3.connect(db)
c.row_factory = sqlite3.Row
tables = [r[0] for r in c.execute("select name from sqlite_master where type='table' and (name like '%source%' or name like '%publication%' or name like '%listing%' or name like '%draft%') order by name")]
print('TABLES', ','.join(tables))
for t in tables:
    cols = [r[1] for r in c.execute(f'pragma table_info({t})')]
    print('SCHEMA', t, ','.join(cols))
    if 'source_name' in cols:
        rows = [dict(r) for r in c.execute(f"select source_name,count(*) n from {t} group by source_name order by n desc")]
        print('COUNTS', t, rows)
    if 'source_post_id' in cols and 'created_at' in cols:
        select = [x for x in ['source_post_id','source_name','source_chat_id','source_message_id','grouped_id','created_at','status','listing_id','offer_id','draft_id'] if x in cols]
        where = " where source_name='zufang555'" if 'source_name' in cols else ''
        rows = [dict(r) for r in c.execute(f"select {','.join(select)} from {t}{where} order by created_at desc limit 8")]
        print('RECENT', t, rows)
c.close()
