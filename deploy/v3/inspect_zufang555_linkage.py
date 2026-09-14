#!/usr/bin/env python3
import json, sqlite3

db='/opt/qiaolian_dual_bots/data/qiaolian_dual_bot.db'
c=sqlite3.connect(db); c.row_factory=sqlite3.Row
rows=c.execute("""
select i.channel_message_id,i.instance_id,i.package_id,i.listing_id,i.offer_id,
       p.source_identity_json,p.snapshot_json
from publication_instances i
join publication_packages_v3 p on p.package_id=i.package_id
where i.platform='telegram' and cast(coalesce(i.channel_message_id,'0') as integer)>0
order by cast(i.channel_message_id as integer)
""").fetchall()
print('PUB_COUNT',len(rows))
counts={}
for r in rows:
    src={}
    try: src=json.loads(r['source_identity_json'] or '{}')
    except Exception: pass
    name=str(src.get('source_name') or src.get('name') or '')
    post=str(src.get('source_post_id') or src.get('post_id') or src.get('message_id') or '')
    counts[name]=counts.get(name,0)+1
    print('PUB',r['channel_message_id'],'listing=',r['listing_id'],'source=',name,'source_post_id=',post)
print('SOURCE_COUNTS',counts)
c.close()
