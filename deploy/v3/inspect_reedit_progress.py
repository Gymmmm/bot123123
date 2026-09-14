#!/usr/bin/env python3
import sqlite3
DB='/opt/qiaolian_dual_bots/data/qiaolian_dual_bot.db'
c=sqlite3.connect(DB); c.row_factory=sqlite3.Row
rows=c.execute("""
select i.channel_message_id,i.offer_id,i.package_id,i.updated_at,p.status package_status
from publication_instances i join publication_packages_v3 p on p.package_id=i.package_id
where i.platform='telegram' and cast(coalesce(i.channel_message_id,'0') as integer)>=3211
order by cast(i.channel_message_id as integer)
""").fetchall()
print('ROWS',len(rows))
for r in rows: print(dict(r))
print('HOLD',c.execute("select count(*) from publication_packages_v3 where status='rebuild_hold'").fetchone()[0])
print('RECENT_READY',c.execute("select count(*) from publication_packages_v3 where created_at >= datetime('now','-20 minutes') and status='package_ready'").fetchone()[0])
print('RECENT_PUBLISHED',c.execute("select count(*) from publication_packages_v3 where created_at >= datetime('now','-20 minutes') and status='published'").fetchone()[0])
c.close()
