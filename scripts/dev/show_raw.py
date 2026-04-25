#!/usr/bin/env python3
"""展示 source_posts 中采集到的原始数据"""
import sqlite3, os, json
from dotenv import load_dotenv
load_dotenv("/opt/qiaolian_dual_bots/.env")

db = os.getenv("DB_PATH", "data/qiaolian_dual_bot.db")
conn = sqlite3.connect(db)
conn.row_factory = sqlite3.Row

rows = conn.execute(
    "SELECT id, source_name, source_post_id, source_url, raw_text, raw_images_json, raw_videos_json, created_at FROM source_posts ORDER BY id LIMIT 20"
).fetchall()

for r in rows:
    imgs = []
    vids = []
    try:
        imgs = json.loads(r['raw_images_json'] or '[]')
    except:
        pass
    try:
        vids = json.loads(r['raw_videos_json'] or '[]')
    except:
        pass

    print(f"\n{'='*60}")
    print(f"ID={r['id']} | 频道={r['source_name']} | post_id={r['source_post_id']}")
    print(f"链接={r['source_url']}")
    print(f"图片数={len(imgs)} | 视频数={len(vids)}")
    if imgs:
        print(f"图片file_id: {imgs[0] if isinstance(imgs[0], str) else json.dumps(imgs[0])[:80]}")
    print(f"--- 原文 ---")
    print(r['raw_text'] or '（无文字）')

conn.close()
