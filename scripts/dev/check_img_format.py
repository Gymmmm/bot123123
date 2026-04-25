#!/usr/bin/env python3
"""检查 raw_images_json 和 raw_videos_json 的实际数据格式"""
import sqlite3, os, json
from dotenv import load_dotenv
load_dotenv("/opt/qiaolian_dual_bots/.env")

db = os.getenv("DB_PATH", "data/qiaolian_dual_bot.db")
conn = sqlite3.connect(db)
conn.row_factory = sqlite3.Row

rows = conn.execute(
    "SELECT id, source_name, source_post_id, raw_images_json, raw_videos_json, raw_meta_json "
    "FROM source_posts WHERE raw_images_json IS NOT NULL AND raw_images_json != '[]' LIMIT 10"
).fetchall()

print("=== raw_images_json 格式样本 ===\n")
for r in rows:
    print(f"ID={r['id']} source_name={r['source_name']} post_id={r['source_post_id']}")
    try:
        imgs = json.loads(r['raw_images_json'])
        print(f"  图片数量: {len(imgs)}")
        for i, img in enumerate(imgs[:3]):
            print(f"  [{i}] type={type(img).__name__} value={json.dumps(img, ensure_ascii=False)[:120]}")
    except Exception as e:
        print(f"  解析失败: {e}, raw={r['raw_images_json'][:100]}")
    
    if r['raw_meta_json']:
        try:
            meta = json.loads(r['raw_meta_json'])
            print(f"  meta keys: {list(meta.keys())[:10]}")
        except:
            pass
    print()

# 视频格式
print("\n=== raw_videos_json 格式样本 ===\n")
vrows = conn.execute(
    "SELECT id, source_name, source_post_id, raw_videos_json "
    "FROM source_posts WHERE raw_videos_json IS NOT NULL AND raw_videos_json != '[]' LIMIT 5"
).fetchall()
for r in vrows:
    print(f"ID={r['id']} post_id={r['source_post_id']}")
    try:
        vids = json.loads(r['raw_videos_json'])
        print(f"  视频数量: {len(vids)}")
        for i, v in enumerate(vids[:2]):
            print(f"  [{i}] type={type(v).__name__} value={json.dumps(v, ensure_ascii=False)[:120]}")
    except Exception as e:
        print(f"  解析失败: {e}")
    print()

conn.close()
