#!/usr/bin/env python3
"""全面检查数据库中的数据情况"""
import sqlite3, os, json
from dotenv import load_dotenv
load_dotenv("/opt/qiaolian_dual_bots/.env")

db = os.getenv("DB_PATH", "data/qiaolian_dual_bot.db")
conn = sqlite3.connect(db)
conn.row_factory = sqlite3.Row

print("=" * 60)
print("【source_posts 采集情况】")
print("=" * 60)

# 总体统计
rows = conn.execute("SELECT parse_status, COUNT(*) as cnt FROM source_posts GROUP BY parse_status").fetchall()
for r in rows:
    print(f"  parse_status={r['parse_status']}: {r['cnt']} 条")

# 图片情况
total = conn.execute("SELECT COUNT(*) FROM source_posts").fetchone()[0]
has_img = conn.execute("SELECT COUNT(*) FROM source_posts WHERE raw_images_json IS NOT NULL AND raw_images_json != '[]' AND raw_images_json != ''").fetchone()[0]
has_vid = conn.execute("SELECT COUNT(*) FROM source_posts WHERE raw_videos_json IS NOT NULL AND raw_videos_json != '[]' AND raw_videos_json != ''").fetchone()[0]
has_text = conn.execute("SELECT COUNT(*) FROM source_posts WHERE raw_text IS NOT NULL AND raw_text != ''").fetchone()[0]

print(f"\n  总计: {total} 条")
print(f"  有文字: {has_text} 条")
print(f"  有图片(raw_images_json): {has_img} 条")
print(f"  有视频(raw_videos_json): {has_vid} 条")

# 查看几条样本的图片字段
print("\n  【图片字段样本（前5条）】")
samples = conn.execute("SELECT id, source_name, raw_images_json, raw_text FROM source_posts LIMIT 5").fetchall()
for s in samples:
    imgs = s['raw_images_json'] or '[]'
    try:
        img_list = json.loads(imgs)
        img_count = len(img_list)
    except:
        img_count = 0
    text_preview = (s['raw_text'] or '')[:50].replace('\n', ' ')
    print(f"  id={s['id']} [{s['source_name']}] 图片数={img_count} 文字={text_preview}...")

# media_assets 情况
print("\n" + "=" * 60)
print("【media_assets 情况】")
print("=" * 60)
ma_total = conn.execute("SELECT COUNT(*) FROM media_assets").fetchone()[0]
ma_cover = conn.execute("SELECT COUNT(*) FROM media_assets WHERE is_cover=1").fetchone()[0]
ma_source = conn.execute("SELECT COUNT(*) FROM media_assets WHERE owner_type='source_post'").fetchone()[0]
ma_draft = conn.execute("SELECT COUNT(*) FROM media_assets WHERE owner_type='draft'").fetchone()[0]
print(f"  总计: {ma_total} 条")
print(f"  封面图(is_cover=1): {ma_cover} 条")
print(f"  来自采集(owner_type=source_post): {ma_source} 条")
print(f"  来自draft(owner_type=draft): {ma_draft} 条")

# 查看媒体文件是否实际存在
print("\n  【媒体文件实际存在情况（前10条）】")
ma_rows = conn.execute("SELECT local_path, media_type, is_cover FROM media_assets LIMIT 10").fetchall()
for m in ma_rows:
    path = m['local_path'] or ''
    exists = os.path.exists(path) if path else False
    print(f"  {'✓' if exists else '✗'} {path} [{m['media_type']}] cover={m['is_cover']}")

# drafts 字段完整性
print("\n" + "=" * 60)
print("【drafts 字段完整性（130条 pending）】")
print("=" * 60)

fields = ['title', 'project', 'area', 'price', 'layout', 'size', 'floor',
          'deposit', 'furniture', 'amenities', 'highlights', 'advisor_comment',
          'normalized_data', 'listing_id', 'source_post_id']

for field in fields:
    try:
        cnt = conn.execute(
            f"SELECT COUNT(*) FROM drafts WHERE review_status='pending' AND {field} IS NOT NULL AND {field} != '' AND {field} != '[]'"
        ).fetchone()[0]
        print(f"  {field}: {cnt}/130 有值")
    except Exception as e:
        print(f"  {field}: 查询失败 {e}")

# 查看一条完整 draft 样本
print("\n  【完整 draft 样本（第1条）】")
d = conn.execute("SELECT * FROM drafts WHERE review_status='pending' LIMIT 1").fetchone()
if d:
    for key in d.keys():
        val = d[key]
        if val and len(str(val)) > 80:
            val = str(val)[:80] + "..."
        print(f"  {key}: {val}")

conn.close()
