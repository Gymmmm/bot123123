#!/usr/bin/env python3
import sqlite3, os, json
from dotenv import load_dotenv
load_dotenv("/opt/qiaolian_dual_bots/.env")

db = os.getenv("DB_PATH", "data/qiaolian_dual_bot.db")
conn = sqlite3.connect(db)
conn.row_factory = sqlite3.Row

# 取一条有 normalized_data 的 pending draft
row = conn.execute(
    "SELECT draft_id, title, normalized_data FROM drafts WHERE review_status='pending' AND normalized_data IS NOT NULL LIMIT 1"
).fetchone()

if row:
    nd = json.loads(row["normalized_data"])
    tg_post = nd.get("tg_post", "")
    print(f"draft_id: {row['draft_id']}")
    print(f"title: {row['title']}")
    print(f"tg_post 存在: {bool(tg_post)}")
    print(f"normalized_data keys: {list(nd.keys())}")
    if tg_post:
        print(f"\ntg_post 全文:\n{tg_post}")
    else:
        print("\n没有 tg_post 字段，这批数据是旧 prompt 解析的")
        print("需要重新解析或手动补充 tg_post")
else:
    print("没有带 normalized_data 的 pending draft")

conn.close()
