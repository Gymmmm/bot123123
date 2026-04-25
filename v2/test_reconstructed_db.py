import os
import sys
import json
from datetime import datetime

# 模拟远程环境中的 Database 类（仅用于测试）
# 实际测试时，我们将此脚本上传并运行，它会导入远程服务器上的 db.py
def test_remote_db():
    # 假设我们已经将重构后的代码部署到了远程服务器的测试路径
    # 这里我们直接在远程服务器上运行此脚本
    try:
        from qiaolian_publisher_v2.db import Database
    except ImportError:
        print("Error: Could not import Database from qiaolian_publisher_v2.db")
        return

    db_path = "/opt/qiaolian_dual_bots/data/qiaolian_dual_bot.db"
    db = Database(db_path)
    
    print(f"Testing Database at {db_path}...")
    
    # 1. 测试创建草稿并获取 lastrowid
    draft_id = f"TEST_DRF_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    draft_data = {
        "draft_id": draft_id,
        "title": "Test Apartment",
        "price": 999,
        "highlights": ["Highlight 1", "Highlight 2"],
        "drawbacks": ["Drawback 1"],
        "review_status": "test_pending"
    }
    
    try:
        row_id = db.create_draft(draft_data)
        print(f"SUCCESS: Created draft, lastrowid = {row_id}")
        
        # 验证写入的数据
        with db._connect() as conn:
            row = conn.execute("SELECT * FROM drafts WHERE id = ?", (row_id,)).fetchone()
            if row:
                print(f"VERIFIED: draft_id in DB = {row['draft_id']}")
                print(f"VERIFIED: highlights in DB = {row['highlights']} (Type: {type(row['highlights'])})")
            else:
                print("FAILED: Could not find the inserted row.")
    except Exception as e:
        print(f"FAILED: create_draft error: {e}")

    # 2. 测试保存房源 (ON CONFLICT)
    listing_id = f"TEST_LST_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    listing_data = {
        "listing_id": listing_id,
        "title": "Test Listing",
        "price": 1500,
        "highlights": ["Luxury", "Quiet"],
        "images": ["img1.jpg", "img2.jpg"]
    }
    
    try:
        db.save_listing(listing_data)
        print(f"SUCCESS: Saved listing {listing_id}")
        
        # 验证写入的数据
        with db._connect() as conn:
            row = conn.execute("SELECT * FROM listings WHERE listing_id = ?", (listing_id,)).fetchone()
            if row:
                print(f"VERIFIED: price in DB = {row['price']} (Type: {type(row['price'])})")
                print(f"VERIFIED: images in DB = {row['images']}")
            else:
                print("FAILED: Could not find the inserted listing.")
    except Exception as e:
        print(f"FAILED: save_listing error: {e}")

if __name__ == "__main__":
    test_remote_db()
