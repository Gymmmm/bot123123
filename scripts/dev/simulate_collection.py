
import json
import os
import hashlib
from db import DatabaseManager

# Configuration
DB_PATH = os.getenv("DB_PATH", "data/qiaolian_dual_bot.db")

def simulate_collection(db_path):
    db_manager = DatabaseManager(db_path)
    
    # Scraped data from Telegram channel
    listings = [
        {
            "source_name": "jinbianfangchanzushou",
            "source_post_id": "18",
            "source_url": "https://t.me/jinbianfangchanzushou/18",
            "raw_text": "#永旺一 附近公寓出租\n户型：3室一厅\n租金：1500$\n面积：115平方米\n楼层：15楼\n合同：一年，押一付一\n包括：物业管理费，游泳池，健身房，网络，打扫卫生",
            "raw_images": ["https://cdn4.telegram-cdn.org/file/dummy_image_1.jpg"],
            "source_author": "金边租房售房最全房源公寓别墅大平层土地优选"
        },
        {
            "source_name": "jinbianfangchanzushou",
            "source_post_id": "22",
            "source_url": "https://t.me/jinbianfangchanzushou/22",
            "raw_text": "一号路炳发城联排别墅出租 #联排别墅出租 #一号路炳发城\n出租价格：1200\n户型情况：4房5卫\n家具家电：齐全\n建筑面积：5米 X 12米",
            "raw_images": ["https://cdn4.telegram-cdn.org/file/dummy_image_2.jpg"],
            "source_author": "金边租房售房最全房源公寓别墅大平层土地优选"
        },
        {
            "source_name": "jinbianfangchanzushou",
            "source_post_id": "29",
            "source_url": "https://t.me/jinbianfangchanzushou/29",
            "raw_text": "香格里拉2+1房出租，900$包物业费 压一付一",
            "raw_images": ["https://cdn4.telegram-cdn.org/file/dummy_image_3.jpg"],
            "source_author": "金边租房售房最全房源公寓别墅大平层土地优选"
        }
    ]
    
    for item in listings:
        # Create a unique dedupe hash
        dedupe_str = f"telegram_channel|{item['source_name']}|{item['source_post_id']}"
        dedupe_hash = hashlib.md5(dedupe_str.encode()).hexdigest()
        
        try:
            post_id = db_manager.save_source_post(
                source_id=1,
                source_type='telegram_channel',
                source_name=item['source_name'],
                source_post_id=item['source_post_id'],
                source_url=item['source_url'],
                source_author=item['source_author'],
                raw_text=item['raw_text'],
                raw_images_json=item['raw_images'],
                raw_videos_json=[],
                raw_contact='',
                raw_meta_json={},
                dedupe_hash=dedupe_hash,
                parse_status='pending'
            )
            print(f"Simulated Collection: Saved source post {item['source_post_id']} with DB ID: {post_id}")
        except Exception as e:
            print(f"Simulated Collection: Failed to save post {item['source_post_id']}: {e}")

if __name__ == '__main__':
    simulate_collection(DB_PATH)
