from datetime import datetime

from qiaolian_dual.db import db


def now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


demo = [
    {
        "listing_id": "L0001",
        "title": "BKK1 The Peak 精装一居室",
        "property_type": "apartment",
        "area": "BKK1",
        "community": "The Peak",
        "price": 650,
        "layout": "1房1厅1卫",
        "size_sqm": "52",
        "tags": ["拎包入住", "泳池", "健身房"],
        "highlights": "步行到超市和咖啡店，楼下打车方便。",
        "hidden_costs": "押1付1，水电网按使用自理。",
        "drawbacks": "客厅不算大，空调比较普通。",
        "deposit_rule": "押1付1",
        "available_date": "随时入住",
        "media_file_id": "",
        "media_type": "",
        "status": "active",
        "created_at": now_text(),
        "updated_at": now_text(),
    },
    {
        "listing_id": "L0002",
        "title": "钻石岛 河景两居室",
        "property_type": "apartment",
        "area": "钻石岛",
        "community": "河景公寓",
        "price": 900,
        "layout": "2房2卫",
        "size_sqm": "85",
        "tags": ["河景", "阳台", "泳池"],
        "highlights": "采光好，适合情侣或小家庭。",
        "hidden_costs": "押1付1，停车另计。",
        "drawbacks": "晚高峰桥上会堵一点。",
        "deposit_rule": "押1付1",
        "available_date": "5月1日",
        "media_file_id": "",
        "media_type": "",
        "status": "active",
        "created_at": now_text(),
        "updated_at": now_text(),
    },
    {
        "listing_id": "L0003",
        "title": "富力城 2房整租",
        "property_type": "apartment",
        "area": "富力城",
        "community": "富力城华府",
        "price": 520,
        "layout": "2房1卫",
        "size_sqm": "68",
        "tags": ["中国房东", "停车方便"],
        "highlights": "性价比高，楼下商店多。",
        "hidden_costs": "押1付1，电费按表。",
        "drawbacks": "装修偏普通。",
        "deposit_rule": "押1付1",
        "available_date": "随时入住",
        "media_file_id": "",
        "media_type": "",
        "status": "active",
        "created_at": now_text(),
        "updated_at": now_text(),
    }
]

for item in demo:
    if db.get_listing(item["listing_id"]) is None:
        db.create_listing(item)

print("done", db.stats())
