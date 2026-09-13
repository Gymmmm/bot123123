#!/usr/bin/env python3
from __future__ import annotations
import asyncio, json, sqlite3, sys
from pathlib import Path
from telegram import Bot, InputMediaPhoto
from telegram.constants import ParseMode
from v3_core.publishing.admin_bot import load_settings
from v3_core.publishing.telegram_adapter import build_channel_keyboard

repo = Path(sys.argv[1])
tmp = Path(sys.argv[2])
sys.path.insert(0, str(repo / "media_pipeline"))
from cover_maker_gold_bottom import generate_cover

settings = load_settings()
conn = sqlite3.connect(settings.db_path)
conn.row_factory = sqlite3.Row
row = conn.execute('''
    SELECT p.channel_message_id AS _channel_message_id,
           p.package_id AS _package_id,p.listing_id AS _listing_id,p.offer_id AS _offer_id,
           k.cover_path AS _cover_path,k.gallery_json AS _gallery_json,
           k.post_text AS _post_text,k.actions_json AS _actions_json,
           o.monthly_rent_usd AS _monthly_rent_usd,
           l.*
    FROM publication_instances p
    JOIN publication_packages_v3 k ON k.package_id=p.package_id
    JOIN listings_v3 l ON l.listing_id=p.listing_id
    JOIN listing_offers o ON o.offer_id=p.offer_id
    WHERE p.platform='telegram' AND p.publish_status='published'
      AND o.offer_type='rent' AND o.offer_status='active'
    ORDER BY CAST(p.channel_message_id AS INTEGER) DESC
    LIMIT 1
''').fetchone()
conn.close()
if not row:
    raise SystemExit("PREVIEW_NO_PUBLISHED_LISTING")
data = dict(row)

def pick(*names, default=None):
    for n in names:
        v = data.get(n)
        if v not in (None, ""):
            return v
    return default

def collect_paths(obj):
    out = []
    if isinstance(obj, str): out.append(obj)
    elif isinstance(obj, list):
        for x in obj: out.extend(collect_paths(x))
    elif isinstance(obj, dict):
        for x in obj.values(): out.extend(collect_paths(x))
    return out

try:
    gallery_obj = json.loads(pick("_gallery_json", default="[]") or "[]")
except Exception:
    gallery_obj = []
seen, gallery_paths = set(), []
for raw in collect_paths(gallery_obj):
    p = Path(str(raw))
    if p.exists() and p.is_file() and p.suffix.lower() in {".jpg", ".jpeg", ".png", ".webp"}:
        key = str(p.resolve())
        if key not in seen:
            seen.add(key); gallery_paths.append(p)
cover_path = pick("_cover_path")
if cover_path:
    p = Path(str(cover_path))
    if p.exists() and p.is_file() and str(p.resolve()) not in seen:
        gallery_paths.append(p)
if not gallery_paths:
    raise SystemExit("PREVIEW_NO_EXISTING_IMAGE_PATH")
bg = gallery_paths[0]

bedrooms = pick("bedrooms", "bedroom_count")
bathrooms = pick("bathrooms", "bathroom_count")
ptype = pick("property_type", "property_type_display", default="房源")
layout = f"{bedrooms}房" if bedrooms else ""
if bedrooms and bathrooms: layout += f"{bathrooms}卫"
layout = layout or str(ptype)
title = str(pick("project_name", "community_name", "building_name", "title", default="金边房源"))
location = str(pick("canonical_area_display", "public_location_display", "area_name", "district_name", "location", "address", default="金边"))
area = pick("area_sqm", "size_sqm")
size = f"{int(float(area))}㎡" if area else "—"
floor_raw = pick("floor", "floor_text")
floor = str(floor_raw or "—")
if floor not in {"—", ""} and "楼" not in floor and "层" not in floor: floor += "楼"
rent_raw = pick("_monthly_rent_usd")
rent = int(float(rent_raw)) if rent_raw else "面议"
public_id = str(pick("public_listing_id", "public_id", default=pick("_listing_id")))
out = tmp / "channel_preview.jpg"
logo = repo / "media_pipeline/assets/qiaolian_logo_mark_gold.png"
generate_cover(str(bg), str(out), title=title, rent=rent, property_type=layout,
               location=location, area=size, floor=floor, tags=[], logo_path=str(logo))

actions = json.loads(pick("_actions_json", default="{}") or "{}")
inventory_status = str(pick("inventory_status", default="pending"))
keyboard = build_channel_keyboard(actions, inventory_status=inventory_status)
caption = str(pick("_post_text", default="")).strip()
if not caption:
    caption = f"🏡 {title}｜{layout}\n💵 ${rent}/月\n\n🔵 房态待确认　{public_id}"

async def send():
    bot = Bot(token=settings.token)
    await bot.initialize()
    try:
        with out.open("rb") as f:
            main = await bot.send_photo(chat_id=settings.channel_chat_id, photo=f,
                                        caption=caption, parse_mode=ParseMode.HTML,
                                        reply_markup=keyboard)
        print("FULL_PREVIEW_MAIN_SENT", main.message_id)
        extras = []
        excluded = {str(out.resolve()), str(bg.resolve())}
        if cover_path: excluded.add(str(Path(str(cover_path)).resolve()))
        for p in gallery_paths:
            if str(p.resolve()) not in excluded: extras.append(p)
        extras = extras[:9]
        if len(extras) >= 2:
            handles = []
            try:
                media = []
                for i, p in enumerate(extras):
                    h = p.open("rb"); handles.append(h)
                    media.append(InputMediaPhoto(media=h, caption=(f"📸 更多实拍｜{public_id}" if i == 0 else None)))
                msgs = await bot.send_media_group(chat_id=settings.channel_chat_id, media=media)
                print("FULL_PREVIEW_GALLERY_SENT", ",".join(str(m.message_id) for m in msgs), "count=", len(msgs))
            finally:
                for h in handles: h.close()
        elif len(extras) == 1:
            with extras[0].open("rb") as f:
                m = await bot.send_photo(chat_id=settings.channel_chat_id, photo=f, caption=f"📸 更多实拍｜{public_id}")
            print("FULL_PREVIEW_GALLERY_SENT", m.message_id, "count=1")
        else:
            print("FULL_PREVIEW_GALLERY_NONE")
    finally:
        await bot.shutdown()

asyncio.run(send())
print("FULL_PREVIEW_DONE", public_id, "source_msg=", pick("_channel_message_id"), "gallery_candidates=", len(gallery_paths))
