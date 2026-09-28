from __future__ import annotations

import asyncio
import sqlite3
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import autopilot_publish_bot
from meihua_publisher import build_chinese_listing_post, build_discussion_detail_text
from qiaolian_dual.canonical_facts import canonicalize_source, draft_projection
from test_caption_variant_queue import _add_approved_package, _add_bot_settings
from test_media_consistency import _init_db, _seed_draft
from v2.qiaolian_publisher_v2.keyboards import admin_menu


def test_discount_and_discussion_details_stay_in_canonical_facts() -> None:
    facts = canonicalize_source(
        """项目：The Peak
区域：BKK1
物业类型：公寓
2房1厅
原价：$750/月
现价：$650/月
面积：75㎡
楼层：22楼
租期：1年
押1付1
入住：随时
管理费：包含
网络：包含
水费：$0.50/m³
电费：$0.25/kWh
停车：免费
家具家电齐全 泳池 健身房 24小时安保
看房时间：每天 09:00-18:00
视频看房：可以安排"""
    )
    assert facts["monthly_rent_usd"] == 650
    assert facts["original_monthly_rent_usd"] == 750
    assert facts["price_status"] == "confirmed"

    projection = draft_projection(facts)
    projection["listing_id"] = "l_315"
    caption = build_chinese_listing_post(projection)
    detail = build_discussion_detail_text(projection)

    assert "<s>$750</s>" in caption
    assert "<b>$650/月</b>" in caption
    assert "管理费｜包含" in detail
    assert "水费｜$0.50/m³" in detail
    assert "24H安保" in detail
    assert "看房时间｜每天 09:00-18:00" in detail
    assert not any(token in detail for token in ("___", "【", "】", "待确认"))


def test_unlabelled_two_rents_are_blocked_instead_of_guessed() -> None:
    facts = canonicalize_source("区域：BKK1\n公寓出租 1房\n$750/月 $650/月")
    assert facts["monthly_rent_usd"] is None
    assert facts["price_status"] == "conflict"
    assert "conflicting_rental_price" in facts["quality"]["blocking_flags"]


def test_each_property_family_has_one_default_layout() -> None:
    common = {"listing_id": "l_88", "area": "BKK1", "layout": "2房", "price": 680}
    apartment = build_chinese_listing_post({**common, "property_type": "公寓"})
    villa = build_chinese_listing_post({**common, "property_type": "别墅"})
    office = build_chinese_listing_post({**common, "property_type": "办公室"})

    assert apartment.startswith("<b>🏠")
    assert villa.startswith("📍 <b>")
    assert office.startswith("🏢 <b>")
    for caption in (apartment, villa, office):
        assert "BKK1" in caption and "$680/月" in caption and "2房" in caption
        assert len(caption) <= 1024
        for tag in ("b", "i", "u", "s", "code", "a"):
            assert caption.count(f"<{tag}") == caption.count(f"</{tag}>")


def test_admin_menu_and_send_command_expose_clickable_approved_queue() -> None:
    callbacks = {button.callback_data for row in admin_menu().inline_keyboard for button in row}
    assert {"cmd:pending", "cmd:quality", "cmd:dashboard", "cmd:send_queue"} <= callbacks

    with tempfile.TemporaryDirectory() as tmp:
        db_path = str(Path(tmp) / "queue.db")
        _init_db(db_path)
        _add_bot_settings(db_path)
        _seed_draft(db_path, tmp=Path(tmp), review_status="approved")
        _add_approved_package(db_path, "c")
        with sqlite3.connect(db_path) as conn:
            conn.execute("UPDATE drafts SET listing_id='l_10' WHERE draft_id='DRF_TEST'")
            conn.commit()

        message = SimpleNamespace(reply_text=AsyncMock())
        update = SimpleNamespace(
            effective_user=SimpleNamespace(id=123),
            effective_message=message,
            message=message,
        )
        context = SimpleNamespace(args=[])
        old_db, old_admins = autopilot_publish_bot.DB_PATH, autopilot_publish_bot.ADMIN_IDS
        try:
            autopilot_publish_bot.DB_PATH = db_path
            autopilot_publish_bot.ADMIN_IDS = {123}
            asyncio.run(autopilot_publish_bot.cmd_send(update, context))
        finally:
            autopilot_publish_bot.DB_PATH = old_db
            autopilot_publish_bot.ADMIN_IDS = old_admins

        call = message.reply_text.await_args
        assert "已审核待发布" in call.args[0]
        keyboard = call.kwargs["reply_markup"]
        assert keyboard.inline_keyboard[0][0].callback_data == "ap:nc:10"
        from qiaolian_dual.public_listing_id import public_listing_id
        assert public_listing_id("l_10") in keyboard.inline_keyboard[0][0].text
