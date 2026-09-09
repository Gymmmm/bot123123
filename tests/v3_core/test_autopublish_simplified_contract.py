from __future__ import annotations

import asyncio
from datetime import datetime
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from v3_core.media.cover_styles import FINAL_COVER_STYLES, recommended_cover_style
from v3_core.publishing.autopilot import AutoPublishResult
from v3_core.publishing.autopilot_anomalies import (
    FinalAutoPublishRepository,
    FinalAutoPublishService,
)
from v3_core.publishing.channel_renderer import render_channel_caption
from v3_core.publishing.package_store import FrozenPackage
from v3_core.publishing.simple_admin import SimplePublisherAdminController
from v3_core.storage.bootstrap import REQUIRED_V3_TABLES, initialize_v3_storage
from v3_core.user_bot.telegram_start_handler import _render_invalid_link


CHANNEL_ID = "-1001234567890"


def _insert_listing(
    db: Path,
    *,
    suffix: str,
    facts_hash: str | None = None,
    rent: int = 800,
    deal_type: str = "rent",
    project: str = "炳发城",
    location: str = "BKK1",
    layout: str = "2房2卫",
    property_type: str = "公寓",
    inventory_status: str = "active",
    with_offer: bool = True,
    created_at: str = "2026-09-09 01:00:00",
) -> tuple[str, str, str]:
    canonical_id = f"CAN_{suffix}"
    listing_id = f"l_{suffix}"
    offer_id = f"OFF_{suffix}"
    review_id = f"REV_{suffix}"
    digest = facts_hash or f"hash_{suffix}"
    facts_json = (
        '{'
        f'"deal_type":"{deal_type}",'
        f'"monthly_rent_usd":{rent if rent > 0 else 0},'
        f'"project_name":"{project}",'
        f'"public_location_display":"{location}",'
        f'"layout":"{layout}",'
        f'"property_type":"{property_type}",'
        '"quality":{"blocking_flags":[]}'
        '}'
    )
    with sqlite3.connect(db) as conn:
        conn.execute(
            """INSERT INTO canonical_records
               (canonical_record_id,source_post_id,schema_version,deal_type,facts_json,facts_hash,
                quality_score,quality_json,review_status,created_at)
               VALUES (?,?, 'v3', ?,?,?,90,'{}','unreviewed',?)""",
            (canonical_id, f"src_{suffix}", deal_type, facts_json, digest, created_at),
        )
        conn.execute(
            """INSERT INTO listings_v3
               (listing_id,public_listing_id,canonical_record_id,project_name,property_type,
                public_location_display,layout,display_title,canonical_facts_hash,
                canonical_facts_schema,inventory_status,created_at,updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,'v3',?,?,?)""",
            (
                listing_id,
                f"QL-PP-A{int(suffix) % 8 + 2}B{int(suffix) % 7 + 2}",
                canonical_id,
                project,
                property_type,
                location,
                layout,
                f"{project} {layout}",
                digest,
                inventory_status,
                created_at,
                created_at,
            ),
        )
        if with_offer:
            conn.execute(
                """INSERT INTO listing_offers
                   (offer_id,listing_id,offer_type,currency,monthly_rent_usd,sale_price_usd,
                    offer_status,publication_policy,publishable,publish_block_reason,created_at,updated_at)
                   VALUES (?,?, 'rent','USD',?,NULL,'active','telegram_rent',0,'review_required',?,?)""",
                (offer_id, listing_id, rent, created_at, created_at),
            )
            review_offer = offer_id
        else:
            review_offer = ""
        conn.execute(
            """INSERT INTO review_items
               (review_id,canonical_record_id,listing_id,offer_id,review_status,review_score,
                review_note,created_at,updated_at)
               VALUES (?,?,?,?, 'pending',90,'',?,?)""",
            (review_id, canonical_id, listing_id, review_offer, created_at, created_at),
        )
        conn.commit()
    return listing_id, offer_id, review_id


def _service(repo: FinalAutoPublishRepository) -> FinalAutoPublishService:
    return FinalAutoPublishService(
        workflow=object(),
        repository=repo,
        channel_chat_id=CHANNEL_ID,
    )


def test_bootstrap_contains_all_autopublish_runtime_tables(tmp_path):
    db = tmp_path / "qiaolian.db"
    initialize_v3_storage(db)
    with sqlite3.connect(db) as conn:
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    expected = {
        "publisher_autopilot_settings_v3",
        "publisher_post_windows_v3",
        "publisher_auto_items_v3",
        "publisher_auto_versions_v3",
        "publisher_auto_validations_v3",
        "publisher_review_exceptions_v3",
        "publisher_autopilot_lock_v3",
        "v3_component_status",
        "collector_source_state_v3",
    }
    assert expected <= REQUIRED_V3_TABLES
    assert expected <= tables


def test_homepage_has_exactly_seven_operator_entries():
    keyboard = SimplePublisherAdminController.home_keyboard()
    labels = [button.text for row in keyboard.inline_keyboard for button in row]
    assert labels == [
        "➕ 新建房源",
        "🟢 运行状态",
        "📣 每日广播",
        "📊 今日统计",
        "🏠 房源状态",
        "🕒 发帖时段",
        "📡 采集源",
    ]
    forbidden = {
        "待审核",
        "待生成",
        "待批准",
        "待冻结",
        "待发送",
        "发布恢复中心",
        "选择封面和实拍",
    }
    assert not forbidden.intersection(labels)


def test_default_cover_style_policy():
    assert recommended_cover_style("公寓") == "right_price"
    assert recommended_cover_style("排屋") == "right_price"
    assert recommended_cover_style("别墅") == "black_gold"
    assert recommended_cover_style("Villa") == "black_gold"
    assert "classic_blue" in FINAL_COVER_STYLES
    assert "right_price" in FINAL_COVER_STYLES
    assert "black_gold" in FINAL_COVER_STYLES
    assert "white" not in FINAL_COVER_STYLES


def test_channel_caption_matches_final_format_and_hides_internal_ids():
    text = render_channel_caption(
        listing={
            "project_name": "炳发城",
            "property_type": "公寓",
            "layout": "2房2卫",
            "size_sqm": 95,
            "floor": "19",
            "inventory_status": "active",
            "listing_id": "l_999",
        },
        offer={
            "offer_type": "rent",
            "monthly_rent_usd": 800,
            "payment_terms": "押一付一",
            "contract_term": "一年起租",
            "offer_id": "OFF_SECRET",
        },
        public_listing_id="QL-PP-A2B3",
    )
    assert text == (
        "🏡 炳发城｜2房2卫\n\n"
        "💵 $800/月\n\n"
        "🏢 公寓\n\n"
        "📐 95㎡｜19楼\n\n"
        "🗝️ 押一付一｜一年起租\n\n"
        "🟢 当前可预约　QL-PP-A2B3"
    )
    assert "l_999" not in text
    assert "OFF_SECRET" not in text
    assert "#" not in text


def test_channel_caption_missing_fields_leave_no_empty_separators():
    text = render_channel_caption(
        listing={
            "project_name": "",
            "public_location_display": "BKK1",
            "property_type": "排屋",
            "layout": "3房",
            "size_sqm": None,
            "floor": "12",
            "inventory_status": "active",
        },
        offer={
            "offer_type": "rent",
            "monthly_rent_usd": 1200,
            "payment_terms": "",
            "contract_term": "一年起租",
        },
        public_listing_id="QL-PP-A2B3",
    )
    assert "🏡 BKK1｜3房" in text
    assert "🏘️ 排屋" in text
    assert "📐 12楼" in text
    assert "🗝️ 一年起租" in text
    assert "｜｜" not in text
    assert "｜\n" not in text
    assert "None" not in text
    assert "unknown" not in text.lower()
    assert "null" not in text.lower()


def test_strict_autopublish_blockers_cover_required_failures(tmp_path):
    db = tmp_path / "qiaolian.db"
    initialize_v3_storage(db)
    repo = FinalAutoPublishRepository(db)
    repo.ensure_defaults()
    service = _service(repo)
    good_cover = tmp_path / "cover.jpg"
    good_cover.write_bytes(b"jpg")
    good_media = SimpleNamespace(
        gallery_paths=(str(good_cover),) * 4,
        cover_source_path=str(good_cover),
    )
    base_item = {
        "offer_type": "rent",
        "monthly_rent_usd": 800,
        "project_name": "炳发城",
        "public_location_display": "BKK1",
        "layout": "2房",
        "inventory_status": "active",
    }
    base_facts = {"deal_type": "rent", "quality": {"blocking_flags": []}}
    assert service._strict_blockers(base_item, base_facts, good_media) == []

    item = dict(base_item, monthly_rent_usd=0)
    assert "missing_rent" in service._strict_blockers(item, base_facts, good_media)
    item = dict(base_item, project_name="", public_location_display="")
    assert "missing_location" in service._strict_blockers(item, base_facts, good_media)
    item = dict(base_item, layout="")
    assert "missing_listing_info" in service._strict_blockers(item, base_facts, good_media)
    short_media = SimpleNamespace(gallery_paths=(str(good_cover),) * 3, cover_source_path=str(good_cover))
    assert "insufficient_media" in service._strict_blockers(base_item, base_facts, short_media)
    missing_media = SimpleNamespace(gallery_paths=(str(good_cover),) * 4, cover_source_path=str(tmp_path / "missing.jpg"))
    assert "unreadable_media" in service._strict_blockers(base_item, base_facts, missing_media)
    sale_item = dict(base_item, offer_type="sale")
    assert service._strict_blockers(sale_item, {"deal_type": "sale"}, good_media) == ["sale_store_only"]


def test_missing_rent_no_offer_is_visible_as_human_exception(tmp_path):
    db = tmp_path / "qiaolian.db"
    initialize_v3_storage(db)
    _insert_listing(db, suffix="1", rent=0, with_offer=False)
    repo = FinalAutoPublishRepository(db)
    repo.ensure_defaults()
    repo.sync_candidates()
    rows = repo.exception_rows()
    assert len(rows) == 1
    assert rows[0]["reason_code"] == "missing_rent"
    assert rows[0]["reason_text"] == "缺少租金"
    assert rows[0]["offer_id"].startswith("review:")


def test_later_exact_duplicate_is_blocked_but_first_listing_is_kept(tmp_path):
    db = tmp_path / "qiaolian.db"
    initialize_v3_storage(db)
    first_listing, first_offer, _ = _insert_listing(
        db, suffix="1", facts_hash="same_hash", created_at="2026-09-09 01:00:00"
    )
    later_listing, later_offer, _ = _insert_listing(
        db, suffix="2", facts_hash="same_hash", created_at="2026-09-09 02:00:00"
    )
    repo = FinalAutoPublishRepository(db)
    repo.ensure_defaults()
    repo.sync_candidates()
    assert repo.probable_duplicate(first_listing, first_offer) is False
    assert repo.probable_duplicate(later_listing, later_offer) is True
    result = asyncio.run(_service(repo).process_one(bot=object(), force_offer_id=later_offer))
    assert result.status == "exception"
    assert result.reason_code == "duplicate_listing"
    assert result.reason_text == "疑似重复房源"


def test_unknown_delivery_state_blocks_automatic_retry(tmp_path):
    db = tmp_path / "qiaolian.db"
    initialize_v3_storage(db)
    listing_id, offer_id, _ = _insert_listing(db, suffix="1")
    repo = FinalAutoPublishRepository(db)
    repo.ensure_defaults()
    repo.sync_candidates()
    with sqlite3.connect(db) as conn:
        conn.execute(
            """INSERT INTO publication_delivery_attempts_v3
               (attempt_id,package_id,listing_id,offer_id,channel_chat_id,state)
               VALUES ('DLV_X','PKG_X',?,?,?,'unknown')""",
            (listing_id, offer_id, CHANNEL_ID),
        )
        conn.commit()
    result = asyncio.run(_service(repo).process_one(bot=object(), force_offer_id=offer_id))
    assert result.status == "exception"
    assert result.reason_code == "telegram_unknown"
    assert result.reason_text == "发送结果待确认，禁止重复发送"


def test_published_listing_is_not_sent_again_after_restart(tmp_path):
    db = tmp_path / "qiaolian.db"
    initialize_v3_storage(db)
    listing_id, offer_id, _ = _insert_listing(db, suffix="1")
    repo = FinalAutoPublishRepository(db)
    repo.ensure_defaults()
    repo.sync_candidates()
    with sqlite3.connect(db) as conn:
        conn.execute(
            """INSERT INTO publication_instances
               (instance_id,package_id,listing_id,offer_id,platform,channel_chat_id,
                channel_message_id,publish_status)
               VALUES ('PUB_X','PKG_X',?,?,'telegram',?,'7788','published')""",
            (listing_id, offer_id, CHANNEL_ID),
        )
        conn.commit()
    result = asyncio.run(_service(repo).process_one(bot=object(), force_offer_id=offer_id))
    assert result.status == "already_published"
    assert result.channel_message_id == "7788"
    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT COUNT(*) FROM publication_instances").fetchone()[0] == 1


def test_pause_resume_non_window_and_single_item_tick(tmp_path):
    db = tmp_path / "qiaolian.db"
    initialize_v3_storage(db)
    repo = FinalAutoPublishRepository(db)
    repo.ensure_defaults()

    class CountingService(FinalAutoPublishService):
        def __init__(self):
            super().__init__(workflow=object(), repository=repo, channel_chat_id=CHANNEL_ID)
            self.calls = 0

        async def process_one(self, *, bot, force_offer_id="", origin=None):
            self.calls += 1
            return AutoPublishResult("idle")

    service = CountingService()
    context = SimpleNamespace(bot=object())

    repo.set_enabled(False)
    repo.within_window = lambda: True
    asyncio.run(service.scheduled_tick(context))
    assert service.calls == 0

    repo.set_enabled(True)
    repo.within_window = lambda: False
    asyncio.run(service.scheduled_tick(context))
    assert service.calls == 0

    repo.within_window = lambda: True
    asyncio.run(service.scheduled_tick(context))
    assert service.calls == 1
    # One scheduler tick is deliberately allowed to dispatch only one item.
    assert service.calls == 1


def test_single_instance_publish_lock_is_durable(tmp_path):
    db = tmp_path / "qiaolian.db"
    initialize_v3_storage(db)
    repo = FinalAutoPublishRepository(db)
    repo.ensure_defaults()
    assert repo.acquire_lock("publisher-A", ttl_seconds=180) is True
    assert repo.acquire_lock("publisher-B", ttl_seconds=180) is False
    repo.release_lock("publisher-A")
    assert repo.acquire_lock("publisher-B", ttl_seconds=180) is True


def test_post_windows_and_pause_setting_survive_repository_restart(tmp_path, monkeypatch):
    db = tmp_path / "qiaolian.db"
    initialize_v3_storage(db)
    monkeypatch.setenv("V3_AUTO_PUBLISH_WINDOWS", "09:00-10:00")
    repo = FinalAutoPublishRepository(db)
    repo.ensure_defaults()
    assert [(row["start_time"], row["end_time"]) for row in repo.windows()] == [("09:00", "10:00")]
    repo.delete_window(repo.windows()[0]["window_id"])
    repo.set_enabled(False)

    restarted = FinalAutoPublishRepository(db)
    restarted.ensure_defaults()
    assert restarted.windows() == []
    assert restarted.config().enabled is False
    assert restarted.within_window(datetime(2026, 9, 9, 9, 30)) is False


def test_manual_publish_preview_has_one_confirmation_and_optional_cover_controls(tmp_path):
    cover = tmp_path / "cover.png"
    cover.write_bytes(b"cover")
    package = FrozenPackage(
        package_id="PKG_X",
        listing_id="l_1",
        offer_id="OFF_1",
        canonical_record_id="CAN_1",
        package_version=1,
        status="package_ready",
        cover_style="right_price",
        cover_path=str(cover),
        gallery=(str(cover),),
        post_text="最终预览",
        actions={},
        snapshot={},
        frozen_file_hashes={},
        source_identity={},
        public_token="qltoken",
        canonical_facts_hash="hash",
        content_hash="content",
    )

    class Message:
        def __init__(self):
            self.markup = None

        async def reply_photo(self, **kwargs):
            self.markup = kwargs["reply_markup"]

    controller = object.__new__(SimplePublisherAdminController)
    message = Message()
    asyncio.run(controller.send_manual_preview(message, package, review_id="REV_1", offer_id="OFF_1"))
    labels = [button.text for row in message.markup.inline_keyboard for button in row]
    assert labels == ["发布到频道", "更换封面图片", "更换封面模板", "🏠 返回首页"]
    for forbidden in ("审核事实", "批准发布包", "冻结", "待审核", "待发送"):
        assert forbidden not in labels


def test_invalid_link_copy_is_exact_and_has_three_recovery_buttons():
    class Message:
        def __init__(self):
            self.text = ""
            self.markup = None

        async def reply_text(self, text, **kwargs):
            self.text = text
            self.markup = kwargs["reply_markup"]

    message = Message()
    asyncio.run(_render_invalid_link(message))
    assert message.text == "这个链接已经失效或房源信息已更新。\n\n您可以重新找房，或直接联系我们。"
    labels = [button.text for row in message.markup.inline_keyboard for button in row]
    assert labels == ["🔍 帮我找房", "💬 联系我们", "🏠 返回首页"]
