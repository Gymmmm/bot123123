from __future__ import annotations

from datetime import datetime
from pathlib import Path
import sqlite3
from zoneinfo import ZoneInfo

import pytest

from v3_core.publishing.admin_bot import PublisherAdminSettings
from v3_core.publishing.broadcast import (
    BroadcastService,
    BroadcastSettingsRepository,
    LiveDailyInfoBuilder,
    parse_hhmm,
)
from v3_core.publishing.publisher_app import V3PublisherApplication
from v3_core.publishing.marketing_broadcast import (
    MarketingBroadcastService,
    TEMPLATES as MARKETING_TEMPLATES,
)
from v3_core.storage.bootstrap import initialize_v3_storage


class _FakeLiveBuilder:
    def build(self, *, fx_offset=0.0, now=None):
        return f"live:{fx_offset:+.2f}"


def _service(tmp_path: Path) -> BroadcastService:
    db = tmp_path / "qiaolian.db"
    initialize_v3_storage(db)
    service = BroadcastService(
        repository=BroadcastSettingsRepository(db),
        repo_root=Path(__file__).resolve().parents[2],
        user_bot_username="qiaolian_rent_bot",
        live_builder=_FakeLiveBuilder(),
    )
    service.ensure_defaults()
    return service


def test_parse_hhmm_is_strict():
    assert parse_hhmm("09:30") == (9, 30)
    assert parse_hhmm("23:59") == (23, 59)
    assert parse_hhmm("9:30") is None
    assert parse_hhmm("24:00") is None
    assert parse_hhmm("12:60") is None


def test_marketing_template_button_metadata_matches_locked_v1_ctas():
    expected = {
        "mon": (("🔍 开始找房", "find"), ("💬 中文顾问", "advisor"), ("📢 最新房源", "latest")),
        "tue": (("💬 中文顾问", "advisor"), ("🛎️ 侨联服务", "service"), ("🔍 开始找房", "find")),
        "wed": (("🔍 开始找房", "find"), ("💬 中文顾问", "advisor")),
        "thu": (("💬 中文顾问", "advisor"), ("🔍 开始找房", "find")),
        "fri": (("💬 中文顾问", "advisor"), ("🔍 开始找房", "find")),
        "sat": (("🔍 开始找房", "find"), ("🛎️ 侨联服务", "service"), ("💬 中文顾问", "advisor")),
        "sun": (("🛎️ 侨联服务", "service"), ("💬 中文顾问", "advisor")),
    }
    assert {template.key: template.buttons for template in MARKETING_TEMPLATES} == expected


def test_weekly_marketing_copy_is_safe_when_public_inventory_is_empty(tmp_path):
    db = tmp_path / "marketing-safe.db"
    initialize_v3_storage(db)
    service = MarketingBroadcastService(db, user_bot_username="qiaolian_rent_bot")
    bodies = [service.template(day).body for day in range(7)]
    joined = "\n".join(bodies)
    assert "六年，这个承诺没破过" not in joined
    assert "我们赚服务费" not in joined
    assert "顾问实地核实" not in joined
    assert "公开" in joined
    assert "年租" in joined
    assert "金边" in joined
    assert "中国人在金边" in joined
    assert "短租" not in joined
    assert "日租" not in joined


def test_marketing_uses_only_durably_published_rent_inventory(tmp_path):
    db = tmp_path / "marketing-data.db"
    initialize_v3_storage(db)
    with sqlite3.connect(db) as conn:
        conn.execute("INSERT INTO canonical_records (canonical_record_id,source_post_id,schema_version,deal_type,facts_json,facts_hash) VALUES ('CAN_MKT','1','canonical_facts.v1','rent','{}','hash-mkt')")
        conn.execute("""INSERT INTO listings_v3
            (listing_id,public_listing_id,canonical_record_id,property_type,public_location_display,
             canonical_area_display,layout,display_title,canonical_facts_hash,canonical_facts_schema,
             inventory_status,data_status)
            VALUES ('L_MKT','QL-MKT-01','CAN_MKT','公寓','BKK1','BKK1','1房1卫','BKK1 1房',
                    'hash-mkt','canonical_facts.v1','active','current')""")
        conn.execute("INSERT INTO listing_offers (offer_id,listing_id,offer_type,monthly_rent_usd,offer_status,publication_policy,publishable) VALUES ('O_MKT','L_MKT','rent',650,'active','telegram_rent',1)")
        conn.execute("""INSERT INTO publication_packages_v3
            (package_id,listing_id,offer_id,canonical_record_id,package_version,status,cover_style,cover_path,
             gallery_json,post_text,actions_json,snapshot_json,frozen_file_hashes_json,source_identity_json,
             public_token,canonical_facts_hash,content_hash)
            VALUES ('PKG_MKT','L_MKT','O_MKT','CAN_MKT',1,'published','none','','[]','','{}','{}','{}','{}',
                    'tok','hash-mkt','content-mkt')""")
        conn.execute("""INSERT INTO publication_instances
            (instance_id,package_id,listing_id,offer_id,platform,channel_chat_id,channel_message_id,publish_status,published_at)
            VALUES ('PI_MKT','PKG_MKT','L_MKT','O_MKT','telegram','-1001','101','published',CURRENT_TIMESTAMP)""")
        conn.commit()
    service = MarketingBroadcastService(db, user_bot_username="qiaolian_rent_bot")
    monday = service.template(0).body
    tuesday = service.template(1).body
    assert "当前公开有效：<b>1 套</b>" in monday
    assert "BKK1：1 套" in monday
    assert "年租房源月租展示：$650–$650/月" in monday
    assert "$500–700：1 套" in tuesday
    assert "当前公开年租库存共 1 套" in tuesday
    assert "全金边市场均价" in tuesday


def test_marketing_runtime_button_rows_and_urls_match_locked_v1(tmp_path, monkeypatch):
    db = tmp_path / "marketing.db"
    initialize_v3_storage(db)
    monkeypatch.setenv("CHANNEL_USERNAME", "qiaolian_channel")
    service = MarketingBroadcastService(db, user_bot_username="qiaolian_rent_bot")

    expected_labels = {
        "mon": [["🔍 开始找房", "💬 中文顾问"], ["📢 最新房源"]],
        "tue": [["💬 中文顾问", "🛎️ 侨联服务"], ["🔍 开始找房"]],
        "wed": [["🔍 开始找房", "💬 中文顾问"]],
        "thu": [["💬 中文顾问", "🔍 开始找房"]],
        "fri": [["💬 中文顾问", "🔍 开始找房"]],
        "sat": [["🔍 开始找房", "🛎️ 侨联服务"], ["💬 中文顾问"]],
        "sun": [["🛎️ 侨联服务", "💬 中文顾问"]],
    }
    expected_urls = {
        "🔍 开始找房": "https://t.me/qiaolian_rent_bot?start=find",
        "💬 中文顾问": "https://t.me/qiaolian_rent_bot?start=advisor",
        "🛎️ 侨联服务": "https://t.me/qiaolian_rent_bot?start=service",
        "📢 最新房源": "https://t.me/qiaolian_channel",
    }

    for template in MARKETING_TEMPLATES:
        rows = service.footer_rows(template)
        assert [[button.label for button in row] for row in rows] == expected_labels[template.key]
        for row in rows:
            for button in row:
                assert button.url == expected_urls[button.label]
                assert "↗" not in button.label


def test_missing_database_is_not_created(tmp_path):
    db = tmp_path / "missing.db"
    repo = BroadcastSettingsRepository(db)
    with pytest.raises(sqlite3.OperationalError):
        repo.ensure_defaults()
    assert not db.exists()


def test_defaults_are_safe_and_disabled(tmp_path):
    service = _service(tmp_path)
    config = service.config()
    assert config.enabled is False
    assert config.send_time == "09:30"
    assert config.template_key == "live"
    assert config.fx_offset == 0.0
    assert config.button_key == "none"
    assert service.body() == "live:+0.00"


def test_footer_buttons_use_live_v3_start_shortcuts(tmp_path):
    service = _service(tmp_path)
    service.set_button("combo")
    rows = service.footer_rows()
    assert [[button.label for button in row] for row in rows] == [
        ["🔍 帮我找房", "🏠 最新房源"],
        ["💬 联系中文顾问"],
    ]
    urls = [button.url for row in rows for button in row]
    assert urls == [
        "https://t.me/qiaolian_rent_bot?start=find_home",
        "https://t.me/qiaolian_rent_bot?start=latest",
        "https://t.me/qiaolian_rent_bot?start=advisor",
    ]


def test_static_and_custom_templates_are_explicit(tmp_path):
    service = _service(tmp_path)
    service.set_template("weekly")
    assert "金边年租找房" in service.body()
    assert "租金按月展示" in service.body()
    service.set_custom_html("<b>自定义广播</b>")
    assert service.config().template_key == "custom"
    assert service.body() == "<b>自定义广播</b>"


def test_schedule_claims_before_network_and_never_auto_retries_same_day(tmp_path):
    service = _service(tmp_path)
    service.set_enabled(True)
    service.set_time("09:30")
    now = datetime(2026, 9, 9, 9, 30, 10, tzinfo=ZoneInfo("Asia/Phnom_Penh"))
    claimed, local_date = service.claim_scheduled_due(now)
    assert claimed is True
    assert local_date == "2026-09-09"
    claimed_again, _ = service.claim_scheduled_due(now)
    assert claimed_again is False
    assert service.config().last_scheduled_attempt_date == "2026-09-09"
    assert service.config().last_scheduled_sent_date == ""


def test_schedule_does_not_claim_wrong_minute_or_when_disabled(tmp_path):
    service = _service(tmp_path)
    now = datetime(2026, 9, 9, 9, 30, tzinfo=ZoneInfo("Asia/Phnom_Penh"))
    assert service.claim_scheduled_due(now)[0] is False
    service.set_enabled(True)
    assert service.claim_scheduled_due(now.replace(minute=29))[0] is False


def test_live_builder_preserves_weather_and_fx_contract(tmp_path):
    root = Path(__file__).resolve().parents[2]

    def fetch_json(url: str):
        if "open-meteo" in url:
            return {
                "daily": {
                    "weather_code": [61],
                    "temperature_2m_max": [32.0],
                    "temperature_2m_min": [25.0],
                    "precipitation_probability_max": [45],
                }
            }
        return {"rates": {"CNY": 7.20}}

    builder = LiveDailyInfoBuilder(repo_root=root, fetch_json=fetch_json)
    body = builder.build(
        fx_offset=-0.20,
        now=datetime(2026, 9, 9, 8, 0, tzinfo=ZoneInfo("Asia/Phnom_Penh")),
    )
    assert "侨联地产｜早安金边" in body
    assert "2026.09.09" in body
    assert "🌦 今日天气" in body
    assert "小雨 25–32℃｜降雨 45%" in body
    assert "💱 今日汇率" in body
    assert "1 USD ≈ 7.00 CNY" in body
    assert "100 USD ≈ 700 CNY" in body
    assert "📌 今日提醒" in body
    assert "今天有阵雨，出门记得带伞。" in body
    assert "金边年租找房" in body
    assert "租金按月展示" in body
    assert "常去地点 / 区域 + 月预算 + 户型 + 入住时间" in body


def test_publisher_dashboard_exposes_broadcast_center(tmp_path):
    db = tmp_path / "qiaolian.db"
    initialize_v3_storage(db)
    settings = PublisherAdminSettings(
        token="123456:TEST_TOKEN",
        admin_ids=frozenset({1001}),
        db_path=str(db),
        user_bot_username="qiaolian_rent_bot",
        channel_chat_id="-1001234567890",
        cover_output_dir=str(tmp_path / "covers"),
        advisor_url="https://t.me/qiaolian_advisor",
    )
    bot = V3PublisherApplication(settings)
    buttons = [button for row in bot._dashboard_keyboard().inline_keyboard for button in row]
    broadcast = next(button for button in buttons if button.text == "📢 发布中心")
    assert broadcast.callback_data == "v3bc"
