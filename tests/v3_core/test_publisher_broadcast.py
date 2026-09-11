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
from v3_core.storage.bootstrap import initialize_v3_storage


class _FakeLiveBuilder:
    def build(self, *, fx_offset=-0.20, now=None):
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
    assert config.fx_offset == -0.20
    assert config.button_key == "none"
    assert service.body() == "live:-0.20"


def test_footer_buttons_use_live_v3_start_shortcuts(tmp_path):
    service = _service(tmp_path)
    service.set_button("combo")
    rows = service.footer_rows()
    assert [[button.label for button in row] for row in rows] == [
        ["🔍 租房找房", "🏠 查看最新房源"],
        ["💬 租房置业咨询"],
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
    assert "本周找房提醒" in service.body()
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
    # Even without mark_scheduled_sent(), the attempt claim blocks another
    # automatic send after an ambiguous Telegram result.
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
    assert "小雨 25–32℃｜降雨 45%" in body
    assert "1 USD ≈ 7.00 CNY" in body
    assert "今天有阵雨，出门记得带伞。" in body


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
    )
    bot = V3PublisherApplication(settings)
    buttons = [button for row in bot._dashboard_keyboard().inline_keyboard for button in row]
    broadcast = next(button for button in buttons if button.text == "📢 广播中心")
    assert broadcast.callback_data == "v3bc"
