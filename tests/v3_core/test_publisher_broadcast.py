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


def test_weekly_marketing_copy_matches_locked_v1():
    expected = {
        "mon": "📮 <b>侨联周承诺 · 周一</b>\n🔒 <b>真实房源承诺</b>\n• 顾问实地核实\n• 实拍不用网图\n• 费用提前问清\n• 已租及时下架\n到场不符，侨联负责沟通。",
        "tue": "📮 <b>侨联周承诺 · 周二</b>\n🔒 <b>押金保障承诺</b>\n入住前｜全屋拍照存档\n租期中｜维修争议介入协调\n退租时｜对照档案陪同核查\n有纠纷｜帮您与房东沟通\n六年，这个承诺没破过。",
        "wed": "📮 <b>侨联周承诺 · 周三</b>\n💡 <b>租房隐性成本清单</b>\n每套都会问清：\n• 水电：按表 / 固定？\n• 网络：含 / 自装？\n• 物业：房东 / 租客？\n• 停车：含 / 另收？\n• 门禁卡：押金？\n找房时每套都注明。",
        "thu": "📮 <b>侨联周承诺 · 周四</b>\n📹 <b>视频实拍代看</b>\n人不在金边，或没时间跑现场？\n提前告诉我们你在意什么：\n噪音大不大、外卖能不能上楼、\n家电新不新、采光好不好……\n约个时间，顾问替您到现场，\n开实时视频，想看哪就看哪，\n这些细节，我们替您现场把关。",
        "fri": "📮 <b>侨联周承诺 · 周五</b>\n🈚 <b>无中介费承诺</b>\n通过侨联租房：\n• 不向租客收中介费\n• 租金直接与房东签\n• 费用明细提前列清\n我们赚服务费，不赚信息差。",
        "sat": "📮 <b>侨联周承诺 · 周六</b>\n🏆 <b>六年本地服务承诺</b>\n金边本地6年：\n• 真实房源，实拍更新\n• 中文顾问，全程跟进\n• 视频代看，人不到也能选\n• 入住售后，租期内继续管\n六年，这个承诺没破过。",
        "sun": "📮 <b>侨联周承诺 · 周日</b>\n🛡️ <b>入住售后承诺</b>\n签约不是结束，入住才是开始：\n• 报修：工单登记，快速响应\n• 物业：代您沟通，不用自己跑\n• 水电：缴费协助，避免停水停电\n• 搬家保洁网络：需要就找侨联\n租期内，有问题都能找到人。",
    }
    assert {template.key: template.body for template in MARKETING_TEMPLATES} == expected


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
