from pathlib import Path


def test_admin_menu_has_daily_broadcast_entry():
    source = Path("v2/qiaolian_publisher_v2/keyboards.py").read_text(encoding="utf-8")
    assert "📢 每日广播" in source
    assert "cmd:daily" in source


def test_production_runner_installs_locked_daily_contract():
    source = Path("v2/run_publisher_bot_v2.py").read_text(encoding="utf-8")
    assert "install_daily_broadcast_patch" in source
    assert "install_daily_broadcast_patch()" in source


def test_locked_daily_contract_has_only_live_weather_and_usd_cny():
    source = Path("v2/qiaolian_publisher_v2/daily_broadcast_patch.py").read_text(encoding="utf-8")
    assert "日期 + 金边实时天气 + USD/CNY" in source
    assert "scheduled_daily_broadcast" in source
    assert "_fetch_phnom_penh_daily_info" in source
    assert "daily:weekly" in source  # legacy callbacks are explicitly disabled
    assert "这个旧广播选项已经停用" in source
    assert "KHR、新闻、周历内容" in source
    assert "实时 USD/CNY − 0.20" in source


def test_scheduled_broadcast_always_builds_live_body():
    source = Path("v2/qiaolian_publisher_v2/daily_broadcast_patch.py").read_text(encoding="utf-8")
    block = source.split("async def scheduled_daily_broadcast", 1)[1].split("async def cmd_daily_text", 1)[0]
    assert "_fetch_phnom_penh_daily_info" in block
    assert "DAILY_TEMPLATES" not in block
    assert "WEEKLY_DAILY_PLAN" not in block
    assert "KEY_DAILY_TEXT" not in block


def test_live_fetch_uses_usd_cny_minus_point_two_not_khr():
    source = Path("autopilot_publish_bot.py").read_text(encoding="utf-8")
    start = source.index("def _fetch_phnom_penh_daily_info")
    end = source.index("def _conn", start)
    block = source[start:end]
    assert 'rates.get("CNY")' in block
    assert "market_cny - 0.20" in block
    assert "1 USD ≈" in block
    assert " CNY" in block
    assert 'rates.get("KHR")' not in block
