from pathlib import Path


def test_admin_menu_has_broadcast_center_entry():
    source = Path("v2/qiaolian_publisher_v2/keyboards.py").read_text(encoding="utf-8")
    assert "📢 广播中心" in source
    assert "cmd:daily" in source


def test_production_runner_installs_daily_broadcast_contract():
    source = Path("v2/run_publisher_bot_v2.py").read_text(encoding="utf-8")
    assert "install_daily_broadcast_patch" in source
    assert "install_daily_broadcast_patch()" in source


def test_daily_broadcast_can_edit_copy_fx_time_and_footer():
    source = Path("v2/qiaolian_publisher_v2/daily_broadcast_patch.py").read_text(encoding="utf-8")
    assert "scheduled_daily_broadcast" in source
    assert "_fetch_phnom_penh_daily_info" in source
    assert "daily_broadcast_fx_offset" in source
    assert "daily:fxset:-20" in source
    assert "daily:fxset:20" in source
    assert "daily:custom" in source
    assert "daily:time" in source
    assert "daily:buttons" in source
    assert "daily:btn:" in source


def test_broadcast_center_has_ready_to_send_presets():
    source = Path("v2/qiaolian_publisher_v2/daily_broadcast_patch.py").read_text(encoding="utf-8")
    assert "📅 每周找房提醒" in source
    assert "🏠 周末看房" in source
    assert "📋 看房前准备" in source
    assert "📝 签约提醒" in source
    assert "daily:tplsend:" in source


def test_scheduled_broadcast_uses_current_saved_body():
    source = Path("v2/qiaolian_publisher_v2/daily_broadcast_patch.py").read_text(encoding="utf-8")
    block = source.split("async def scheduled_daily_broadcast", 1)[1].split("async def cmd_daily_text", 1)[0]
    assert "_current_body" in block
    assert "WEEKLY_DAILY_PLAN" not in block
    assert "KEY_DAILY_TEXT" in source


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
