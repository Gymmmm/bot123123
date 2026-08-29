from pathlib import Path


def test_admin_menu_has_daily_broadcast_entry():
    source = Path("v2/qiaolian_publisher_v2/keyboards.py").read_text(encoding="utf-8")
    assert "📢 每日广播" in source
    assert "cmd:daily" in source


def test_weekly_broadcast_plan_is_complete_and_quiet():
    source = Path("autopilot_publish_bot.py").read_text(encoding="utf-8")
    for label in ("周一｜金边今日信息", "周二｜今日可看房源", "周三｜区域怎么选", "周四｜租房费用", "周五｜周末看房", "周六｜看房前准备", "周日｜签约前确认"):
        assert label in source
    assert "每天只发 1 条" in source
    assert "daily:weekly" in source
    assert "daily:plan" in source


def test_scheduled_broadcast_uses_weekly_content():
    source = Path("autopilot_publish_bot.py").read_text(encoding="utf-8")
    block = source.split("async def scheduled_daily_broadcast", 1)[1].split("async def tick_daily_broadcast", 1)[0]
    assert "_daily_is_weekly()" in block
    assert "DAILY_TEMPLATES[template_no][1]" in block
    assert "_fetch_phnom_penh_daily_info" in block
