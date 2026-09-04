from pathlib import Path
from urllib.parse import unquote


def test_channel_cta_carries_public_listing_identity(monkeypatch):
    import meihua_publisher as publisher
    monkeypatch.setattr(publisher, "BOT_USERNAME", "QiaolianHouseBot")
    monkeypatch.setattr(publisher, "ADVISOR_TG", "@advisor")
    rendered = publisher._caption_action_links("l_2", listing={"project": "永旺1", "layout": "1房1办公2卫", "property_type": "公寓", "price": 1800})
    decoded = unquote(rendered)
    assert "📅 预约看房" in rendered
    assert "💬 问这套" in rendered
    assert "QC0002" in decoded
    assert "1房＋书房｜2卫" in decoded
    assert "1房1办公2卫" not in decoded


def test_booking_flow_rechecks_status_in_shared_submit_helper():
    source = Path("qiaolian_dual/appointment_flow.py").read_text(encoding="utf-8")
    submit = source.split("async def _submit_appointment(", 1)[1].split("async def appoint_flow_cb(", 1)[0]
    assert "available, reason = listing_is_available(lid)" in submit
    assert "listing_unavailable_text(reason, lid)" in submit
    assert "if data.startswith('aptime:')" in source
    assert "if data == 'apconfirm:yes'" in source
    assert source.count("return await _submit_appointment(") >= 2


def test_booking_display_uses_shared_formatters():
    appointment = Path("qiaolian_dual/appointment_flow.py").read_text(encoding="utf-8")
    assert "layout = _display_layout(item.get('layout')" in appointment
    assert "date_text = _appointment_date_compact" in appointment
    assert "time_text = _appointment_time_compact" in appointment


def test_listing_detail_uses_consistent_advisor_wording():
    source = Path("qiaolian_dual/listing.py").read_text(encoding="utf-8")
    assert "点“联系顾问”逐项核对。" not in source
    assert "InlineKeyboardButton('💬 联系中文顾问'" in source
    assert "咨询顾问" not in source
