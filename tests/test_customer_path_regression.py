from pathlib import Path
from urllib.parse import unquote

def test_channel_cta_carries_public_listing_identity(monkeypatch):
    import meihua_publisher as publisher
    monkeypatch.setattr(publisher, 'BOT_USERNAME', 'QiaolianHouseBot')
    monkeypatch.setattr(publisher, 'ADVISOR_TG', '@advisor')
    rendered = publisher._caption_action_links('l_2', listing={'project': '永旺1', 'layout': '1房1办公2卫', 'property_type': '公寓', 'price': 1800})
    decoded = unquote(rendered)
    assert '📅 预约看房' in rendered
    assert '💬 问这套' in rendered
    assert 'QC0002' in decoded
    assert '1房＋书房｜2卫' in decoded
    assert '1房1办公2卫' not in decoded

def test_booking_flow_rechecks_status_on_final_confirm():
    source = Path('qiaolian_dual/appointment_flow.py').read_text(encoding='utf-8')
    confirm = source.split("if data == 'apconfirm:yes':", 1)[1]
    assert 'listing_is_available(lid_submit)' in confirm
    assert 'listing_unavailable_text(availability_reason)' in confirm

def test_booking_display_uses_shared_formatters():
    flows = Path('qiaolian_dual/flows.py').read_text(encoding='utf-8')
    appointment = Path('qiaolian_dual/appointment_flow.py').read_text(encoding='utf-8')
    assert "floor = _display_floor(info.get('floor'))" in flows
    assert "layout = _display_layout(item.get('layout')" in appointment

def test_listing_detail_uses_consistent_advisor_wording():
    source = Path('qiaolian_dual/listing.py').read_text(encoding='utf-8')
    assert '点“联系顾问”逐项核对。' not in source
    assert '点“联系中文顾问”逐项核对。' in source
