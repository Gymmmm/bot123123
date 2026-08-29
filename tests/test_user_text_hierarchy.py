from __future__ import annotations

import re
from html.parser import HTMLParser


class _StrictTelegramHTML(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.stack: list[str] = []

    def handle_starttag(self, tag: str, attrs) -> None:
        if tag in {'b', 'code', 'i', 'u', 's', 'a'}:
            self.stack.append(tag)

    def handle_endtag(self, tag: str) -> None:
        if tag in {'b', 'code', 'i', 'u', 's', 'a'}:
            assert self.stack and self.stack.pop() == tag


def _assert_html_contract(text: str) -> None:
    parser = _StrictTelegramHTML()
    parser.feed(text)
    parser.close()
    assert parser.stack == []
    assert '**' not in text
    assert r'\<b\>' not in text and r'\</b\>' not in text
    bold_lines = [bool(re.fullmatch(r'\s*<b>.*</b>\s*', line)) for line in text.splitlines()]
    assert '111' not in ''.join('1' if value else '0' for value in bold_lines)


def test_core_user_pages_have_balanced_hierarchical_html(monkeypatch):
    from qiaolian_dual import listing as listing_mod
    from qiaolian_dual.appointment_ui import _appointment_confirm_text
    from qiaolian_dual.messages import home_text
    from qiaolian_dual.texts import service_hub_text

    item = {
        'listing_id': 'l_88', 'project': '永旺1', 'title': '永旺1',
        'layout': '1房1办公2卫', 'property_type': '公寓', 'price': 1800,
        'status': 'reserved', 'deposit_rule': '押2付1', 'size_sqm': 96.3,
    }
    monkeypatch.setattr(listing_mod, 'listing_context', lambda _lid: item)
    pages = [
        home_text(),
        service_hub_text(),
        listing_mod.listing_cost_text('l_88'),
        listing_mod.listing_unavailable_text('pending'),
        listing_mod.listing_unavailable_text('rented'),
        listing_mod.listing_unavailable_text('offline'),
        _appointment_confirm_text({
            'listing_id': 'l_88', 'date': '08/29', 'time': 'pm',
            'mode': 'offline', 'touch_payload': {},
        }),
    ]
    for page in pages:
        _assert_html_contract(page)


def test_status_is_highlighted_once_per_listing_page(monkeypatch):
    from qiaolian_dual import listing as listing_mod

    for status, expected in (
        ('active', '🟢 <b>当前可预约</b>'),
        ('reserved', '🟡 <b>已有预约 · 仍可预约</b>'),
        ('pending', '🔵 <b>房态待确认</b>'),
    ):
        monkeypatch.setattr(listing_mod, 'listing_context', lambda _lid, value=status: {
            'listing_id': 'l_1', 'status': value, 'price': 800,
        })
        text = listing_mod.listing_cost_text('l_1')
        assert text.count(expected) == 1


def test_listing_card_keeps_callbacks_and_escapes_dynamic_values(monkeypatch):
    from qiaolian_dual import results_admin

    item = {
        'listing_id': 'l_1', 'project': '永旺 <一>', 'layout': '1房 & 办公',
        'property_type': '公寓', 'price': 1800, 'status': 'reserved',
    }
    monkeypatch.setattr('qiaolian_dual.listing.listing_context', lambda _lid: item)
    text, keyboard, _ = results_admin._find_result_card_content(item, 0, 1, ['l_1'])
    _assert_html_contract(text)
    assert '&lt;一&gt;' in text and '&amp;' in text
    callbacks = [button.callback_data for row in keyboard.inline_keyboard for button in row]
    assert callbacks == [
        'listing:detail:l_1', 'listing:appoint:l_1',
        'listing:photos:l_1', 'listing:consult:l_1', 'home_smart_search',
    ]


def test_core_html_sends_declare_html_parse_mode():
    paths = {
        'qiaolian_dual/results_admin.py': ('send_listing_photo_preview',),
        'qiaolian_dual/appointment_flow.py': ('_appointment_confirm_text', 'parse_mode=ParseMode.HTML'),
        'qiaolian_dual/callback_service.py': ('报修已提交', 'parse_mode=ParseMode.HTML'),
    }
    for filename, needles in paths.items():
        source = open(filename, encoding='utf-8').read()
        for needle in needles:
            assert needle in source


def test_no_empty_separator_message_remains_in_user_bot():
    source = open('qiaolian_dual/callback_listing.py', encoding='utf-8').read()
    assert "text='—'" not in source

