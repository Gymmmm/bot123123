from pathlib import Path


def test_no_detail_to_appointment_alias_and_no_photo_similar_fallback():
    source = Path('qiaolian_dual/callback_listing.py').read_text(encoding='utf-8')
    detail = source[source.index("if data.startswith('listing:detail:')"):source.index("if data.startswith('listing:similar:')")]
    assert "return await start_appointment" not in detail
    photos = source[source.index("if data.startswith('listing:photos:')"):source.index("if data == 'find:show_more'")]
    assert 'listing:similar:' not in photos


def test_reserved_is_explicitly_available():
    source = Path('qiaolian_dual/listing.py').read_text(encoding='utf-8')
    block = source[source.index('def listing_is_available'):source.index('def listing_unavailable_text')]
    assert "{'active', 'reserved'}" in block


def test_unavailable_callback_paths_use_html_parse_mode():
    for filename in ('qiaolian_dual/callback_listing.py', 'qiaolian_dual/flows.py', 'qiaolian_dual/start_routes.py'):
        source = Path(filename).read_text(encoding='utf-8')
        if 'listing_unavailable_text' in source:
            assert 'parse_mode=ParseMode.HTML' in source


def test_recommendation_card_no_longer_routes_detail_to_open():
    source = Path('qiaolian_dual/results_admin.py').read_text(encoding='utf-8')
    assert "InlineKeyboardButton('📋 租赁详情', callback_data=f'listing:detail:{listing_id}')" in source
