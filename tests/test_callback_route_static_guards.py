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


def test_rental_service_callbacks_are_wired_and_legacy_aliases_are_kept():
    dispatcher = Path('qiaolian_dual/callbacks.py').read_text(encoding='utf-8')
    rental = Path('qiaolian_dual/callback_rental.py').read_text(encoding='utf-8')
    assert 'matches_rental' in dispatcher
    assert '(matches_rental, handle_rental_callback)' in dispatcher
    required = {
        'hub:rental', 'hub:rental:fees', 'hub:rental:handover', 'hub:rental:handover:preview',
        'hub:rental:handover:details', 'hub:rental:handover:pdf', 'hub:rental:deposit',
        'hub:rental:viewing', 'service:handover', 'service:deposit',
    }
    for callback in required:
        assert repr(callback) in rental


def test_rental_customer_copy_uses_non_guarantee_deposit_language():
    rental = Path('qiaolian_dual/callback_rental.py').read_text(encoding='utf-8')
    assert '协助核对和沟通' in rental
    assert '最终押金退还金额' in rental
    assert '仍以合同和实际核对结果为准' in rental
    for banned in ('押金保障承诺', '保证退押金', '保障每一分押金', '一定不扣', '通常不会扣灯泡'):
        assert banned not in rental


def test_rental_handover_uses_v2_preview_and_pdf_actions():
    rental = Path('qiaolian_dual/callback_rental.py').read_text(encoding='utf-8')
    assert "'📸 查看留档单示例', callback_data='hub:rental:handover:preview'" in rental
    assert "'📄 查看押金说明', callback_data='hub:rental:deposit'" in rental
    assert "'📥 下载完整版 PDF', callback_data='hub:rental:handover:pdf'" in rental
    assert 'send_photo' in rental
    assert 'send_document' in rental
