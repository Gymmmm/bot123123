#!/usr/bin/env python3
"""侨联Bot用户体验测试脚本"""
import sys
sys.path.insert(0, '/opt/qiaolian_dual_bots')

def test_basic_info():
    """测试Bot基础信息"""
    print('=== Bot基础信息 ===')
    from qiaolian_dual.config import USER_BOT_USERNAME, CHANNEL_URL
    print(f'Bot用户名: @{USER_BOT_USERNAME}')
    print(f'频道地址: {CHANNEL_URL}')
    print()

def test_database_stats():
    """测试数据库统计"""
    print('=== 数据库统计 ===')
    from qiaolian_dual.db import Database, DB_PATH

    db = Database(DB_PATH)
    cursor = db._get_connection().cursor()

    total = cursor.execute("SELECT COUNT(*) FROM listings").fetchone()[0]
    active = cursor.execute("SELECT COUNT(*) FROM listings WHERE status='active'").fetchone()[0]
    appointments = cursor.execute("SELECT COUNT(*) FROM appointments").fetchone()[0]
    users = cursor.execute("SELECT COUNT(*) FROM user_profiles").fetchone()[0]

    print(f'房源总数: {total}')
    print(f'活跃房源: {active}')
    print(f'预约记录: {appointments}')
    print(f'用户数量: {users}')

    print('\n活跃房源示例:')
    listings = cursor.execute("""
        SELECT listing_id, title, area, rent_usd, room_type
        FROM listings
        WHERE status='active'
        ORDER BY created_at DESC
        LIMIT 5
    """).fetchall()

    for l in listings:
        title = (l[1] or 'N/A')[:40]
        print(f'  [{l[0]}] {title} | {l[2]} | ${l[3]} | {l[4]}')
    print()

def test_conversation_states():
    """测试ConversationHandler状态"""
    print('=== ConversationHandler状态 ===')
    from qiaolian_dual.common import MAIN, FIND_AREA, FIND_BUDGET, APPT_MODE, APPT_FOCUS, APPT_DATE, APPT_TIME, APPT_CONFIRM

    states = {
        'MAIN': MAIN,
        'FIND_AREA': FIND_AREA,
        'FIND_BUDGET': FIND_BUDGET,
        'APPT_MODE': APPT_MODE,
        'APPT_FOCUS': APPT_FOCUS,
        'APPT_DATE': APPT_DATE,
        'APPT_TIME': APPT_TIME,
        'APPT_CONFIRM': APPT_CONFIRM,
    }

    for name, value in states.items():
        print(f'{name:15} = {value}')
    print()

def test_callback_routing():
    """测试回调路由"""
    print('=== 回调路由测试 ===')
    import re
    from qiaolian_dual.common import _MAIN_CB_PATTERN, _APPT_CB_PATTERN

    test_cases = [
        ('home', 'MAIN'),
        ('home_smart_search', 'MAIN'),
        ('listing:open:l_1', 'MAIN'),
        ('apmode:offline', 'APPT'),
        ('apdate:2026-08-20', 'APPT'),
        ('appointment_menu:list', 'MAIN'),
        ('hub:service', 'MAIN'),
    ]

    for callback, expected in test_cases:
        main_match = bool(re.match(_MAIN_CB_PATTERN, callback))
        appt_match = bool(re.match(_APPT_CB_PATTERN, callback))

        result = 'MAIN' if main_match else ('APPT' if appt_match else 'NONE')
        status = 'OK' if result == expected else 'FAIL'
        print(f'[{status}] {callback:30} -> {result}')
    print()

def test_listing_availability():
    """测试房源可用性"""
    print('=== 房源可用性测试 ===')
    from qiaolian_dual.listing import listing_is_available

    test_ids = ['l_1', 'l_9', 'l_25', 'l_999']
    for lid in test_ids:
        available, status = listing_is_available(lid)
        result = 'OK' if available else 'NO'
        print(f'{lid:10} -> [{result}] status={status}')
    print()

def test_main_keyboard():
    """测试主键盘布局"""
    print('=== 主键盘布局 ===')
    from qiaolian_dual.keyboards_common import main_keyboard

    kb = main_keyboard()
    print(f'键盘行数: {len(kb.inline_keyboard)}')
    for i, row in enumerate(kb.inline_keyboard, 1):
        buttons = ' | '.join([btn.text for btn in row])
        print(f'  行{i}: {buttons}')
    print()

def test_search_function():
    """测试搜索功能"""
    print('=== 搜索功能测试 ===')
    from qiaolian_dual.search import search_listings_with_fallback

    # 查看函数签名
    import inspect
    sig = inspect.signature(search_listings_with_fallback)
    print(f'搜索函数参数: {sig}')

    # 简单搜索测试
    try:
        results = search_listings_with_fallback(area='BKK1')
        print(f'BKK1搜索结果: {len(results)}套房源')
        for r in results[:3]:
            title = (r.get('title') or 'N/A')[:30]
            print(f'  [{r["listing_id"]}] {title} ${r["rent_usd"]}')
    except Exception as e:
        print(f'搜索测试失败: {e}')
    print()

def test_module_imports():
    """测试关键模块导入"""
    print('=== 模块导入测试 ===')

    modules = [
        ('qiaolian_dual.user_bot', ['start', 'handle_ui_callback', 'main_keyboard']),
        ('qiaolian_dual.callbacks', ['handle_ui_callback']),
        ('qiaolian_dual.listing', ['listing_is_available']),
        ('qiaolian_dual.search', ['search_listings_with_fallback']),
        ('qiaolian_dual.appointment_flow', ['appoint_flow_cb']),
        ('qiaolian_dual.app', ['build_application', 'main']),
    ]

    for module_name, functions in modules:
        try:
            module = __import__(module_name, fromlist=functions)
            missing = [f for f in functions if not hasattr(module, f)]
            if missing:
                print(f'[WARN] {module_name}: 缺少 {missing}')
            else:
                print(f'[OK] {module_name}')
        except Exception as e:
            print(f'[FAIL] {module_name}: {e}')
    print()

def main():
    """运行所有测试"""
    print('='*60)
    print('侨联Bot用户体验测试')
    print('='*60)
    print()

    try:
        test_basic_info()
        test_database_stats()
        test_conversation_states()
        test_callback_routing()
        test_listing_availability()
        test_main_keyboard()
        test_search_function()
        test_module_imports()

        print('='*60)
        print('测试完成！')
        print('='*60)

    except Exception as e:
        print(f'\n测试失败: {e}')
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    main()
