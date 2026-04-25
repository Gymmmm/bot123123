#!/usr/bin/env python3
"""测试新文案是否生效"""

import sys
sys.path.insert(0, '/opt/qiaolian_dual_bots')

from meihua_publisher import build_chinese_listing_post, build_keyboard

# 测试数据
test_listing = {
    'area': 'BKK1',
    'layout': '1房1卫',
    'price': 800,
    'room_type': '整租',
    'listing_id': 'test_001',
    'draft_id': 'DRF_test',
}

print("=" * 80)
print("测试 A/B/C 文案变体")
print("=" * 80)

for variant in ['a', 'b', 'c']:
    print(f"\n{'='*80}")
    print(f"变体 {variant.upper()}")
    print(f"{'='*80}")
    caption = build_chinese_listing_post(test_listing, variant)
    print(caption)
    print()

print("\n" + "=" * 80)
print("测试按钮文字")
print("=" * 80)

# 测试按钮（需要设置BOT_USERNAME环境变量）
import os
os.environ['BOT_USERNAME'] = '@qiaolian_bot'  # 临时设置

try:
    keyboard = build_keyboard(
        listing_id='test_001',
        area='BKK1',
        post_token='test_token',
        caption_variant='a'
    )
    
    print("\n按钮配置：")
    for row in keyboard.inline_keyboard:
        for button in row:
            print(f"  • {button.text}")
except Exception as e:
    print(f"按钮测试失败: {e}")

print("\n" + "=" * 80)
print("测试用户Bot无结果文案")
print("=" * 80)

from qiaolian_dual.messages import find_no_match_text

print("\n" + find_no_match_text())

print("\n" + "=" * 80)
print("✅ 所有测试完成！")
print("=" * 80)
