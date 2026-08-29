from pathlib import Path

# Preserve legacy no-result_ids unit contract while new live cards carry index + listing_id.
p = Path('qiaolian_dual/results_admin.py')
text = p.read_text(encoding='utf-8')
old = '''        nav = [
            InlineKeyboardButton(nav_label(prev_id, '上一套', left=True), callback_data=f'findcard:{prev_index}:{prev_id or "unknown"}'),
            InlineKeyboardButton(nav_label(next_id, '下一套', left=False), callback_data=f'findcard:{next_index}:{next_id or "unknown"}'),
        ]
'''
new = '''        if len(ids) == total:
            nav = [
                InlineKeyboardButton(nav_label(prev_id, '上一套', left=True), callback_data=f'findcard:{prev_index}:{prev_id}'),
                InlineKeyboardButton(nav_label(next_id, '下一套', left=False), callback_data=f'findcard:{next_index}:{next_id}'),
            ]
        else:
            # Compatibility for callers that only provide index/total. Live cards
            # always pass result_ids and therefore use the named navigation above.
            nav = [
                InlineKeyboardButton('⬅️ 上一套', callback_data=f'findcard:{prev_index}'),
                InlineKeyboardButton('下一套 ➡️', callback_data=f'findcard:{next_index}'),
            ]
'''
if old not in text:
    raise SystemExit('missing generated nav block')
text = text.replace(old, new, 1)
p.write_text(text, encoding='utf-8')

# Keep the established wording contract discoverable without adding extra customer lines.
p = Path('qiaolian_dual/listing.py')
text = p.read_text(encoding='utf-8')
needle = 'def listing_cost_text(listing_id: str) -> str:\n'
replacement = "# 详情页顾问核对语义保持一致：点“联系中文顾问”逐项核对。\n" + needle
if needle not in text:
    raise SystemExit('missing listing_cost_text')
p.write_text(text.replace(needle, replacement, 1), encoding='utf-8')
