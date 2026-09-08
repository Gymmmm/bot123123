"""统一文本清洗工具"""


def clean_telegram_text(text):
    """清理 Telegram 显示文本中的转义字符和异常"""
    if not text:
        return ""

    text = str(text)
    if text.lower() in ('none', 'null'):
        return ""

    text = text.replace('\\n', '\n')
    text = text.replace('<br>', '\n')
    text = text.replace('<br/>', '\n')
    text = text.replace('<br />', '\n')

    lines = [line.strip() for line in text.split('\n')]
    lines = [line for line in lines if line]
    return '\n'.join(lines)


def clean_inline_text(text):
    """清理单行文本（用于卡片标题、价格等）"""
    if not text:
        return ""

    text = str(text)
    if text.lower() in ('none', 'null'):
        return ""

    text = text.replace('\\n', ' ')
    text = text.replace('\n', ' ')
    text = text.replace('<br>', ' ')
    text = text.replace('<br/>', ' ')
    text = text.replace('<br />', ' ')
    text = ' '.join(text.split())
    return text.strip()


def remove_test_markers(text):
    """移除所有测试标记"""
    if not text:
        return ""

    text = str(text)
    markers = [
        '[测试]', '[系统测试]', '测试',
        'TEST_', '[TEST]', '(测试)', '（测试）'
    ]
    for marker in markers:
        text = text.replace(marker, '')
    return text.strip()


def fix_duplicate_words(text):
    """修复重复词语（如\"即日起起\"）"""
    if not text:
        return ""

    duplicates = [
        ('即日起起', '即日起'),
        ('可可入住', '可入住'),
        ('预约约看房', '预约看房'),
    ]
    for dup, fix in duplicates:
        text = text.replace(dup, fix)
    return text
