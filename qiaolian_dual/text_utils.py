"""统一文本清洗工具"""


def clean_telegram_text(text):
    """清理 Telegram 显示文本中的转义字符和异常"""
    if not text:
        return ""

    text = str(text)

    # 处理 None/null
    if text.lower() in ('none', 'null'):
        return ""

    # 清除转义符
    text = text.replace('\\n', '\n')
    text = text.replace('<br>', '\n')
    text = text.replace('<br/>', '\n')
    text = text.replace('<br />', '\n')

    # 清除重复空格和空行
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

    # 单行文本：所有换行符转为空格
    text = text.replace('\\n', ' ')
    text = text.replace('\n', ' ')
    text = text.replace('<br>', ' ')
    text = text.replace('<br/>', ' ')
    text = text.replace('<br />', ' ')

    # 清除重复空格
    text = ' '.join(text.split())

    return text.strip()


def clean_highlights_for_card(text):
    """清理亮点文本用于卡片横向展示"""
    if not text:
        return ""

    # 先用通用清洗
    text = clean_telegram_text(text)

    # 转换为横向展示（最多3个）
    items = []
    for line in text.split('\n')[:3]:
        line = line.strip()
        # 移除开头的项目符号
        for prefix in ['• ', '- ', '* ', '· ']:
            if line.startswith(prefix):
                line = line[len(prefix):].strip()
                break
        if line:
            items.append(line)

    return ' · '.join(items) if items else ""


def remove_test_markers(text):
    """移除所有测试标记"""
    if not text:
        return ""

    text = str(text)

    # 移除测试字样
    markers = [
        '[测试]', '[系统测试]', '测试',
        'TEST_', '[TEST]', '(测试)', '（测试）'
    ]

    for marker in markers:
        text = text.replace(marker, '')

    return text.strip()


def fix_duplicate_words(text):
    """修复重复词语（如"即日起起"）"""
    if not text:
        return ""

    # 常见重复模式
    duplicates = [
        ('即日起起', '即日起'),
        ('可可入住', '可入住'),
        ('预约约看房', '预约看房'),
    ]

    for dup, fix in duplicates:
        text = text.replace(dup, fix)

    return text
