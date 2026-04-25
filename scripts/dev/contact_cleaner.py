"""
contact_cleaner.py
联系方式清洗模块

功能：
  从房源原始文本中识别并移除他人的联系方式，防止转发时带走原始中介信息。

清洗范围：
  - 手机号 / 电话号码（柬埔寨、中国、国际格式）
  - WhatsApp 号码及链接
  - Telegram 联系人（@username、t.me/xxx、Telegram: xxx）
  - 微信号（微信/WeChat/wx: xxx）
  - Line ID
  - 邮箱地址
  - 明确的"联系"引导语句（如"有意者请联系"、"Contact:"等）

使用方式：
  from contact_cleaner import clean_contact_info

  cleaned_text = clean_contact_info(raw_text)
"""

import re

# ── 正则规则表 ────────────────────────────────────────────
# 每条规则：(名称, 正则, 替换文本)
_RULES: list[tuple[str, str, str]] = [

    # ── 电话 / 手机号 ─────────────────────────────────────
    # 柬埔寨手机：+855 / 00855 / 0 开头，8~10 位
    ("phone_kh",
     r"(?:\+855|00855|0)\s*[-.\s]?\d{2}[-.\s]?\d{3}[-.\s]?\d{3,4}",
     ""),

    # 中国手机：+86 / 86 / 1[3-9] 开头，11 位
    ("phone_cn",
     r"(?:\+86\s*|86\s*)?1[3-9]\d{9}",
     ""),

    # 通用国际号码：+国家码 + 8~15 位数字（含空格/短横）
    ("phone_intl",
     r"\+\d{1,3}[\s\-.]?\(?\d{1,4}\)?[\s\-.]?\d{3,5}[\s\-.]?\d{3,5}",
     ""),

    # 纯数字电话（8~11 位，前后非数字，避免误伤价格/面积）
    ("phone_plain",
     r"(?<!\d)(?:0\d{7,10}|\d{8,11})(?!\d)",
     ""),

    # ── WhatsApp ──────────────────────────────────────────
    ("whatsapp_link",
     r"https?://(?:wa\.me|api\.whatsapp\.com/send)[^\s\u4e00-\u9fff]*",
     ""),
    ("whatsapp_label",
     r"(?:WhatsApp|WA|Whatsapp)\s*[:：]?\s*\+?[\d\s\-]{7,20}",
     ""),

    # ── Telegram ──────────────────────────────────────────
    ("tg_link",
     r"https?://t\.me/[A-Za-z0-9_+]{3,}",
     ""),
    # tg_label 必须在 tg_username 之前，避免只删 @xxx 留下 "Telegram:"
    ("tg_label",
     r"(?:Telegram|TG|电报)\s*[:：]?\s*@?[A-Za-z0-9_]{0,32}",
     ""),
    ("tg_username",
     r"@[A-Za-z][A-Za-z0-9_]{3,31}",
     ""),

    # ── 微信 ──────────────────────────────────────────────
    ("wechat",
     r"(?:微信|WeChat|wechat|wx|WX)\s*[:：号]?\s*[A-Za-z0-9_\-]{4,30}",
     ""),

    # ── Line ──────────────────────────────────────────────
    ("line",
     r"(?:Line|LINE|line)\s*(?:ID\s*)?[:：]?\s*[A-Za-z0-9_\-.]{3,30}",
     ""),

    # ── 邮箱 ──────────────────────────────────────────────
    # 邮箱整体替换为空（含前后可能残留的 "or " 等引导词用 contact_guide_en 处理）
    ("email",
     r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}",
     ""),
    # 清理邮箱被删后残留的孤立域名片段（如 "agent.com"）
    ("email_residue",
     r"\b[A-Za-z0-9\-]{2,30}\.[a-z]{2,6}\b(?=\s|$)",
     ""),

    # ── 联系引导语句（中文）────────────────────────────────
    ("contact_guide_cn",
     r"(?:有意者?|感兴趣|欲了解更多|详情|咨询|询价|看房|预约)[\s，,]*(?:请|可以?|欢迎)?[\s]*(?:联系|私信|加|扫码|扫一扫|电话|致电|微信|WhatsApp|WA|Telegram|TG)[^\n。！!？?]{0,60}",
     ""),
    # ── 联系引导语句（英文）───────────────────────────────────
    ("contact_guide_en",
     r"(?:Contact|Call|Message|Reach|DM|PM|Inbox)\s*(?:us|me|agent)?\s*(?:at|on|via|:)?\s*[^\n]{0,60}",
     ""),
    # 清理引导词被删后残留的孤立行（如 "or email:"）
    ("orphan_line",
     r"^(?:or|and|via|\|)\s*(?:email|phone|tel|call|contact)?\s*[:\uff1a]?\s*$",
     ""),    # ── 尾部"联系我们"段落 ────────────────────────────────
    ("contact_section",
     r"(?:联系方式|联系我们|Contact\s*(?:Us|Info|Information))\s*[：:]\s*[^\n]{0,120}",
     ""),
]

# 预编译（orphan_line 需要 MULTILINE 标志）
_COMPILED = [
    (
        name,
        re.compile(
            pattern,
            re.IGNORECASE | re.UNICODE | (re.MULTILINE if name == "orphan_line" else 0)
        ),
        replacement
    )
    for name, pattern, replacement in _RULES
]

# 清理多余空行（连续3行以上空行压缩为1行）
_BLANK_LINES = re.compile(r"\n{3,}")

# 清理行尾多余空格
_TRAILING_SPACE = re.compile(r"[ \t]+\n")


def clean_contact_info(text: str, log: bool = False) -> str:
    """
    清洗文本中的联系方式。

    Args:
        text: 原始房源文本
        log:  是否打印清洗日志（调试用）

    Returns:
        清洗后的文本
    """
    if not text:
        return text

    cleaned = text
    removed = []

    for name, pattern, replacement in _COMPILED:
        new_text, count = pattern.subn(replacement, cleaned)
        if count > 0:
            removed.append((name, count))
            cleaned = new_text

    # 收尾处理
    cleaned = _TRAILING_SPACE.sub("\n", cleaned)
    cleaned = _BLANK_LINES.sub("\n\n", cleaned)
    cleaned = cleaned.strip()

    if log and removed:
        summary = ", ".join(f"{n}×{c}" for n, c in removed)
        print(f"[ContactCleaner] 已清洗：{summary}")

    return cleaned


def extract_contacts(text: str) -> list[dict]:
    """
    提取文本中的联系方式（不删除，仅提取，供存档用）。

    Returns:
        [{"type": "phone_kh", "value": "012 345 678"}, ...]
    """
    if not text:
        return []

    found = []
    for name, pattern, _ in _COMPILED:
        for match in pattern.finditer(text):
            found.append({"type": name, "value": match.group().strip()})
    return found


# ── 自测 ──────────────────────────────────────────────────

if __name__ == "__main__":
    samples = [
        # 柬埔寨房源典型文本
        """
🏠 金边市中心豪华公寓出租
📍 BKK1区，香格里拉附近
🛏 2+1房，90㎡，15楼
💰 月租 $1,500，押一付一

✨ 全新装修，带泳池健身房
🌟 包物业费，水电自理

有意者请联系：+855 12 345 678
微信：agent_kh2024
Telegram: @pprent_agent
WhatsApp: +855 98 765 432
        """,
        # 中介带中国手机
        """
炳发城联排别墅出租，4房5卫，月租$1200
一号路核心地段，交通便利
咨询请加微信 kh_house888 或致电 13812345678
        """,
        # 英文联系
        """
Luxury condo for rent near Aeon Mall
2BR/2BA, 85sqm, $900/month
Contact agent via WhatsApp: +855 77 123 456
or email: agent@pprealty.com
        """,
    ]

    for i, s in enumerate(samples, 1):
        print(f"\n{'='*50}")
        print(f"[原文 {i}]")
        print(s.strip())
        contacts = extract_contacts(s)
        if contacts:
            print(f"\n[提取到的联系方式]")
            for c in contacts:
                print(f"  {c['type']:20s}: {c['value']}")
        cleaned = clean_contact_info(s, log=True)
        print(f"\n[清洗后]")
        print(cleaned)
