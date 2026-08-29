"""第三方房源来源清洗：保留事实，隔离联系方式和推广归属。"""

from __future__ import annotations

import re
from dataclasses import dataclass


_CONTACT_LINE = re.compile(
    r"(?:联系(?:方式|人)?|咨询|客服|经纪|中介|微信|wechat|wx|telegram|飞机|纸飞机|tg|电话|手机|"
    r"whatsapp|line|扫码|二维码|频道|群组|群聊|主页|私聊|私信|加我|找我)",
    re.IGNORECASE,
)
_URL = re.compile(r"(?:https?://|www\.)\S+|t\.me/\S+", re.IGNORECASE)
_HANDLE = re.compile(r"(?<![\w@])@[A-Za-z][A-Za-z0-9_]{3,}")
_PHONE = re.compile(r"(?<!\d)(?:\+?\d[\d\s().-]{6,}\d)(?!\d)")
_PROMO = re.compile(
    r"(?:欢迎关注|点击加入|更多房源|每日更新|房源发布|租房频道|转发|置顶|推广|代理合作|佣金|"
    r"全网最低|独家渠道)",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class SanitizedSourceText:
    text: str
    contacts: tuple[str, ...]
    removed_lines: tuple[str, ...]


def sanitize_source_text(raw_text: str) -> SanitizedSourceText:
    """删除来源联系方式、链接和推广话术，保留价格、区域、户型等房源事实。"""
    contacts: list[str] = []
    kept: list[str] = []
    removed: list[str] = []
    for original in str(raw_text or "").replace("\r\n", "\n").split("\n"):
        line = original.strip()
        if not line:
            continue
        found = _URL.findall(line) + _HANDLE.findall(line) + _PHONE.findall(line)
        contacts.extend(str(item).strip() for item in found if str(item).strip())
        contact_match = _CONTACT_LINE.search(line)
        if contact_match:
            removed.append(line)
            # Keep a factual prefix when the source appends contact details to
            # the same line (for example "租金 $600/月 联系微信...").
            line = line[: contact_match.start()].strip(" -—|｜·,，;；")
            if not line:
                continue
        if _PROMO.search(line):
            removed.append(original.strip())
            continue
        cleaned = _URL.sub("", line)
        cleaned = _HANDLE.sub("", cleaned)
        cleaned = _PHONE.sub("", cleaned)
        cleaned = re.sub(r"\s{2,}", " ", cleaned).strip(" -—|｜·,，;；")
        if cleaned:
            kept.append(cleaned)
        else:
            removed.append(line)
    return SanitizedSourceText(
        text="\n".join(dict.fromkeys(kept)),
        contacts=tuple(dict.fromkeys(contacts)),
        removed_lines=tuple(removed),
    )
