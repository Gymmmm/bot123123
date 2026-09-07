"""V3 source-text sanitizer extracted from locked V2.2 source_sanitizer.py.

Facts are preserved; source contacts/promotion attribution are isolated from the
text that will later enter the canonical parser. This module owns no Parser or
publication behavior.
"""

from __future__ import annotations

import re
import unicodedata
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


def strip_unicode_noise(value: str) -> str:
    """Strip source-app formatting/private-use noise, including WeChat U+F003."""
    return "".join(
        ch for ch in str(value or "")
        if unicodedata.category(ch) not in {"Cf", "Co"}
    )


@dataclass(frozen=True)
class SanitizedSourceText:
    text: str
    contacts: tuple[str, ...]
    removed_lines: tuple[str, ...]


def sanitize_source_text(raw_text: str) -> SanitizedSourceText:
    """Remove contacts/promotion while retaining property facts."""
    contacts: list[str] = []
    kept: list[str] = []
    removed: list[str] = []
    cleaned_source = strip_unicode_noise(str(raw_text or "")).replace("\r\n", "\n")
    for original in cleaned_source.split("\n"):
        line = original.strip()
        if not line:
            continue
        found = _URL.findall(line) + _HANDLE.findall(line) + _PHONE.findall(line)
        contacts.extend(str(item).strip() for item in found if str(item).strip())
        contact_match = _CONTACT_LINE.search(line)
        if contact_match:
            removed.append(line)
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
