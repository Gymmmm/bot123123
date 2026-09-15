"""Independent 7-day Telegram marketing broadcast plan.

Daily weather/FX remains owned by broadcast.py.  This module adds the 18:30
weekly marketing loop without changing collector, parser, listing or frozen
publication policy.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import sqlite3
from zoneinfo import ZoneInfo

from .broadcast import BUTTON_KEYS, BroadcastButton, parse_hhmm

KEY_ENABLED = "marketing_broadcast_enabled"
KEY_TIME = "marketing_broadcast_time"
KEY_LAST_ATTEMPT = "marketing_broadcast_last_attempt_date"
KEY_LAST_SENT = "marketing_broadcast_last_sent_date"
DEFAULTS = {KEY_ENABLED: "1", KEY_TIME: "18:30", KEY_LAST_ATTEMPT: "", KEY_LAST_SENT: ""}


@dataclass(frozen=True)
class MarketingTemplate:
    key: str
    title: str
    body: str
    buttons: tuple[tuple[str, str], ...]


TEMPLATES = (
    MarketingTemplate(
        "mon",
        "🏠 本周找房",
        "<b>🏠 这周要在金边找房？</b>\n\n别急着一套套刷。\n\n先把这 4 样定下来：\n📍 想住哪片\n💰 月租预算\n🏠 几房\n📅 什么时候入住\n\n比如：\n\nBKK1｜2房｜$800以内｜月底入住\n\n条件清楚了，找房才快。\n\n👇 点「帮我找房」，直接跟我说你的需求。",
        (("🔍 帮我找房", "find_home"), ("🏘 当前可约", "latest")),
    ),
    MarketingTemplate(
        "tue",
        "📋 看房准备",
        "<b>📋 看房别只看装修和照片</b>\n\n到了现场，这几样盯紧点：\n\n✓ 空调冷不冷\n✓ 热水和水压\n✓ 冰箱、洗衣机\n✓ 门锁和门禁\n✓ 手机信号 / 网络\n✓ 停车方不方便\n\n看得上的房子，家具、墙面、已有损坏都拍张照留底。\n\n好不好住，现场看比照片靠谱。\n\n👇 准备看房？直接约就行。",
        (("📅 预约看房", "latest"), ("🔍 继续找房", "find_home")),
    ),
    MarketingTemplate(
        "wed",
        "💰 租房预算",
        "<b>💰 月租 $600，可不等于每月只花 $600</b>\n\n找房时，这些一起算清楚：\n\n🏠 月租\n🔐 押金\n⚡ 电费\n💧 水费\n📶 网络\n🧾 管理费\n🚗 停车费\n\n尤其问明白：\n\n哪些包在租金里？哪些要另付？\n\n👇 按你真实能掏的预算找，比只看月租靠谱。",
        (("💰 按预算找房", "budget"), ("💬 联系中文顾问", "advisor")),
    ),
    MarketingTemplate(
        "thu",
        "🔍 房源怎么选",
        "<b>🔍 两套房价钱差不多，咋选？</b>\n\n别光比装修。\n\n按这个顺序比：\n\n① 位置｜上下班、买菜方便吗？\n② 总预算｜水电管理费算进去了吗？\n③ 房屋状态｜设备实际怎么样？\n④ 租约｜押付、租期你能不能接受？\n⑤ 配套｜停车、泳池、健身房你真需要吗？\n\n没有“最好”的房，\n\n合适你的，才值得约看。\n\n👇 按自己的条件继续找。",
        (("🔍 帮我找房", "find_home"), ("🏘 当前可约", "latest")),
    ),
    MarketingTemplate(
        "fri",
        "📝 签约提醒",
        "<b>📝 要签租约了？这几项先看清楚</b>\n\n签字、交押金前核对：\n\n✓ 月租和怎么付\n✓ 押金多少、怎么退\n✓ 租期和入住日期\n✓ 提前退租咋办\n✓ 水电、网络、管理费谁出\n✓ 家具家电和房屋现状\n✓ 坏了谁修\n\n口头谈好的要紧事，尽量写进合同。\n\n入住时再把房屋和物品状态拍照留底。",
        (("🛡 侨联保障", "assurance"), ("💬 联系中文顾问", "advisor")),
    ),
    MarketingTemplate(
        "sat",
        "🏠 周末看房",
        "<b>🏠 周末有空？集中看几套更省事</b>\n\n一次约 2–4 套同片区的：\n\n📍 路线集中，少跑路\n💰 价钱好对比\n🏠 户型差别一眼看出来\n📝 看完当场心里有数\n\n比如：\n\nBKK1｜2房｜$600–800\n\n先筛出几套合适的再约看。\n\n👇 看看今天有哪些能约。",
        (("🏘 当前可约", "latest"), ("📅 我的预约", "appointments")),
    ),
    MarketingTemplate(
        "sun",
        "🛡 侨联保障",
        "<b>🛡 租到房，事还没完。</b>\n\n容易踩坑的环节，我们提前帮你盯：\n\n签约前\n租金、押金、杂费对清楚\n\n入住时\n房屋、表数、家具家电拍照留底\n\n入住后\n报修、找物业，找侨联就行\n\n退租时\n按入住记录一项项对\n\n找房、看房，到住进去以后，\n\n有事就找侨联——金边自己人。",
        (("🛡 了解侨联保障", "assurance"), ("🛠 入住服务", "service")),
    ),
)


class MarketingBroadcastService:
    def __init__(self, db_path: str | Path, *, user_bot_username: str, timezone_name: str = "Asia/Phnom_Penh"):
        self.db_path = Path(db_path).expanduser().resolve()
        self.user_bot_username = str(user_bot_username or "").strip().lstrip("@")
        self.timezone = ZoneInfo(timezone_name)
        if not self.user_bot_username:
            raise ValueError("marketing_user_bot_username_required")
        self.ensure_defaults()

    def _connect(self):
        conn = sqlite3.connect(f"file:{self.db_path.as_posix()}?mode=rw", uri=True, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def ensure_defaults(self):
        with self._connect() as conn:
            for key, value in DEFAULTS.items():
                conn.execute("INSERT OR IGNORE INTO publisher_settings_v3(setting_key,setting_value) VALUES (?,?)", (key, value))
            conn.commit()

    def _get(self, key: str) -> str:
        with self._connect() as conn:
            row = conn.execute("SELECT setting_value FROM publisher_settings_v3 WHERE setting_key=?", (key,)).fetchone()
        return str(row[0]) if row else DEFAULTS[key]

    def _get_optional(self, key: str, default: str = "") -> str:
        with self._connect() as conn:
            row = conn.execute("SELECT setting_value FROM publisher_settings_v3 WHERE setting_key=?", (key,)).fetchone()
        return str(row[0]) if row else str(default)

    def _set(self, key: str, value: str):
        with self._connect() as conn:
            conn.execute("INSERT INTO publisher_settings_v3(setting_key,setting_value,updated_at) VALUES (?,?,CURRENT_TIMESTAMP) ON CONFLICT(setting_key) DO UPDATE SET setting_value=excluded.setting_value,updated_at=CURRENT_TIMESTAMP", (key, value))
            conn.commit()

    @property
    def enabled(self) -> bool:
        return self._get(KEY_ENABLED) == "1"

    @property
    def send_time(self) -> str:
        value = self._get(KEY_TIME)
        return value if parse_hhmm(value) else "18:30"

    def set_enabled(self, enabled: bool):
        self._set(KEY_ENABLED, "1" if enabled else "0")

    def set_time(self, value: str) -> str:
        parsed = parse_hhmm(value)
        if parsed is None:
            raise ValueError("marketing_invalid_hhmm")
        normalized = f"{parsed[0]:02d}:{parsed[1]:02d}"
        self._set(KEY_TIME, normalized)
        return normalized

    def local_now(self) -> datetime:
        return datetime.now(self.timezone)

    def template(self, weekday: int | None = None) -> MarketingTemplate:
        index = self.local_now().weekday() if weekday is None else int(weekday)
        base = TEMPLATES[index % 7]
        custom = self._get_optional(f"marketing_body_{base.key}").strip()
        return MarketingTemplate(base.key, base.title, custom or base.body, base.buttons)

    def set_custom_body(self, weekday: int, body: str) -> None:
        clean = str(body or "").strip()
        if not clean:
            raise ValueError("marketing_body_required")
        template = TEMPLATES[int(weekday) % 7]
        self._set(f"marketing_body_{template.key}", clean[:12000])

    def reset_body(self, weekday: int) -> None:
        template = TEMPLATES[int(weekday) % 7]
        self._set(f"marketing_body_{template.key}", "")

    def button_key(self, weekday: int | None = None) -> str:
        index = self.local_now().weekday() if weekday is None else int(weekday)
        template = TEMPLATES[index % 7]
        value = self._get_optional(f"marketing_button_{template.key}", "default").strip().lower()
        return value if value in {*BUTTON_KEYS, "default"} else "default"

    def set_button(self, weekday: int, key: str) -> None:
        clean = str(key or "").strip().lower()
        if clean not in {*BUTTON_KEYS, "default"}:
            raise ValueError("marketing_unknown_button")
        template = TEMPLATES[int(weekday) % 7]
        self._set(f"marketing_button_{template.key}", clean)

    def footer_rows(self, template: MarketingTemplate) -> tuple[tuple[BroadcastButton, ...], ...]:
        base = f"https://t.me/{self.user_bot_username}?start="
        weekday = next((index for index, item in enumerate(TEMPLATES) if item.key == template.key), 0)
        selected = self.button_key(weekday)
        if selected == "none":
            return ()
        generic = {
            "find": BroadcastButton("🔍 帮我找房", base + "find_home"),
            "latest": BroadcastButton("🏠 最新房源", base + "latest"),
            "contact": BroadcastButton("💬 联系中文顾问", base + "advisor"),
        }
        if selected == "combo":
            return ((generic["find"], generic["latest"]), (generic["contact"],))
        if selected in generic:
            return ((generic[selected],),)
        return (tuple(BroadcastButton(label, base + payload) for label, payload in template.buttons),)

    def claim_due(self, now: datetime | None = None) -> tuple[bool, str]:
        current = now.astimezone(self.timezone) if now else self.local_now()
        local_date, hhmm = current.date().isoformat(), current.strftime("%H:%M")
        if not self.enabled or hhmm != self.send_time:
            return False, local_date
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT setting_value FROM publisher_settings_v3 WHERE setting_key=?", (KEY_LAST_ATTEMPT,)).fetchone()
            if row and str(row[0]) == local_date:
                conn.rollback()
                return False, local_date
            conn.execute("INSERT INTO publisher_settings_v3(setting_key,setting_value,updated_at) VALUES (?,?,CURRENT_TIMESTAMP) ON CONFLICT(setting_key) DO UPDATE SET setting_value=excluded.setting_value,updated_at=CURRENT_TIMESTAMP", (KEY_LAST_ATTEMPT, local_date))
            conn.commit()
        return True, local_date

    def mark_sent(self, local_date: str):
        self._set(KEY_LAST_SENT, local_date)
