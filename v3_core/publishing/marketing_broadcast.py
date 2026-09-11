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

from .broadcast import BroadcastButton, parse_hhmm

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
        "<b>🏠 这周准备在金边找房？</b>\n\n先别急着一套套看。\n\n把这 4 个条件先定下来：\n📍 想住哪里\n💰 月租预算\n🏠 几房\n📅 什么时候入住\n\n例如：\n\nBKK1｜2房｜$800以内｜月底入住\n\n条件越清楚，找房越快。\n\n👇 点「帮我找房」，直接告诉侨联你的需求。",
        (("🔍 帮我找房", "find_home"), ("🏘 当前可约", "latest")),
    ),
    MarketingTemplate(
        "tue",
        "📋 看房准备",
        "<b>📋 看房别只看装修和照片</b>\n\n到现场，建议重点检查：\n\n✓ 空调制冷\n✓ 热水和水压\n✓ 冰箱、洗衣机\n✓ 门锁和门禁\n✓ 手机信号 / 网络\n✓ 停车是否方便\n\n喜欢的房子，再把家具、墙面和现有损坏拍照留档。\n\n房子好不好住，现场检查比照片更重要。\n\n👇 准备看房？可以直接预约。",
        (("📅 预约看房", "latest"), ("🔍 继续找房", "find_home")),
    ),
    MarketingTemplate(
        "wed",
        "💰 租房预算",
        "<b>💰 月租 $600，不代表每月只花 $600</b>\n\n找房时，建议一起确认：\n\n🏠 月租\n🔐 押金\n⚡ 电费\n💧 水费\n📶 网络\n🧾 管理费\n🚗 停车费\n\n尤其要问清楚：\n\n哪些包含在租金里？哪些需要另外付？\n\n👇 按你的真实预算找房，会比只看月租更准确。",
        (("💰 按预算找房", "budget"), ("💬 联系中文顾问", "advisor")),
    ),
    MarketingTemplate(
        "thu",
        "🔍 房源怎么选",
        "<b>🔍 两套房价格差不多，怎么选？</b>\n\n别只比装修。\n\n建议按这个顺序：\n\n① 位置｜通勤和生活方便吗？\n② 总预算｜水电管理费算进去了吗？\n③ 房屋状态｜设备实际怎么样？\n④ 租约｜押付、租期能接受吗？\n⑤ 配套｜停车、泳池、健身房真的需要吗？\n\n没有“最好”的房子，\n\n适合你的，才值得约看。\n\n👇 按自己的条件继续找。",
        (("🔍 帮我找房", "find_home"), ("🏘 当前可约", "latest")),
    ),
    MarketingTemplate(
        "fri",
        "📝 签约提醒",
        "<b>📝 准备签租约？这几项先看清楚</b>\n\n签字、付押金前确认：\n\n✓ 月租和付款方式\n✓ 押金金额及退还条件\n✓ 租期和入住日期\n✓ 提前退租怎么处理\n✓ 水电、网络、管理费谁承担\n✓ 家具家电及房屋现状\n✓ 维修责任怎么划分\n\n口头谈好的重要条件，尽量写进租约。\n\n入住时再把房屋和物品状态拍照留档。",
        (("🛡 侨联保障", "assurance"), ("💬 联系中文顾问", "advisor")),
    ),
    MarketingTemplate(
        "sat",
        "🏠 周末看房",
        "<b>🏠 周末有时间？集中看房更省时间</b>\n\n建议一次安排 2–4 套同区域房源：\n\n📍 路线更集中\n💰 价格更容易比较\n🏠 户型差异看得更清楚\n📝 看完马上就有判断\n\n例如：\n\nBKK1｜2房｜$600–800\n\n可以一次筛出几套合适的再约看。\n\n👇 看看今天有哪些房源可以预约。",
        (("🏘 当前可约", "latest"), ("📅 我的预约", "appointments")),
    ),
    MarketingTemplate(
        "sun",
        "🛡 侨联保障",
        "<b>🛡 租到房，不代表服务就结束了。</b>\n\n侨联希望把容易出问题的环节提前做好：\n\n签约前\n核对租金、押金和相关费用\n\n入住时\n房屋、表计、家具家电拍照留档\n\n入住后\n需要报修或物业协调，可以找侨联\n\n退租时\n按入住记录协助逐项核对\n\n从找房、看房，到入住后的事情，\n\n有需要，都可以找侨联。",
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
        return TEMPLATES[index % 7]

    def footer_rows(self, template: MarketingTemplate) -> tuple[tuple[BroadcastButton, ...], ...]:
        base = f"https://t.me/{self.user_bot_username}?start="
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
