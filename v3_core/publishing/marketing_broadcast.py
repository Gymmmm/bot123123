"""Independent 7-day Telegram marketing broadcast plan.

Daily weather/FX remains owned by broadcast.py.  This module adds the 18:30
weekly marketing loop without changing collector, parser, listing or frozen
publication policy.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import os
import sqlite3
from zoneinfo import ZoneInfo

from .broadcast import BUTTON_KEYS, BroadcastButton, parse_hhmm
from .channel_contract import channel_general_action_url

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
        "🏠 金边年租找房",
        "🏠 <b>这周准备在金边找年租？</b>\n\n"
        "先发 4 个条件：\n"
        "• 常去地点 / 想住区域\n"
        "• 月预算\n"
        "• 户型\n"
        "• 入住时间\n\n"
        "频道里的出租房源以长期年租为主，<b>价格按月租展示</b>。条件越清楚，越容易直接筛掉不合适的房。\n\n"
        "<b>侨联地产｜您在金边的自己人</b>",
        (("🔍 开始找房", "find"), ("💬 中文顾问", "advisor"), ("📢 最新房源", "latest")),
    ),
    MarketingTemplate(
        "tue",
        "💰 年租预算",
        "💰 <b>在金边长期住，预算别只看房租</b>\n\n"
        "看房时一起问清：\n"
        "• 水电怎么计费\n"
        "• 物业谁承担\n"
        "• 网络含不含\n"
        "• 停车是否另收\n"
        "• 押付怎么谈\n\n"
        "同样的月租，住一年后的实际成本可能不一样。具体费用以每套房和合同为准。\n\n"
        "<b>侨联地产｜您在金边的自己人</b>",
        (("💬 中文顾问", "advisor"), ("🛎️ 侨联服务", "service"), ("🔍 开始找房", "find")),
    ),
    MarketingTemplate(
        "wed",
        "📍 长住先选区域",
        "📍 <b>中国人在金边长住，区域先按日常路线选</b>\n\n"
        "别只问“哪里最好”。先看你每天去哪：\n"
        "• 上班通勤\n"
        "• 吃饭买菜\n"
        "• 接送孩子\n"
        "• 常去商场 / 办事地点\n\n"
        "把常去地点发给我们，再从当前 Telegram 年租房源里反推区域，通常比先定一个区域更实用。\n\n"
        "<b>侨联地产｜您在金边的自己人</b>",
        (("🔍 开始找房", "find"), ("💬 中文顾问", "advisor")),
    ),
    MarketingTemplate(
        "thu",
        "🔍 年租怎么挑",
        "🔍 <b>在金边住一年，看房别只看装修</b>\n\n"
        "现场重点看：\n"
        "• 空调制冷和热水\n"
        "• 家具家电实际状态\n"
        "• 卧室采光与噪音\n"
        "• 网络条件\n"
        "• 停车 / 宠物要求\n\n"
        "人不在金边，也可以先约视频看房，把你在意的细节直接告诉顾问。\n\n"
        "<b>侨联地产｜您在金边的自己人</b>",
        (("💬 中文顾问", "advisor"), ("🔍 开始找房", "find")),
    ),
    MarketingTemplate(
        "fri",
        "📝 年租签约",
        "📝 <b>准备在金边住一年，签约前把这些写清楚</b>\n\n"
        "租期｜押付方式｜水电计费｜物业 / 网络｜维修责任｜提前退租｜家具家电清单｜退房条件\n\n"
        "口头说过的不算完成，重要条件尽量落到合同或附件里。具体条款以双方实际约定为准。\n\n"
        "<b>侨联地产｜您在金边的自己人</b>",
        (("💬 中文顾问", "advisor"), ("🔍 开始找房", "find")),
    ),
    MarketingTemplate(
        "sat",
        "🏠 周末年租看房",
        "🏠 <b>周末集中看金边年租房</b>\n\n"
        "提前发：<b>常去地点 / 区域 + 月预算 + 户型 + 入住时间</b>。\n\n"
        "先确认实时房态，再尽量把同一路线的房源排在一起，少在金边来回跑。\n\n"
        "频道房源以年租为主，租金按月展示；具体租期和押付以每套房实际约定为准。\n\n"
        "<b>侨联地产｜您在金边的自己人</b>",
        (("🔍 开始找房", "find"), ("🛎️ 侨联服务", "service"), ("💬 中文顾问", "advisor")),
    ),
    MarketingTemplate(
        "sun",
        "🛡 长住入住检查",
        "🛡️ <b>年租入住当天，先把房子留好底</b>\n\n"
        "建议一起确认：\n"
        "• 房屋现状照片 / 视频\n"
        "• 家具家电清单\n"
        "• 水电表读数\n"
        "• 钥匙 / 门卡数量\n"
        "• 已有损坏和维修事项\n\n"
        "住一年，入住时多留一份记录，退房时会省很多沟通。\n\n"
        "<b>侨联地产｜您在金边的自己人</b>",
        (("🛎️ 侨联服务", "service"), ("💬 中文顾问", "advisor")),
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
        # Retire the one legacy Monday override already stored in production.
        # New operator-edited copy remains fully supported.
        legacy_override = "金边找房第一步：先别急着看房！"
        body = base.body if legacy_override in custom else (custom or base.body)
        return MarketingTemplate(base.key, base.title, body, base.buttons)

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
        weekday = next((index for index, item in enumerate(TEMPLATES) if item.key == template.key), 0)
        selected = self.button_key(weekday)
        if selected == "none":
            return ()
        channel_username = str(os.getenv("CHANNEL_USERNAME") or "").strip().lstrip("@")
        if not channel_username:
            channel_url = str(os.getenv("CHANNEL_URL") or "").strip().rstrip("/")
            if channel_url.startswith("https://t.me/"):
                channel_username = channel_url.removeprefix("https://t.me/").split("/", 1)[0].lstrip("@")
        if not channel_username:
            channel_id = str(os.getenv("CHANNEL_ID") or "").strip()
            if channel_id.startswith("@"):
                channel_username = channel_id.lstrip("@")
        find = BroadcastButton("🔍 开始找房", channel_general_action_url(self.user_bot_username, "find"))
        advisor = BroadcastButton("💬 中文顾问", channel_general_action_url(self.user_bot_username, "advisor"))
        service = BroadcastButton("🛎️ 侨联服务", channel_general_action_url(self.user_bot_username, "service"))
        latest = BroadcastButton("📢 最新房源", f"https://t.me/{channel_username}") if channel_username else None
        if template.key == "mon" and latest is None:
            raise ValueError("marketing_channel_username_required")
        locked = {
            "mon": ((find, advisor), (latest,)),
            "tue": ((advisor, service), (find,)),
            "wed": ((find, advisor),),
            "thu": ((advisor, find),),
            "fri": ((advisor, find),),
            "sat": ((find, service), (advisor,)),
            "sun": ((service, advisor),),
        }
        if selected == "default":
            return locked[template.key]
        generic = {"find": BroadcastButton("🔍 帮我找房", find.url), "contact": BroadcastButton("💬 联系中文顾问", advisor.url)}
        if latest is not None:
            generic["latest"] = latest
        if selected == "combo":
            first = tuple(button for button in (find, latest) if button is not None)
            return (first, (advisor,))
        if selected in generic:
            return ((generic[selected],),)
        return locked[template.key]

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
