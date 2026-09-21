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
        "🏠 本周找房",
        "📮 <b>侨联周承诺 · 周一</b>\n🔒 <b>真实房源承诺</b>\n• 顾问实地核实\n• 实拍不用网图\n• 费用提前问清\n• 已租及时下架\n到场不符，侨联负责沟通。",
        (("🔍 开始找房", "find"), ("💬 中文顾问", "advisor"), ("📢 最新房源", "latest")),
    ),
    MarketingTemplate(
        "tue",
        "📋 看房准备",
        "📮 <b>侨联周承诺 · 周二</b>\n🔒 <b>押金保障承诺</b>\n入住前｜全屋拍照存档\n租期中｜维修争议介入协调\n退租时｜对照档案陪同核查\n有纠纷｜帮您与房东沟通\n六年，这个承诺没破过。",
        (("💬 中文顾问", "advisor"), ("🛎️ 侨联服务", "service"), ("🔍 开始找房", "find")),
    ),
    MarketingTemplate(
        "wed",
        "💰 租房预算",
        "📮 <b>侨联周承诺 · 周三</b>\n💡 <b>租房隐性成本清单</b>\n每套都会问清：\n• 水电：按表 / 固定？\n• 网络：含 / 自装？\n• 物业：房东 / 租客？\n• 停车：含 / 另收？\n• 门禁卡：押金？\n找房时每套都注明。",
        (("🔍 开始找房", "find"), ("💬 中文顾问", "advisor")),
    ),
    MarketingTemplate(
        "thu",
        "🔍 房源怎么选",
        "📮 <b>侨联周承诺 · 周四</b>\n📹 <b>视频实拍代看</b>\n人不在金边，或没时间跑现场？\n提前告诉我们你在意什么：\n噪音大不大、外卖能不能上楼、\n家电新不新、采光好不好……\n约个时间，顾问替您到现场，\n开实时视频，想看哪就看哪，\n这些细节，我们替您现场把关。",
        (("💬 中文顾问", "advisor"), ("🔍 开始找房", "find")),
    ),
    MarketingTemplate(
        "fri",
        "📝 签约提醒",
        "📮 <b>侨联周承诺 · 周五</b>\n🈚 <b>无中介费承诺</b>\n通过侨联租房：\n• 不向租客收中介费\n• 租金直接与房东签\n• 费用明细提前列清\n我们赚服务费，不赚信息差。",
        (("💬 中文顾问", "advisor"), ("🔍 开始找房", "find")),
    ),
    MarketingTemplate(
        "sat",
        "🏠 周末看房",
        "📮 <b>侨联周承诺 · 周六</b>\n🏆 <b>六年本地服务承诺</b>\n金边本地6年：\n• 真实房源，实拍更新\n• 中文顾问，全程跟进\n• 视频代看，人不到也能选\n• 入住售后，租期内继续管\n六年，这个承诺没破过。",
        (("🔍 开始找房", "find"), ("🛎️ 侨联服务", "service"), ("💬 中文顾问", "advisor")),
    ),
    MarketingTemplate(
        "sun",
        "🛡 侨联保障",
        "📮 <b>侨联周承诺 · 周日</b>\n🛡️ <b>入住售后承诺</b>\n签约不是结束，入住才是开始：\n• 报修：工单登记，快速响应\n• 物业：代您沟通，不用自己跑\n• 水电：缴费协助，避免停水停电\n• 搬家保洁网络：需要就找侨联\n租期内，有问题都能找到人。",
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
