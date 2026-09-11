"""V3-owned Publisher daily broadcast domain.

This module replaces the production runtime patch with explicit storage and
services.  It never creates schema implicitly and never calls Telegram.
Scheduled delivery is claimed in SQLite before the network call so an
ambiguous Telegram result cannot cause an automatic duplicate on the same day.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import json
from pathlib import Path
import sqlite3
from typing import Callable, Mapping
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo


DDL = """
CREATE TABLE IF NOT EXISTS publisher_settings_v3 (
    setting_key TEXT PRIMARY KEY,
    setting_value TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS publisher_broadcast_log_v3 (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trigger_type TEXT NOT NULL,
    template_key TEXT NOT NULL,
    channel_chat_id TEXT NOT NULL,
    channel_message_id INTEGER,
    status TEXT NOT NULL,
    local_date TEXT NOT NULL,
    error_text TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_publisher_broadcast_log_v3_created
    ON publisher_broadcast_log_v3(created_at DESC);
"""

KEY_ENABLED = "daily_broadcast_enabled"
KEY_TIME = "daily_broadcast_time"
KEY_TEMPLATE = "daily_broadcast_template"
KEY_CUSTOM_HTML = "daily_broadcast_html"
KEY_FX_OFFSET = "daily_broadcast_fx_offset"
KEY_BUTTON = "daily_broadcast_button"
KEY_LAST_ATTEMPT_DATE = "daily_broadcast_last_scheduled_attempt_date"
KEY_LAST_SENT_DATE = "daily_broadcast_last_scheduled_sent_date"

DEFAULTS: Mapping[str, str] = {
    KEY_ENABLED: "0",
    KEY_TIME: "09:30",
    KEY_TEMPLATE: "live",
    KEY_CUSTOM_HTML: "",
    KEY_FX_OFFSET: "0.00",
    KEY_BUTTON: "none",
    KEY_LAST_ATTEMPT_DATE: "",
    KEY_LAST_SENT_DATE: "",
}

BUTTON_LABELS = {
    "none": "不带按钮",
    "find": "帮我找房",
    "latest": "最新房源",
    "contact": "联系中文顾问",
    "combo": "组合按钮",
}
BUTTON_KEYS = frozenset(BUTTON_LABELS)

TEMPLATES: Mapping[str, tuple[str, str]] = {
    "weekly": (
        "📅 每周找房提醒",
        "<b>📅 侨联地产｜本周找房提醒</b>\n\n"
        "这周准备找房或换房，可以直接把 <b>区域 + 月预算 + 户型 + 入住时间</b> 发给我们。\n\n"
        "我们按当前在租房源为您筛选，不需要先翻一大堆无关房源。\n\n"
        "<b>💎 侨联地产｜您在金边的自己人</b>",
    ),
    "weekend": (
        "🏠 周末看房",
        "<b>🏠 周末看房安排</b>\n\n"
        "周末准备集中看房的，可以提前把 <b>区域、预算、户型、方便时间</b> 发过来。\n\n"
        "我们先确认房态，再安排实际可看的房源；不方便到现场也可以先约视频看房。\n\n"
        "<b>💎 侨联地产｜您在金边的自己人</b>",
    ),
    "viewing": (
        "📋 看房前准备",
        "<b>📋 看房前，先把这几项定下来</b>\n\n"
        "• 想住的区域\n"
        "• 每月预算\n"
        "• 户型\n"
        "• 预计入住时间\n\n"
        "这几项明确以后，找房和排看房都会快很多。\n\n"
        "<b>💎 侨联地产｜您在金边的自己人</b>",
    ),
    "contract": (
        "📝 签约提醒",
        "<b>📝 签约前再确认一次</b>\n\n"
        "租期、押付方式、水电、物业、网络、停车，以及提前退租和维修责任，最好都在签约前确认清楚。\n\n"
        "入住当天再把房屋现状、家具家电、表计读数、钥匙和门卡做好留档。\n\n"
        "<b>💎 侨联地产｜您在金边的自己人</b>",
    ),
}
TEMPLATE_KEYS = frozenset({"live", "custom", *TEMPLATES.keys()})

_WEATHER_LABELS = {
    0: "晴朗", 1: "大致晴朗", 2: "多云", 3: "阴天", 45: "有雾", 48: "有雾",
    51: "毛毛雨", 53: "毛毛雨", 55: "毛毛雨", 56: "冻雨", 57: "冻雨",
    61: "小雨", 63: "有雨", 65: "大雨", 66: "冻雨", 67: "冻雨",
    71: "小雪", 73: "有雪", 75: "大雪", 77: "冰粒",
    80: "阵雨", 81: "阵雨", 82: "强阵雨", 85: "阵雪", 86: "强阵雪",
    95: "雷雨", 96: "雷雨夹冰雹", 99: "雷雨夹冰雹",
}
_WEATHER_FALLBACK_ORDER = (
    "storm", "rain_heavy", "rain_light", "rain_possible", "hot",
    "sunny", "humid", "good", "stable",
)
_FALLBACK_WEATHER_TEXT = {
    "storm": "今天有雷雨，出门前建议看一下路况，看房时间可以和顾问确认是否需要调整。",
    "rain_heavy": "今天雨势较大，看房建议预留多一点通勤时间。",
    "rain_light": "今天有阵雨，出门记得带伞。",
    "rain_possible": "今天午后可能有雨，安排看房时间可以避开这个时段。",
    "hot": "今天气温偏高，看房尽量避开正午时段。",
    "sunny": "今天太阳比较大，户外通勤建议做好防晒。",
    "humid": "今天体感较闷热，室内看房影响不大，户外通勤注意补水。",
    "good": "今天天气不错，适合安排看房或出门办事。",
    "stable": "今天天气比较稳定，没有特别需要注意的。",
}


def parse_hhmm(value: object) -> tuple[int, int] | None:
    raw = str(value or "").strip()
    if len(raw) != 5 or raw[2] != ":":
        return None
    try:
        hour = int(raw[:2])
        minute = int(raw[3:])
    except ValueError:
        return None
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        return None
    return hour, minute


def _truthy(value: object) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _clamp_fx(value: object) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        parsed = 0.0
    return max(-2.0, min(2.0, parsed))


@dataclass(frozen=True)
class BroadcastConfig:
    enabled: bool
    send_time: str
    template_key: str
    custom_html: str
    fx_offset: float
    button_key: str
    last_scheduled_attempt_date: str
    last_scheduled_sent_date: str


@dataclass(frozen=True)
class BroadcastButton:
    label: str
    url: str


class BroadcastSettingsRepository:
    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path).expanduser().resolve()

    def _connect(self) -> sqlite3.Connection:
        uri = f"file:{self.db_path.as_posix()}?mode=rw"
        conn = sqlite3.connect(uri, uri=True, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    @staticmethod
    def _read_values(conn: sqlite3.Connection) -> dict[str, str]:
        rows = conn.execute(
            "SELECT setting_key, setting_value FROM publisher_settings_v3"
        ).fetchall()
        return {str(row["setting_key"]): str(row["setting_value"]) for row in rows}

    @staticmethod
    def _upsert(conn: sqlite3.Connection, key: str, value: str) -> None:
        conn.execute(
            """INSERT INTO publisher_settings_v3(setting_key, setting_value, updated_at)
               VALUES (?, ?, CURRENT_TIMESTAMP)
               ON CONFLICT(setting_key) DO UPDATE SET
                   setting_value=excluded.setting_value,
                   updated_at=CURRENT_TIMESTAMP""",
            (key, value),
        )

    def ensure_defaults(self) -> None:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            for key, value in DEFAULTS.items():
                conn.execute(
                    "INSERT OR IGNORE INTO publisher_settings_v3(setting_key, setting_value) VALUES (?, ?)",
                    (key, value),
                )
            conn.commit()

    def get_all(self) -> dict[str, str]:
        with self._connect() as conn:
            return self._read_values(conn)

    def set(self, key: str, value: object) -> None:
        if key not in DEFAULTS:
            raise ValueError(f"unsupported_broadcast_setting:{key}")
        with self._connect() as conn:
            self._upsert(conn, key, str(value))
            conn.commit()

    def claim_scheduled_send(self, *, local_date: str, local_hhmm: str) -> bool:
        """Atomically claim today's scheduled send before touching Telegram."""
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            values = {**DEFAULTS, **self._read_values(conn)}
            if not _truthy(values[KEY_ENABLED]):
                conn.rollback()
                return False
            if str(values[KEY_TIME]).strip() != local_hhmm:
                conn.rollback()
                return False
            if str(values[KEY_LAST_ATTEMPT_DATE]).strip() == local_date:
                conn.rollback()
                return False
            self._upsert(conn, KEY_LAST_ATTEMPT_DATE, local_date)
            conn.commit()
            return True

    def mark_scheduled_sent(self, local_date: str) -> None:
        self.set(KEY_LAST_SENT_DATE, local_date)

    def log_delivery(
        self,
        *,
        trigger_type: str,
        template_key: str,
        channel_chat_id: str,
        local_date: str,
        status: str,
        channel_message_id: int | None = None,
        error_text: str = "",
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO publisher_broadcast_log_v3(
                       trigger_type, template_key, channel_chat_id,
                       channel_message_id, status, local_date, error_text
                   ) VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    str(trigger_type), str(template_key), str(channel_chat_id),
                    channel_message_id, str(status), str(local_date), str(error_text)[:2000],
                ),
            )
            conn.commit()


JsonFetcher = Callable[[str], Mapping[str, object]]


def _fetch_json(url: str) -> Mapping[str, object]:
    request = Request(url, headers={"User-Agent": "QiaolianRentalBot/3.0"})
    with urlopen(request, timeout=6) as response:
        value = json.loads(response.read().decode("utf-8"))
    if not isinstance(value, Mapping):
        raise ValueError("broadcast_json_root_must_be_object")
    return value


class LiveDailyInfoBuilder:
    def __init__(
        self,
        *,
        repo_root: str | Path,
        timezone_name: str = "Asia/Phnom_Penh",
        fetch_json: JsonFetcher = _fetch_json,
    ):
        self.repo_root = Path(repo_root).expanduser().resolve()
        self.timezone = ZoneInfo(timezone_name)
        self.fetch_json = fetch_json

    def _weather_texts(self) -> dict[str, str]:
        path = self.repo_root / "assets" / "v2_2" / "weather_reminder_templates.json"
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            order = tuple(payload.get("fallback_order") or ())
            if order != _WEATHER_FALLBACK_ORDER:
                raise ValueError("weather_fallback_order_mismatch")
            rows = payload.get("weather_reminder_templates") or []
            by_id = {
                str(row.get("id") or ""): str(row.get("text") or "").strip()
                for row in rows
                if isinstance(row, Mapping)
            }
            if all(by_id.get(key) for key in order):
                return by_id
        except Exception:
            pass
        return dict(_FALLBACK_WEATHER_TEXT)

    def _select_weather_reminder(self, code: int, high: float, low: float, rain: int) -> str:
        texts = self._weather_texts()
        predicates = {
            "storm": code in {95, 96, 99},
            "rain_heavy": code in {65, 67, 82, 86} or rain >= 70,
            "rain_light": code in {61, 63, 80, 81} or rain >= 40,
            "rain_possible": code in {51, 53, 55, 56, 57} or rain >= 20,
            "hot": high >= 35,
            "sunny": code in {0, 1} and high >= 32,
            "humid": code in {2, 3} and high >= 28,
            "good": code in {0, 1, 2} and 25 <= high < 32,
            "stable": True,
        }
        for key in _WEATHER_FALLBACK_ORDER:
            if predicates[key]:
                return texts[key]
        return texts["stable"]

    def build(self, *, fx_offset: float = 0.0, now: datetime | None = None) -> str:
        current = now.astimezone(self.timezone) if now is not None else datetime.now(self.timezone)
        weekday = "一二三四五六日"[current.weekday()]
        weather_line = "🌤 天气：暂时无法获取"
        weather_note = _FALLBACK_WEATHER_TEXT["stable"]
        fx_line = "💵 美元/人民币：暂时无法获取"

        weather_url = (
            "https://api.open-meteo.com/v1/forecast?latitude=11.5564&longitude=104.9282"
            "&daily=weather_code,temperature_2m_max,temperature_2m_min,precipitation_probability_max"
            "&timezone=Asia%2FPhnom_Penh&forecast_days=1"
        )
        try:
            weather = self.fetch_json(weather_url)
            daily = weather.get("daily") or {}
            if not isinstance(daily, Mapping):
                raise ValueError("weather_daily_missing")
            code = int((daily.get("weather_code") or [None])[0])
            high = float((daily.get("temperature_2m_max") or [0])[0])
            low = float((daily.get("temperature_2m_min") or [0])[0])
            rain = int(round(float((daily.get("precipitation_probability_max") or [0])[0])))
            weather_line = f"🌤 天气：{_WEATHER_LABELS.get(code, '天气多变')} {low:.0f}–{high:.0f}℃｜降雨 {rain}%"
            weather_note = self._select_weather_reminder(code, high, low, rain)
        except Exception:
            pass

        try:
            exchange = self.fetch_json("https://open.er-api.com/v6/latest/USD")
            rates = exchange.get("rates") or {}
            if not isinstance(rates, Mapping):
                raise ValueError("fx_rates_missing")
            market_cny = float(rates.get("CNY"))
            display_cny = max(0.0, market_cny + _clamp_fx(fx_offset))
            fx_line = f"💵 美元/人民币：1 USD ≈ {display_cny:.2f} CNY"
        except Exception:
            pass

        weather_display = weather_line.replace("🌤 天气：", "")
        fx_display = fx_line.replace("💵 美元/人民币：", "")
        hundred_line = ""
        if "1 USD ≈" in fx_display:
            try:
                rate = float(fx_display.split("≈", 1)[1].split("CNY", 1)[0].strip())
                hundred_line = f"\n100 USD ≈ {rate * 100:.0f} CNY"
            except (ValueError, IndexError):
                pass
        return (
            "<b>☀️ 侨联地产｜早安金边</b>\n"
            f"📅 {current:%Y.%m.%d}｜星期{weekday}\n\n"
            "<b>🌦 今日天气</b>\n"
            f"{weather_display}\n\n"
            "<b>💱 今日汇率</b>\n"
            f"{fx_display}{hundred_line}\n\n"
            "<b>📌 今日提醒</b>\n"
            f"{weather_note}\n\n"
            "<i>天气与汇率仅供参考，以实时信息及实际牌价为准。</i>"
        )


class BroadcastService:
    def __init__(
        self,
        *,
        repository: BroadcastSettingsRepository,
        repo_root: str | Path,
        user_bot_username: str,
        timezone_name: str = "Asia/Phnom_Penh",
        live_builder: LiveDailyInfoBuilder | None = None,
    ):
        self.repository = repository
        self.user_bot_username = str(user_bot_username or "").strip().lstrip("@")
        if not self.user_bot_username:
            raise ValueError("broadcast_user_bot_username_required")
        self.timezone = ZoneInfo(timezone_name)
        self.live_builder = live_builder or LiveDailyInfoBuilder(
            repo_root=repo_root,
            timezone_name=timezone_name,
        )

    def ensure_defaults(self) -> None:
        self.repository.ensure_defaults()

    def config(self) -> BroadcastConfig:
        values = {**DEFAULTS, **self.repository.get_all()}
        template = str(values[KEY_TEMPLATE]).strip().lower()
        if template not in TEMPLATE_KEYS:
            template = "live"
        button = str(values[KEY_BUTTON]).strip().lower()
        if button not in BUTTON_KEYS:
            button = "none"
        send_time = str(values[KEY_TIME]).strip()
        if parse_hhmm(send_time) is None:
            send_time = "09:30"
        return BroadcastConfig(
            enabled=_truthy(values[KEY_ENABLED]),
            send_time=send_time,
            template_key=template,
            custom_html=str(values[KEY_CUSTOM_HTML]),
            fx_offset=_clamp_fx(values[KEY_FX_OFFSET]),
            button_key=button,
            last_scheduled_attempt_date=str(values[KEY_LAST_ATTEMPT_DATE]),
            last_scheduled_sent_date=str(values[KEY_LAST_SENT_DATE]),
        )

    def set_enabled(self, enabled: bool) -> None:
        self.repository.set(KEY_ENABLED, "1" if enabled else "0")

    def set_time(self, value: str) -> str:
        parsed = parse_hhmm(value)
        if parsed is None:
            raise ValueError("broadcast_invalid_hhmm")
        normalized = f"{parsed[0]:02d}:{parsed[1]:02d}"
        self.repository.set(KEY_TIME, normalized)
        return normalized

    def set_template(self, key: str) -> None:
        clean = str(key or "").strip().lower()
        if clean not in TEMPLATE_KEYS:
            raise ValueError("broadcast_unknown_template")
        self.repository.set(KEY_TEMPLATE, clean)

    def set_custom_html(self, text: str) -> None:
        clean = str(text or "").strip()
        if not clean:
            raise ValueError("broadcast_custom_html_required")
        self.repository.set(KEY_CUSTOM_HTML, clean[:12000])
        self.repository.set(KEY_TEMPLATE, "custom")

    def set_fx_offset(self, value: float) -> float:
        normalized = _clamp_fx(value)
        self.repository.set(KEY_FX_OFFSET, f"{normalized:.2f}")
        return normalized

    def set_button(self, key: str) -> None:
        clean = str(key or "").strip().lower()
        if clean not in BUTTON_KEYS:
            raise ValueError("broadcast_unknown_button")
        self.repository.set(KEY_BUTTON, clean)

    def template_title(self, key: str | None = None) -> str:
        selected = str(key or self.config().template_key).strip().lower()
        if selected == "live":
            return "🌤 每日天气汇率"
        if selected == "custom":
            return "✏️ 自定义文案"
        return TEMPLATES.get(selected, (selected, ""))[0]

    def body(self, key: str | None = None, *, now: datetime | None = None) -> str:
        config = self.config()
        selected = str(key or config.template_key).strip().lower()
        if selected == "live":
            return self.live_builder.build(fx_offset=config.fx_offset, now=now)
        if selected == "custom":
            if not config.custom_html.strip():
                raise ValueError("broadcast_custom_html_empty")
            return config.custom_html.strip()
        template = TEMPLATES.get(selected)
        if template is None:
            raise ValueError("broadcast_unknown_template")
        return template[1]

    def footer_rows(self, key: str | None = None) -> tuple[tuple[BroadcastButton, ...], ...]:
        selected = str(key or self.config().button_key).strip().lower()
        if selected == "none":
            return ()
        if selected not in BUTTON_KEYS:
            raise ValueError("broadcast_unknown_button")
        base = f"https://t.me/{self.user_bot_username}?start="
        buttons = {
            "find": BroadcastButton("🔍 帮我找房", base + "find_home"),
            "latest": BroadcastButton("🏠 最新房源", base + "latest"),
            "contact": BroadcastButton("💬 联系中文顾问", base + "advisor"),
        }
        if selected == "combo":
            return ((buttons["find"], buttons["latest"]), (buttons["contact"],))
        return ((buttons[selected],),)

    def local_now(self) -> datetime:
        return datetime.now(self.timezone)

    def claim_scheduled_due(self, now: datetime | None = None) -> tuple[bool, str]:
        current = now.astimezone(self.timezone) if now is not None else self.local_now()
        local_date = current.date().isoformat()
        local_hhmm = current.strftime("%H:%M")
        claimed = self.repository.claim_scheduled_send(
            local_date=local_date,
            local_hhmm=local_hhmm,
        )
        return claimed, local_date

    def mark_scheduled_sent(self, local_date: str) -> None:
        self.repository.mark_scheduled_sent(local_date)


__all__ = [
    "BUTTON_KEYS",
    "BUTTON_LABELS",
    "BroadcastButton",
    "BroadcastConfig",
    "BroadcastService",
    "BroadcastSettingsRepository",
    "DDL",
    "LiveDailyInfoBuilder",
    "TEMPLATES",
    "TEMPLATE_KEYS",
    "parse_hhmm",
]
