"""Independent 7-day Telegram marketing broadcast plan.

Daily weather/FX remains owned by broadcast.py.  This module adds the 18:30
weekly marketing loop without changing collector, parser, listing or frozen
publication policy.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from html import escape
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
        "mon", "📊 本周租房库存",
        "📊 <b>本周金边租房库存</b>\n\n当前公开库存暂时没有足够数据，直接进入最新房源查看实时房态。",
        (("🔍 开始找房", "find"), ("💬 中文顾问", "advisor"), ("📢 最新房源", "latest")),
    ),
    MarketingTemplate(
        "tue", "💰 预算能租什么",
        "💰 <b>金边租房预算参考</b>\n\n按侨联当前公开库存统计；库存不足时不生成价格结论。",
        (("💬 中文顾问", "advisor"), ("🛎️ 侨联服务", "service"), ("🔍 开始找房", "find")),
    ),
    MarketingTemplate(
        "wed", "📍 当前房源区域",
        "📍 <b>当前房源主要分布</b>\n\n只按侨联当前已公开房源统计，不代表整个金边市场成交量。",
        (("🔍 开始找房", "find"), ("💬 中文顾问", "advisor")),
    ),
    MarketingTemplate(
        "thu", "🆕 近7天上新",
        "🆕 <b>近7天房源更新</b>\n\n只统计已经公开发布、当前仍有效的租房房源。",
        (("💬 中文顾问", "advisor"), ("🔍 开始找房", "find")),
    ),
    MarketingTemplate(
        "fri", "📋 周末看房准备",
        "📋 <b>周末看房前先确认</b>\n\n区域、预算、户型、入住时间确定后，再按当前房态安排看房，减少无效往返。",
        (("💬 中文顾问", "advisor"), ("🔍 开始找房", "find")),
    ),
    MarketingTemplate(
        "sat", "🏠 周末可看房源",
        "🏠 <b>周末找房</b>\n\n先从当前公开房源里筛选，再确认实时房态和可看时间。",
        (("🔍 开始找房", "find"), ("🛎️ 侨联服务", "service"), ("💬 中文顾问", "advisor")),
    ),
    MarketingTemplate(
        "sun", "📈 本周房源复盘",
        "📈 <b>本周房源复盘</b>\n\n用本周真实公开库存做复盘，不使用虚构热度、成交量或回报率。",
        (("🛎️ 侨联服务", "service"), ("💬 中文顾问", "advisor")),
    ),
)


@dataclass(frozen=True)
class InventoryMarketingSnapshot:
    total: int = 0
    active: int = 0
    reserved: int = 0
    new_7d: int = 0
    min_rent: int | None = None
    max_rent: int | None = None
    budget_under_500: int = 0
    budget_500_700: int = 0
    budget_701_1000: int = 0
    budget_over_1000: int = 0
    top_areas: tuple[tuple[str, int], ...] = ()


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

    def _inventory_snapshot(self) -> InventoryMarketingSnapshot:
        """Aggregate only currently published Telegram rent inventory."""
        sql = """
            SELECT l.listing_id,l.inventory_status,
                   COALESCE(NULLIF(TRIM(l.public_location_display), ''), TRIM(l.canonical_area_display)) AS area,
                   o.monthly_rent_usd,MAX(pi.published_at) AS latest_published
            FROM listings_v3 l
            JOIN listing_offers o
              ON o.listing_id=l.listing_id AND o.offer_type='rent'
             AND o.offer_status='active' AND o.publication_policy='telegram_rent'
            JOIN publication_instances pi
              ON pi.listing_id=l.listing_id AND pi.offer_id=o.offer_id
             AND pi.platform='telegram' AND pi.publish_status='published'
            JOIN publication_packages_v3 pp
              ON pp.package_id=pi.package_id AND pp.listing_id=l.listing_id
             AND pp.offer_id=o.offer_id AND pp.status IN ('approved','published')
            WHERE l.data_status='current'
              AND l.inventory_status IN ('active','reserved')
              AND TRIM(COALESCE(l.public_listing_id,''))<>''
            GROUP BY l.listing_id,l.inventory_status,area,o.monthly_rent_usd
        """
        with self._connect() as conn:
            rows = conn.execute(sql).fetchall()
        now_utc = datetime.now(timezone.utc)
        area_counts: dict[str, int] = {}
        rents: list[int] = []
        active = reserved = new_7d = 0
        bands = [0, 0, 0, 0]
        for row in rows:
            status = str(row["inventory_status"] or "").strip().lower()
            active += int(status == "active")
            reserved += int(status == "reserved")
            area = str(row["area"] or "").strip()
            if area:
                area_counts[area] = area_counts.get(area, 0) + 1
            try:
                rent = int(row["monthly_rent_usd"])
            except (TypeError, ValueError):
                rent = 0
            if rent > 0:
                rents.append(rent)
                if rent < 500: bands[0] += 1
                elif rent <= 700: bands[1] += 1
                elif rent <= 1000: bands[2] += 1
                else: bands[3] += 1
            raw_published = str(row["latest_published"] or "").strip()
            if raw_published:
                try:
                    published = datetime.fromisoformat(raw_published.replace("Z", "+00:00"))
                    if published.tzinfo is None:
                        published = published.replace(tzinfo=timezone.utc)
                    if now_utc - published.astimezone(timezone.utc) <= timedelta(days=7):
                        new_7d += 1
                except ValueError:
                    pass
        top_areas = tuple(sorted(area_counts.items(), key=lambda item: (-item[1], item[0]))[:4])
        return InventoryMarketingSnapshot(
            total=len(rows), active=active, reserved=reserved, new_7d=new_7d,
            min_rent=min(rents) if rents else None, max_rent=max(rents) if rents else None,
            budget_under_500=bands[0], budget_500_700=bands[1],
            budget_701_1000=bands[2], budget_over_1000=bands[3], top_areas=top_areas,
        )

    @staticmethod
    def _money(value: int | None) -> str:
        return f"${int(value):,}" if value else "—"

    def _data_body(self, key: str, fallback: str) -> str:
        s = self._inventory_snapshot()
        if s.total <= 0:
            return fallback
        area_lines = "\n".join(f"• {escape(area)}：{count} 套" for area, count in s.top_areas) or "• 区域信息待补全"
        rent_range = f"{self._money(s.min_rent)}–{self._money(s.max_rent)}/月" if s.min_rent and s.max_rent else "价格信息待补全"
        bodies = {
            "mon": "📊 <b>本周金边租房库存</b>\n\n"
                   f"当前公开有效：<b>{s.total} 套</b>\n可租：{s.active} 套｜已预留：{s.reserved} 套\n"
                   f"当前月租范围：{rent_range}\n\n<b>房源较多的区域</b>\n{area_lines}\n\n"
                   "<i>数据：侨联地产当前已公开租房库存；房态会变化，以咨询时实时确认为准。</i>",
            "tue": "💰 <b>现在这个预算，侨联有多少套？</b>\n\n"
                   f"$500 以下：{s.budget_under_500} 套\n$500–700：{s.budget_500_700} 套\n"
                   f"$701–1,000：{s.budget_701_1000} 套\n$1,000 以上：{s.budget_over_1000} 套\n\n"
                   f"当前公开库存共 {s.total} 套。\n<i>这是侨联当前公开库存，不代表全金边市场均价或成交价。</i>",
            "wed": "📍 <b>侨联当前房源主要在哪些区域？</b>\n\n"
                   f"{area_lines}\n\n当前公开有效：{s.total} 套\n"
                   "<i>按当前公开库存数量排序，不使用“最热门”等无法验证的市场结论。</i>",
            "thu": "🆕 <b>近7天房源更新</b>\n\n"
                   f"近7天新公开 / 重新发布且当前有效：<b>{s.new_7d} 套</b>\n"
                   f"当前公开有效：{s.total} 套\n当前月租范围：{rent_range}\n\n"
                   "找房时直接给区域 + 预算 + 户型 + 入住时间，按当前库存筛。",
            "fri": "📋 <b>周末看房，先把这4项定下来</b>\n\n"
                   "1. 区域\n2. 月预算\n3. 户型\n4. 预计入住时间\n\n"
                   f"侨联当前公开有效房源：{s.total} 套。\n先筛当前库存，再确认房态和可看时间，避免无效往返。",
            "sat": "🏠 <b>周末找房库存</b>\n\n"
                   f"当前可租：<b>{s.active} 套</b>\n已预留：{s.reserved} 套\n月租范围：{rent_range}\n\n"
                   f"{area_lines}\n\n先选条件，再确认今天实际能看的房源。",
            "sun": "📈 <b>本周房源数据</b>\n\n"
                   f"当前公开有效：{s.total} 套\n近7天更新：{s.new_7d} 套\n"
                   f"可租：{s.active} 套｜已预留：{s.reserved} 套\n月租范围：{rent_range}\n\n"
                   "<i>全部来自侨联当前公开库存；不把挂牌库存包装成成交量、热度或投资回报率。</i>",
        }
        return bodies.get(key, fallback)

    def template(self, weekday: int | None = None) -> MarketingTemplate:
        index = self.local_now().weekday() if weekday is None else int(weekday)
        base = TEMPLATES[index % 7]
        custom = self._get_optional(f"marketing_body_{base.key}").strip()
        body = custom or self._data_body(base.key, base.body)
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
