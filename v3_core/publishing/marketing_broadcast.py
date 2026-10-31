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

SALE_SITE_URL = "https://qiaolian-jinbian-sale.vercel.app/?utm_source=telegram&utm_medium=marketing&utm_campaign=weekly_sale"

KEY_ENABLED = "marketing_broadcast_enabled"
KEY_TIME = "marketing_broadcast_time"
KEY_LAST_ATTEMPT = "marketing_broadcast_last_attempt_date"
KEY_LAST_SENT = "marketing_broadcast_last_sent_date"
DEFAULTS = {KEY_ENABLED: "1", KEY_TIME: "17:30", KEY_LAST_ATTEMPT: "", KEY_LAST_SENT: ""}


@dataclass(frozen=True)
class MarketingTemplate:
    key: str
    title: str
    body: str
    buttons: tuple[tuple[str, str], ...]


TEMPLATES = (
    MarketingTemplate('mon', '🏠 租房费用', '🏠 <b>金边租房，先把费用问清楚</b>\n\n同样是 $800/月，实际入住支出可能不同。\n\n看房前确认：\n• 押金和付款周期\n• 水电、物业及停车费用\n• 合同期限和最早入住时间\n\n侨联先帮你核对条件，再安排合适的房源。\n\n<b>找房需求：区域＋预算＋户型＋入住时间</b>\n\n💎 <b>侨联地产｜您在金边的自己人</b>', (("🏠 我要租房", "find"), ("🏢 我要买房", "sale"), ("💬 中文顾问", "advisor"))),
    MarketingTemplate('tue', '📌 看房先核对', '📌 <b>一套房值不值得看？</b>\n\n照片只能帮你初筛，真正要核对的是：\n① 目前房态是否可预约\n② 月租以外还有哪些费用\n③ 押金、租期是否符合预期\n④ 位置是否方便日常通勤\n\n先看清房源信息，再决定是否到现场。\n\n💎 <b>侨联地产｜您在金边的自己人</b>', (("🏠 我要租房", "find"), ("🏢 我要买房", "sale"), ("💬 中文顾问", "advisor"))),
    MarketingTemplate('wed', '🏙 金边选区域', '🏙 <b>BKK1、钻石岛、TK 怎么选？</b>\n\n📍 BKK1：餐饮与商业配套集中，部分路段高峰较拥堵。\n📍 钻石岛：新公寓较多，具体交通及物业配套因项目而异。\n📍 TK（堆谷）：住宅、学校及日常商业分布较多。\n\n区域没有绝对的好坏。把常去地点、预算和户型告诉侨联，先按你的生活路线筛选。\n\n💎 <b>侨联地产｜您在金边的自己人</b>', (("🏠 我要租房", "find"), ("🏢 我要买房", "sale"), ("💬 中文顾问", "advisor"))),
    MarketingTemplate('thu', '📷 看实拍细节', '📷 <b>看房源照片，别只看客厅</b>\n\n选房时，还要关注：\n• 卧室采光和窗外环境\n• 厨房、卫浴及晾衣空间\n• 家具家电的实际状态\n• 楼层、公区与噪音\n\n先看完整实拍，感兴趣再预约核对现场情况。\n\n💎 <b>侨联地产｜您在金边的自己人</b>', (("🏠 我要租房", "find"), ("🏢 我要买房", "sale"), ("💬 中文顾问", "advisor"))),
    MarketingTemplate('fri', '🎥 远程视频带看', '🎥 <b>人不在金边，也能先了解房子</b>\n\n可以预约顾问到现场进行 1 对 1 视频带看，重点查看：\n• 卧室、厨房、卫浴和阳台\n• 家具家电及实际采光\n• 电梯、停车及公共区域\n\n在房源详情选择「在线预约」→「视频带看」，提交希望的时段。顾问确认后再安排连线。\n\n💎 <b>侨联地产｜您在金边的自己人</b>', (("🏠 我要租房", "find"), ("🏢 我要买房", "sale"), ("💬 中文顾问", "advisor"))),
    MarketingTemplate('sat', '⚖️ 比较两套房', '⚖️ <b>两套房怎么选？别只比租金</b>\n\n一套 $750、一套 $850，未必便宜的更合适。\n\n一起比较：\n• 通勤与周边生活便利度\n• 押金、水电和物业费用\n• 家具配置、停车及厨房条件\n• 入住时间和实际房态\n\n把候选房源发给中文顾问，按同一标准逐项比较。\n\n💎 <b>侨联地产｜您在金边的自己人</b>', (("🏠 我要租房", "find"), ("🏢 我要买房", "sale"), ("💬 中文顾问", "advisor"))),
    MarketingTemplate('sun', '🏠 本周找房需求', '🏠 <b>不知道从哪里开始找房？</b>\n\n只要提供四项信息：\n📍 区域或常去地点\n💰 每月预算\n🏠 需要几房\n🗓 预计入住时间\n\n侨联先按条件筛选现有房源。如果没有完全匹配，也会说明哪些条件可以调整。\n\n<b>先把需求说清楚，再去看房。</b>\n\n💎 <b>侨联地产｜您在金边的自己人</b>', (("🏠 我要租房", "find"), ("🏢 我要买房", "sale"), ("💬 中文顾问", "advisor"))),
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
        area_lines = "\n".join(f"• {escape(area)}：{count} 套" for area, count in s.top_areas) or "可直接告诉顾问你常去的地点。"
        bodies = {
            "mon": fallback + f"\n\n当前可咨询房源：<b>{s.total} 套</b>。房态与费用以顾问核实为准。",
            "tue": fallback + f"\n\n<b>按月租看现有选择</b>\n低于$500：{s.budget_under_500} 套\n$500–700：{s.budget_500_700} 套\n高于$700、不超过$1,000：{s.budget_701_1000} 套\n高于$1,000：{s.budget_over_1000} 套\n以上仅为侨联当前可咨询房源。",
            "wed": fallback + f"\n\n<b>当前可参考的区域</b>\n{area_lines}",
            "thu": fallback + f"\n\n近7天新公开或重新发布、目前仍可咨询：<b>{s.new_7d} 套</b>。",
            "fri": fallback,
            "sat": fallback,
        }
        return bodies.get(key, fallback)

    def template(self, weekday: int | None = None) -> MarketingTemplate:
        index = self.local_now().weekday() if weekday is None else int(weekday)
        base = TEMPLATES[index % 7]
        custom = self._get_optional(f"marketing_body_{base.key}").strip()
        body = custom or (base.body if base.key == "sun" else self._data_body(base.key, base.body))
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
        locked = {
            key: ((find,), (advisor,)) for key in ("mon", "tue", "wed", "fri", "sat")
        }
        locked["thu"] = ((latest or find,), (advisor,))
        locked["sun"] = ((service,), (advisor,))
        sale = BroadcastButton("🏠 查看出售房源", SALE_SITE_URL)
        rent = BroadcastButton("🏠 我要租房", find.url)
        buy = BroadcastButton("🏢 我要买房", SALE_SITE_URL)
        # A compact two-row CTA, stable for all seven weekday campaigns.
        if selected == "none":
            return ()
        return ((rent, buy), (advisor,))

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
