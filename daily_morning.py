#!/usr/bin/env python3
"""
daily_morning.py — 小彭·金边早报生成器

对外暴露：
  build_morning_text(db_path, channel_url) -> str   构建早报正文
  send_morning_report(bot, channel_id, db_path, channel_url) -> None  发送
"""
from __future__ import annotations

import json
import logging
import sqlite3
import urllib.request
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

_TZ = ZoneInfo("Asia/Phnom_Penh")
_UA = {"User-Agent": "Mozilla/5.0"}


# ─── 天气 ─────────────────────────────────────────────────────────────────────

def _weather_icon(desc: str) -> str:
    desc = desc.lower()
    if any(x in desc for x in ("thunder", "storm", "雷")):
        return "⛈"
    if any(x in desc for x in ("rain", "drizzle", "shower")):
        return "🌧"
    if "cloud" in desc or "overcast" in desc:
        return "⛅"
    return "☀️"


def _weather_action(desc: str, icon: str) -> str:
    if icon == "⛈":
        return "下午易雷阵雨，尽量早出门"
    if icon == "🌧":
        return "今日有雨，记得带伞"
    if icon == "⛅":
        return "多云，出行舒适"
    return "晴天，注意防晒"


def fetch_weather() -> dict:
    """返回 {min_c, max_c, icon, action}，失败返回 None。"""
    try:
        req = urllib.request.Request(
            "https://wttr.in/Phnom+Penh?format=j1", headers=_UA
        )
        with urllib.request.urlopen(req, timeout=8) as r:
            d = json.loads(r.read())
        w = d["weather"][0]
        desc = w["hourly"][4]["weatherDesc"][0]["value"]
        icon = _weather_icon(desc)
        return {
            "min_c": w["mintempC"],
            "max_c": w["maxtempC"],
            "icon": icon,
            "action": _weather_action(desc, icon),
        }
    except Exception as e:
        logger.warning("天气获取失败: %s", e)
        return {}


# ─── 汇率 ─────────────────────────────────────────────────────────────────────

def fetch_rates() -> dict:
    """返回 {cny}（1 USD 兑人民币），失败返回 {}。"""
    try:
        req = urllib.request.Request(
            "https://open.er-api.com/v6/latest/USD", headers=_UA
        )
        with urllib.request.urlopen(req, timeout=8) as r:
            d = json.loads(r.read())
        cny = d.get("rates", {}).get("CNY", 0)
        return {"cny": round(cny, 4)}
    except Exception as e:
        logger.warning("汇率获取失败: %s", e)
        return {}


# ─── 房源 ─────────────────────────────────────────────────────────────────────

def fetch_listings(db_path: str, limit: int = 3) -> list[dict]:
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """SELECT listing_id,
                      COALESCE(NULLIF(project,''), NULLIF(title,''), listing_id) AS proj,
                      layout,
                      price,
                      area
               FROM listings
               WHERE status='active'
               ORDER BY updated_at DESC, id DESC
               LIMIT ?""",
            (limit,),
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as e:
        logger.warning("房源获取失败: %s", e)
        return []


# ─── 组装正文 ─────────────────────────────────────────────────────────────────

def _contact_html_link(contact: str) -> str:
    """频道 HTML 正文中用 t.me 链接替代裸 @username（与按钮深链策略一致）。"""
    h = (contact or "").strip().lstrip("@")
    if not h:
        h = "pengqingw"
    return f'<a href="https://t.me/{h}">联系顾问</a>'


def build_morning_text(db_path: str, channel_url: str, contact: str = "@pengqingw") -> str:
    now = datetime.now(_TZ)
    month, day = now.month, now.day

    weather = fetch_weather()
    rates = fetch_rates()
    listings = fetch_listings(db_path)

    lines: list[str] = []

    # 标题
    lines.append(f"☀️ <b>小彭·金边早报｜{month}/{day}</b>")
    lines.append("")
    lines.append("━━━━━━━━━━━━━━")

    # 天气
    lines.append("🌤 <b>今日天气</b>")
    if weather:
        lines.append(
            f"📍 金边：{weather['min_c']}°C – {weather['max_c']}°C"
        )
        lines.append(f"{weather['icon']} {weather['action']}")
    else:
        lines.append("📍 金边：天气数据暂不可用")
    lines.append("")
    lines.append("━━━━━━━━━━━━━━")

    # 汇率
    lines.append("💱 <b>今日汇率</b>")
    if rates:
        lines.append(f"• 1 USD ≈ {rates['cny']} 人民币（CNY）")
    else:
        lines.append("• 汇率数据暂不可用")
    lines.append("")
    lines.append("━━━━━━━━━━━━━━")

    # 房源
    lines.append("🏠 <b>今日房源速递</b>")
    if listings:
        for lst in listings:
            proj = lst.get("proj") or lst.get("listing_id", "")
            layout = lst.get("layout") or ""
            price = lst.get("price") or "面议"
            area = lst.get("area") or ""
            price_str = f"${price}/月" if price and price != "面议" else "面议"
            area_str = f"【{area}】 " if area else ""
            lines.append(
                f"• {area_str}{proj} {layout} {price_str} → {_contact_html_link(contact)}"
            )
    else:
        lines.append("• 更多精选房源持续更新中")

    ch_url = (channel_url or "").rstrip("/")
    if ch_url:
        lines.append(f"\n📢 更多房源 → {ch_url}")
    lines.append("")
    lines.append("━━━━━━━━━━━━━━")
    lines.append("❓ 有问题直接回复，小彭看到就回")

    return "\n".join(lines)


# ─── 发送 ─────────────────────────────────────────────────────────────────────

async def send_morning_report(
    bot,
    channel_id: str,
    db_path: str,
    channel_url: str,
    contact: str = "@pengqingw",
) -> None:
    text = build_morning_text(db_path, channel_url, contact)
    try:
        await bot.send_message(
            chat_id=channel_id,
            text=text,
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
        logger.info("每日早报已发送到频道")
    except Exception as e:
        logger.error("每日早报发送失败: %s", e)


# ─── 本地预览 ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    db = sys.argv[1] if len(sys.argv) > 1 else "/opt/qiaolian_dual_bots/data/qiaolian_dual_bot.db"
    ch_url = sys.argv[2] if len(sys.argv) > 2 else "https://t.me/Jinbianzufanz"
    print(build_morning_text(db, ch_url))
