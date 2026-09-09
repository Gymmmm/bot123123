"""Locked-copy V3 views for current tenant-service navigation."""
from __future__ import annotations

from dataclasses import dataclass
from html import escape as he

from .service_flow import ServiceRequestDraft


@dataclass(frozen=True)
class ServiceChoice:
    label: str
    callback_data: str


@dataclass(frozen=True)
class ServiceView:
    kind: str
    text: str
    rows: tuple[tuple[ServiceChoice, ...], ...]


def service_home_view() -> ServiceView:
    return ServiceView(
        kind="service_home",
        text=(
            "🛠 <b>入住服务</b>\n\n"
            "已经租下来的事，从这里找我们。\n"
            "报修、物业、搬家，点一项就行。"
        ),
        rows=(
            (
                ServiceChoice("🔧 报修", "v3u:service:repair"),
                ServiceChoice("🏢 找物业", "v3u:service:property"),
            ),
            (
                ServiceChoice("🚚 搬家", "v3u:assure:moving"),
                ServiceChoice("📍 周边", "v3u:service:local"),
            ),
            (
                ServiceChoice("💬 联系我们", "v3u:home:contact"),
                ServiceChoice("🏠 返回首页", "v3u:t:home"),
            ),
        ),
    )


def repair_home_view() -> ServiceView:
    return ServiceView(
        kind="repair_home",
        text="🔧 <b>设备报修</b>\n\n请选择出现问题的设备。",
        rows=(
            (
                ServiceChoice("❄️ 空调", "v3u:service:issue:repair_ac"),
                ServiceChoice("🚿 热水器", "v3u:service:issue:repair_water"),
            ),
            (
                ServiceChoice("🧺 洗衣机", "v3u:service:issue:repair_washer"),
                ServiceChoice("🧊 冰箱", "v3u:service:issue:repair_fridge"),
            ),
            (
                ServiceChoice("📶 网络", "v3u:service:issue:repair_network"),
                ServiceChoice("🔐 门锁/门禁", "v3u:service:issue:repair_door"),
            ),
            (ServiceChoice("🔧 其他设备", "v3u:service:issue:repair_other"),),
            (ServiceChoice("⬅️ 返回", "v3u:home:service"),),
        ),
    )
