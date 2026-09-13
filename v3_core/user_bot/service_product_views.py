"""Product-layer tenant service hub for the production V3 User Bot.

This module intentionally reuses existing V3 callbacks so the upgraded public
navigation does not create a second repair, assurance, contact, or nearby flow.
"""
from __future__ import annotations

from .service_views import ServiceChoice, ServiceView


def service_home_view() -> ServiceView:
    return ServiceView(
        kind="service_home",
        text=(
            "🛡 <b>入住服务</b>\n\n"
            "入住才是侨联服务的开始。\n\n"
            "通过侨联租房后，整个租期内的住房问题，都可以从这里处理。\n"
            "已绑定房屋信息的流程会继续沿用现有 V3 档案，不需要重复说明。"
        ),
        rows=(
            (
                ServiceChoice("📄 租赁服务指南", "v3u:home:rental"),
                ServiceChoice("🔧 报修与维护", "v3u:service:repair"),
            ),
            (
                ServiceChoice("🏢 物业沟通", "v3u:service:property"),
                ServiceChoice("🔄 续租 / 换房", "v3u:home:contact"),
            ),
            (
                ServiceChoice("🔐 退租与押金", "v3u:assure:deposit"),
                ServiceChoice("🧭 周边服务", "v3u:service:local"),
            ),
            (
                ServiceChoice("💬 联系顾问", "v3u:home:contact"),
                ServiceChoice("🏠 返回首页", "v3u:t:home"),
            ),
        ),
    )


__all__ = ["service_home_view"]
