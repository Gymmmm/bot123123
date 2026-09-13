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
            "签约不是服务的结束。\n\n"
            "通过侨联租房后，租期内遇到住房问题，都可以从这里继续处理。\n\n"
            "找房中的客户也可以先了解侨联签约后的服务范围。"
        ),
        rows=(
            (
                ServiceChoice("🔧 报修与维护", "v3u:service:repair"),
                ServiceChoice("🏢 物业沟通", "v3u:service:property"),
            ),
            (
                ServiceChoice("🔄 续租 / 退租", "v3u:home:contact"),
                ServiceChoice("🧭 周边服务", "v3u:service:local"),
            ),
            (ServiceChoice("📄 租赁服务指南", "v3u:home:rental"),),
            (
                ServiceChoice("💬 联系顾问", "v3u:home:contact"),
                ServiceChoice("🏠 返回首页", "v3u:t:home"),
            ),
        ),
    )


__all__ = ["service_home_view"]
