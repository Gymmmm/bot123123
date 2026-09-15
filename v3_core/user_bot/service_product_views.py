"""Fallback tenant-service gate used when binding context is unavailable."""
from __future__ import annotations

from .service_views import ServiceChoice, ServiceView


def service_home_view() -> ServiceView:
    return ServiceView(
        kind="service_home",
        text=(
            "🛠 <b>入住服务</b>\n\n"
            "入住服务会按当前有效租约开放。\n\n"
            "如果当前账号还没有绑定有效租约，可以先查看公开租赁服务、继续找房或联系中文顾问。"
        ),
        rows=(
            (ServiceChoice("🛠 检查当前租约", "v3u:service:tenant"),),
            (ServiceChoice("📄 租赁服务", "v3u:home:rental"), ServiceChoice("🔍 开始找房", "v3u:home:search")),
            (ServiceChoice("💬 中文顾问", "v3u:home:contact"),),
            (ServiceChoice("🏠 返回首页", "v3u:t:home"),),
        ),
    )


__all__ = ["service_home_view"]
