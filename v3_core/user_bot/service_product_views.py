"""Fallback tenant-service gate used when binding context is unavailable."""
from __future__ import annotations

from .service_views import ServiceChoice, ServiceView


def service_home_view() -> ServiceView:
    return ServiceView(
        kind="service_home",
        text=(
            "🛠 <b>入住服务</b>\n\n"
            "这边还没有显示你的住房信息。\n\n"
            "如果已经通过侨联入住，但这里还没有显示，可以联系中文顾问处理。\n"
            "还没租房的话，可以先了解入住之后侨联怎么服务，或继续找房。"
        ),
        rows=(
            (ServiceChoice("💬 中文顾问", "v3u:home:contact"),),
            (ServiceChoice("🛡 看租后服务", "v3u:home:rental"), ServiceChoice("🔍 开始找房", "v3u:home:search")),
            (ServiceChoice("⬅️ 回首页", "v3u:t:home"),),
        ),
    )


__all__ = ["service_home_view"]
