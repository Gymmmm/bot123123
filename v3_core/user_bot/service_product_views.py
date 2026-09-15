"""Production V3 tenant-service entry; all privileged actions sit behind binding check."""
from __future__ import annotations
from .service_views import ServiceChoice,ServiceView

def service_home_view()->ServiceView:
    return ServiceView('service_home','🛠 <b>入住服务</b>\n\n已通过侨联入住的租客，可从这里查看租约、报修、物业沟通、租赁服务指南、续租和退租。\n\n系统会先核对当前 Telegram 账号是否存在有效租约绑定。',((ServiceChoice('🏠 进入我的入住服务','v3u:service:tenant'),),(ServiceChoice('💬 中文顾问','v3u:home:contact'),ServiceChoice('🏠 返回首页','v3u:t:home'))))

__all__=['service_home_view']
