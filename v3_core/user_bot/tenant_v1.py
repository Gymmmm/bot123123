"""Bound-tenant product surface for the V3 User Bot."""
from __future__ import annotations

from html import escape as he
from pathlib import Path
from typing import Any

from telegram import InputFile

from .assurance_views import assurance_asset_bundle
from .lead_service import LeadUser
from .service_flow import TenantService
from .service_product_views import service_home_view
from .service_views import ServiceChoice, ServiceView

_UNKNOWN = {"", "none", "null", "undefined", "n/a", "na", "-"}


def _safe_text(value: Any, fallback: str = "待补全") -> str:
    text = str(value or "").strip()
    lowered = text.lower()
    if lowered in _UNKNOWN or "1970" in lowered:
        return fallback
    return text


def _public_rows() -> tuple[tuple[ServiceChoice, ...], ...]:
    return (
        (ServiceChoice("💬 中文顾问", "v3u:home:contact"),),
        (ServiceChoice("🛡 看租后服务", "v3u:home:rental"), ServiceChoice("🔍 开始找房", "v3u:home:search")),
        (ServiceChoice("⬅️ 回首页", "v3u:t:home"),),
    )


def tenant_home_view(service: TenantService, user_id: int) -> ServiceView:
    _ = service, user_id
    return service_home_view()


def missing_lease_view() -> ServiceView:
    return ServiceView(
        "tenant_lease_missing",
        "📋 <b>我的租约</b>\n\n暂时没有识别到你的租约信息，请联系中文顾问核对。",
        (
            (ServiceChoice("💬 中文顾问", "v3u:home:contact"),),
            (ServiceChoice("⬅️ 返回入住服务", "v3u:home:service"),),
        ),
    )

def lease_view(service: TenantService, user_id: int) -> ServiceView:
    binding = service.require_active_binding(user_id)
    end = _safe_text(binding.lease_end_date or binding.contract_end_date)
    rent = f"${binding.monthly_rent:g}/月" if float(binding.monthly_rent or 0) > 0 else "待补全"
    deposit = f"{binding.deposit_months}个月" if isinstance(binding.deposit_months, int) and binding.deposit_months > 0 else "待补全"
    day = f"每月 {binding.rent_day} 号" if isinstance(binding.rent_day, int) and 1 <= binding.rent_day <= 31 else "待补全"
    property_name = he(_safe_text(binding.property_name))
    return ServiceView(
        "tenant_lease",
        f"📋 <b>我的租约</b>\n\n"
        f"🏠 房源｜{property_name}\n"
        f"💰 月租｜{rent}\n"
        f"🔐 押金｜{deposit}\n"
        f"📅 交租日｜{day}\n"
        f"⏳ 合同到期｜{he(end)}\n"
        "🟢 当前状态｜有效租约",
        (
            (ServiceChoice("🔄 续租", "v3u:service:tenant_renew"), ServiceChoice("🚪 退租", "v3u:service:tenant_terminate")),
            (ServiceChoice("💬 中文顾问", "v3u:home:contact"),),
            (ServiceChoice("⬅️ 返回入住服务", "v3u:service:tenant"),),
        ),
    )


def renew_view(service: TenantService, user_id: int) -> ServiceView:
    binding = service.require_active_binding(user_id)
    end = he(_safe_text(binding.lease_end_date or binding.contract_end_date))
    prop = he(_safe_text(binding.property_name))
    return ServiceView(
        "tenant_renew",
        f"🔄 <b>续租</b>\n\n🏠 {prop}\n📅 到期日：{end}\n\n"
        "提交后，中文顾问会核对新的租期和价格。\n"
        "这一步是提出需求，不是已经续好。",
        (
            (ServiceChoice("✅ 提交续租需求", "v3u:service:tenant_renew_submit"),),
            (ServiceChoice("💬 中文顾问", "v3u:home:contact"),),
            (ServiceChoice("⬅️ 返回我的租约", "v3u:service:tenant_lease"),),
        ),
    )


def terminate_view(service: TenantService, user_id: int) -> ServiceView:
    binding = service.require_active_binding(user_id)
    end = he(_safe_text(binding.lease_end_date or binding.contract_end_date))
    prop = he(_safe_text(binding.property_name))
    return ServiceView(
        "tenant_terminate",
        f"🚪 <b>退租</b>\n\n🏠 {prop}\n📅 合同到期：{end}\n\n"
        "提交后，顾问会确认通知期、交接、费用和押金核对。\n"
        "这一步是提出需求，不是已经退好。",
        (
            (ServiceChoice("✅ 提交退租需求", "v3u:service:tenant_terminate_submit"),),
            (ServiceChoice("📋 入住交接留档", "v3u:assure:handover"),),
            (ServiceChoice("💬 中文顾问", "v3u:home:contact"),),
            (ServiceChoice("⬅️ 返回我的租约", "v3u:service:tenant_lease"),),
        ),
    )


def guide_view(service: TenantService, user_id: int) -> ServiceView:
    return ServiceView(
        "tenant_guide_updated",
        "🛡 <b>租到房，不代表服务就结束了。</b>\n\n请从租后服务查看入住之后侨联怎么接着服务。",
        ((ServiceChoice("🛡 看租后服务", "v3u:home:rental"),), (ServiceChoice("⬅️ 回首页", "v3u:t:home"), ServiceChoice("💬 中文顾问", "v3u:home:contact"))),
    )


def handover_view(service: TenantService, user_id: int) -> ServiceView:
    return guide_view(service, user_id)


def deposit_view(service: TenantService, user_id: int) -> ServiceView:
    return guide_view(service, user_id)


async def submit_request(*, kind: str, service: TenantService, effects, update, context):
    obj = update.effective_user
    user = LeadUser(
        int(obj.id),
        str(getattr(obj, "username", "") or ""),
        str(getattr(obj, "full_name", "") or ""),
    )
    binding = service.require_active_binding(user.user_id)
    effect = None
    if effects is not None:
        effect = await effects.tenancy_request(
            bot=getattr(context, "bot", None),
            user=user,
            binding=binding,
            kind=kind,
        )
    duplicate = bool(effect and effect.lead.status == "skipped")
    if kind == "renew":
        text = (
            "💬 <b>续租需求正在跟进</b>\n\n之前已经提交过当前租约的续租需求，中文顾问会继续处理。"
            if duplicate else
            "✅ <b>续租需求已提交</b>\n\n中文顾问会核对新的租期和价格，再反馈结果。"
        )
    else:
        text = (
            "💬 <b>退租需求正在跟进</b>\n\n之前已经提交过当前租约的退租需求，中文顾问会继续处理。"
            if duplicate else
            "✅ <b>退租需求已提交</b>\n\n中文顾问会确认通知期、交接、费用和押金核对安排。"
        )
    return ServiceView(
        "tenant_submit",
        text,
        ((ServiceChoice("💬 中文顾问", "v3u:home:contact"),), (ServiceChoice("⬅️ 返回我的租约", "v3u:service:tenant_lease"),)),
    ), effect


async def send_pdf(*, kind, service, user_id, repo_root, context, chat_id):
    service.require_active_binding(user_id)
    bundle = assurance_asset_bundle(Path(repo_root), kind)
    if not bundle.pdf_path.is_file():
        raise FileNotFoundError(str(bundle.pdf_path))
    with bundle.pdf_path.open("rb") as fh:
        await context.bot.send_document(chat_id=chat_id, document=InputFile(fh, filename=bundle.filename))


__all__ = [
    "tenant_home_view", "missing_lease_view", "lease_view", "guide_view", "handover_view", "deposit_view",
    "renew_view", "terminate_view", "submit_request", "send_pdf",
]
