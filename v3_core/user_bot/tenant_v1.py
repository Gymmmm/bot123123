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
        (ServiceChoice("中文顾问", "v3u:home:contact"),),
        (ServiceChoice("侨联服务", "v3u:home:service"), ServiceChoice("开始找房", "v3u:home:search")),
        (ServiceChoice("返回首页", "v3u:t:home"),),
    )


def tenant_home_view(service: TenantService, user_id: int) -> ServiceView:
    _ = service, user_id
    return service_home_view()


def missing_lease_view() -> ServiceView:
    return ServiceView(
        "tenant_lease_missing",
        "📋 <b>我的租约</b>\n"
        "目前没有查到已绑定的租约。如果你已经通过侨联入住，但这里暂时没有显示，可以联系中文顾问帮你核对。",
        (
            (ServiceChoice("💬 中文顾问", "v3u:home:contact"),),
            (ServiceChoice("⬅️ 返回侨联服务", "v3u:home:service"),),
        ),
    )

def lease_view(service: TenantService, user_id: int) -> ServiceView:
    binding = service.require_active_binding(user_id)
    end = _safe_text(binding.lease_end_date or binding.contract_end_date)
    rent = ("$" + f"{binding.monthly_rent:g}/月") if float(binding.monthly_rent or 0) > 0 else "待补全"
    deposit = f"{binding.deposit_months}个月" if isinstance(binding.deposit_months, int) and binding.deposit_months > 0 else "待补全"
    day = f"每月{binding.rent_day}日" if isinstance(binding.rent_day, int) and 1 <= binding.rent_day <= 31 else "待补全"
    property_name = he(_safe_text(binding.property_name))
    return ServiceView(
        "tenant_lease",
        f"📋 <b>租赁详情</b>\n"
        f"🏠 {property_name}\n"
        f"月租｜{rent}\n"
        f"押金｜{deposit}\n"
        f"交租日｜{day}\n"
        f"到期日｜{he(end)}\n"
        "状态｜🟢 租约有效",
        (
            (ServiceChoice("🔄 申请续租", "v3u:service:tenant_renew"), ServiceChoice("💬 中文顾问", "v3u:home:contact")),
            (ServiceChoice("⬅️ 返回侨联服务", "v3u:home:service"),),
        ),
    )

def renew_view(service: TenantService, user_id: int) -> ServiceView:
    binding = service.require_active_binding(user_id)
    end = he(_safe_text(binding.lease_end_date or binding.contract_end_date))
    prop = he(_safe_text(binding.property_name))
    return ServiceView(
        "tenant_renew",
        f"<b>续租</b>\n\n<b>{prop}</b>\n当前租约至 {end}\n\n"
        "如果您准备继续租住，可以先提交续租意向。\n"
        "后续租期和租金条件，由中文顾问与您进一步确认。",
        (
            (ServiceChoice("提交续租意向", "v3u:service:tenant_renew_submit"),),
            (ServiceChoice("中文顾问", "v3u:home:contact"),),
        ),
    )


def terminate_view(service: TenantService, user_id: int) -> ServiceView:
    binding = service.require_active_binding(user_id)
    end = he(_safe_text(binding.lease_end_date or binding.contract_end_date))
    prop = he(_safe_text(binding.property_name))
    return ServiceView(
        "tenant_terminate",
        f"<b>退租</b>\n\n<b>{prop}</b>\n当前租约至 {end}\n\n"
        "如果您准备退租，可以先提交退租意向。\n"
        "后续时间及需要处理的事项，由中文顾问与您进一步确认。",
        (
            (ServiceChoice("提交退租意向", "v3u:service:tenant_terminate_submit"),),
            (ServiceChoice("中文顾问", "v3u:home:contact"),),
        ),
    )


def guide_view(service: TenantService, user_id: int) -> ServiceView:
    _ = service, user_id
    return ServiceView(
        "tenant_guide_updated",
        "<b>侨联服务</b>\n\n请从侨联服务查看入住之后可以继续使用的服务。",
        ((ServiceChoice("侨联服务", "v3u:home:service"),), (ServiceChoice("返回首页", "v3u:t:home"),)),
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
            "<b>续租意向已提交</b>\n\n之前已经提交过当前租约的续租意向，中文顾问会继续跟进。"
            if duplicate else
            "<b>续租意向已提交</b>\n\n续租意向已记录。\n后续租期及租金条件，以顾问与您确认的结果为准。"
        )
    else:
        text = (
            "<b>退租意向已提交</b>\n\n之前已经提交过当前租约的退租意向，中文顾问会继续跟进。"
            if duplicate else
            "<b>退租意向已提交</b>\n\n退租意向已记录。\n"
            "中文顾问会根据当前租约情况，与您确认退租时间及后续需要处理的事项。"
        )
    return ServiceView(
        "tenant_submit",
        text,
        ((ServiceChoice("中文顾问", "v3u:home:contact"),),),
    ), effect


async def send_pdf(*, kind, service, user_id, repo_root, context, chat_id):
    """Compatibility boundary only.

    Public assurance templates must never be presented as lease-bound historical
    documents. The legacy helper remains callable only for the public assurance
    assets explicitly requested by its callers.
    """
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
