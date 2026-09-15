"""Telegram adapter for bound-tenant V1 routes; runs before legacy V3 service adapter."""
from __future__ import annotations
from dataclasses import dataclass
from typing import Any
from .service_effects import ServiceEffectExecutor
from .service_flow import TenantService
from .telegram_service_handler import render_service_view
from .tenant_v1 import tenant_home_view,lease_view,guide_view,handover_view,deposit_view,renew_view,terminate_view,submit_request,send_pdf

@dataclass(frozen=True)
class TenantV1Outcome:
    handled: bool
    action: str=''

async def handle_tenant_v1_callback(update:Any,context:Any,*,service:TenantService,effects:ServiceEffectExecutor|None,repo_root,advisor_url:str='')->TenantV1Outcome:
    q=getattr(update,'callback_query',None); raw=str(getattr(q,'data','') or '') if q else ''
    if q is None: return TenantV1Outcome(False)
    user=getattr(update,'effective_user',None)
    if user is None or getattr(user,'id',None) is None: return TenantV1Outcome(False)
    uid=int(user.id)
    if raw=='v3u:home:service':
        await q.answer(); await render_service_view(q,tenant_home_view(service,uid),advisor_url=advisor_url); return TenantV1Outcome(True,'home')
    if not raw.startswith('v3u:service:'): return TenantV1Outcome(False)
    action=raw[len('v3u:service:'):]
    sensitive=action in {'repair','property'} or action.startswith(('issue:','slot:'))
    if sensitive and service.active_binding(uid) is None:
        await q.answer('当前账号没有有效租约绑定',show_alert=True); await render_service_view(q,tenant_home_view(service,uid),advisor_url=advisor_url); return TenantV1Outcome(True,action)
    views={'tenant':tenant_home_view,'tenant_lease':lease_view,'tenant_guide':guide_view,'tenant_handover':handover_view,'tenant_deposit':deposit_view,'tenant_renew':renew_view,'tenant_terminate':terminate_view}
    if action in views:
        await q.answer(); await render_service_view(q,views[action](service,uid),advisor_url=advisor_url); return TenantV1Outcome(True,action)
    if action in {'tenant_handover_pdf','tenant_deposit_pdf'}:
        await q.answer(); kind='handover' if action.endswith('handover_pdf') else 'deposit'; await send_pdf(kind=kind,service=service,user_id=uid,repo_root=repo_root,context=context,chat_id=int(update.effective_chat.id)); return TenantV1Outcome(True,action)
    if action in {'tenant_renew_submit','tenant_terminate_submit'}:
        await q.answer(); kind='renew' if action=='tenant_renew_submit' else 'terminate'; view,_=await submit_request(kind=kind,service=service,effects=effects,update=update,context=context); await render_service_view(q,view,advisor_url=advisor_url); return TenantV1Outcome(True,action)
    return TenantV1Outcome(False,action)

__all__=['TenantV1Outcome','handle_tenant_v1_callback']
