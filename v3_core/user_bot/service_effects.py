"""Best-effort lead/admin effects for V3 tenant-service submissions."""
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from html import escape as he
import json, sqlite3
from typing import Any, Callable
from .admin_notification_plans import user_contact_text,user_mention_html
from .admin_notifications import AdminNotification,AdminNotificationResult,TelegramAdminNotifier
from .lead_effects import LeadEffectResult
from .lead_service import LeadRequest,LeadService,LeadUser
from .service_flow import RepairSubmission

@dataclass(frozen=True)
class ServiceEffectResult:
    lead:LeadEffectResult; admin:AdminNotificationResult

class ServiceEffectExecutor:
    def __init__(self,*,leads:LeadService,admins:TelegramAdminNotifier,now:Callable[[],str]|None=None):
        self.leads=leads; self.admins=admins; self.now=now or (lambda:datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    def _record(self,*,user,request):
        try: return LeadEffectResult(status='recorded',record=self.leads.record(user=user,request=request,created_at=self.now()))
        except Exception as exc: return LeadEffectResult(status='failed',error=str(exc))
    def _already_recorded(self,*,user_id:int,action:str,binding_id:int)->bool:
        repo=getattr(self.leads,'repository',None); path=getattr(repo,'db_path',None)
        if path is None: return False
        try:
            with sqlite3.connect(str(path)) as conn:
                rows=conn.execute("SELECT payload_json FROM leads_v3 WHERE user_id=? AND action=? AND lead_status IN ('new','pending') ORDER BY id DESC LIMIT 20",(int(user_id),action)).fetchall()
            for (raw,) in rows:
                try: payload=json.loads(raw or '{}')
                except Exception: payload={}
                if int(payload.get('binding_id') or 0)==int(binding_id): return True
        except Exception: return False
        return False
    async def repair(self,*,bot,user,submission):
        if not submission.created: return ServiceEffectResult(LeadEffectResult(status='skipped'),AdminNotificationResult((),(),()))
        t=submission.ticket
        lead=self._record(user=user,request=LeadRequest(action='service_request_submit',source='service_hub',listing_id=t.property_name,payload={'ticket_id':t.id,'request_token':t.request_token,'issue_key':t.issue_key,'issue_label':t.issue_type,'time_slot':t.time_slot,'detail':t.description,'binding_id':t.binding_id}))
        admin=await self.admins.send(bot,AdminNotification(title='新报修请求',lines=(f'客户：{user_mention_html(user)}',f'联系方式：{he(user_contact_text(user))}',f'租客绑定：#{t.binding_id or 0}',f'房源：{he(t.property_name or "-")}',f'问题：{he(t.issue_type)}',f'说明：{he(t.description.split(chr(10),1)[0])}',f'希望时间：{he(submission.slot_label)}')))
        return ServiceEffectResult(lead,admin)
    async def tenancy_request(self,*,bot,user,binding,kind:str):
        action='tenant_renewal_request' if kind=='renew' else 'tenant_termination_request'; title='续租申请' if kind=='renew' else '退租申请'
        if self._already_recorded(user_id=user.user_id,action=action,binding_id=binding.id): return ServiceEffectResult(LeadEffectResult(status='skipped'),AdminNotificationResult((),(),()))
        lead=self._record(user=user,request=LeadRequest(action=action,source='tenant_service',listing_id=binding.property_name,payload={'binding_id':binding.id,'property_name':binding.property_name,'lease_end_date':binding.lease_end_date or binding.contract_end_date,'request_status':'pending'}))
        admin=await self.admins.send(bot,AdminNotification(title=title,lines=(f'客户：{user_mention_html(user)}',f'联系方式：{he(user_contact_text(user))}',f'租客绑定：#{binding.id}',f'房源：{he(binding.property_name or "-")}',f'合同到期：{he(binding.lease_end_date or binding.contract_end_date or "待确认")}', '状态：申请已提交 / 待顾问跟进')))
        return ServiceEffectResult(lead,admin)
    async def general(self,*,bot,user,details:str,nearby:bool=False):
        kind='周边需求' if nearby else '通用服务咨询'; action='nearby_request' if nearby else 'service_general'; source='nearby_service' if nearby else 'service_hub'; clean=str(details or '').strip()[:800]
        lead=self._record(user=user,request=LeadRequest(action=action,source=source,payload={'kind':kind,'details':clean}))
        admin=await self.admins.send(bot,AdminNotification(title=kind,lines=(f'用户：{user_mention_html(user)}',f'联系方式：{he(user_contact_text(user))}',f'需求：<code>{he(clean[:700])}</code>')))
        return ServiceEffectResult(lead,admin)

__all__=['ServiceEffectExecutor','ServiceEffectResult']
