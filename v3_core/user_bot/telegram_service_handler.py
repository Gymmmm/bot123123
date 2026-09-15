"""Telegram callback/text adapters for V3 tenant service."""
from __future__ import annotations
from dataclasses import dataclass
from typing import Any
from uuid import uuid4
from telegram import InlineKeyboardButton,InlineKeyboardMarkup
from telegram.constants import ParseMode
from .lead_service import LeadUser
from .service_effects import ServiceEffectExecutor,ServiceEffectResult
from .service_flow import ServiceRequestDraft,TenantService
from .service_views import ServiceChoice,ServiceView,general_prompt_view,general_success_view,issue_prompt_view,local_life_view,nearby_view,property_view,repair_home_view,repair_success_view,rfcity_category_view,rfcity_home_view,slot_view
from .telegram_navigation import advisor_handoff_url
from .tenant_v1 import tenant_home_view,lease_view,guide_view,handover_view,deposit_view,renew_view,terminate_view,submit_request

SERVICE_REQUEST_SESSION_KEY='v3_service_request'; SERVICE_GENERAL_WAIT_KEY='v3_service_general_wait'; SERVICE_NEARBY_WAIT_KEY='v3_service_nearby_wait'
@dataclass(frozen=True)
class TelegramServiceOutcome:
    handled:bool; action:str=''; rendered:bool=False; effect:ServiceEffectResult|None=None; ticket_id:int|None=None

def _service_home_with_tenant_entry(): return tenant_home_view

def _tenant_binding_view(service,user_id): return tenant_home_view(service,user_id)

def build_service_keyboard(view,*,advisor_url=''):
    if not view.rows:return None
    rows=[]; advisor=str(advisor_url or '').strip()
    for row in view.rows:
        buttons=[]
        for c in row:
            label='💬 中文顾问' if c.label in {'💬 联系我们','💬 联系中文顾问','💬 联系顾问'} else c.label
            if c.callback_data=='v3u:home:contact' and advisor: buttons.append(InlineKeyboardButton(label,url=advisor_handoff_url(advisor)))
            else: buttons.append(InlineKeyboardButton(label,callback_data=c.callback_data))
        rows.append(buttons)
    return InlineKeyboardMarkup(rows)
async def render_service_view(query,view,*,advisor_url=''):
    markup=build_service_keyboard(view,advisor_url=advisor_url); msg=getattr(query,'message',None)
    if getattr(msg,'photo',None): await query.edit_message_caption(caption=view.text,parse_mode=ParseMode.HTML,reply_markup=markup)
    else: await query.edit_message_text(view.text,parse_mode=ParseMode.HTML,reply_markup=markup)
async def _reply(message,view,*,advisor_url=''): await message.reply_text(view.text,parse_mode=ParseMode.HTML,reply_markup=build_service_keyboard(view,advisor_url=advisor_url))
def _lead_user(update):
    u=update.effective_user
    if u is None or getattr(u,'id',None) is None: raise ValueError('telegram_effective_user_missing_for_service')
    return LeadUser(int(u.id),str(getattr(u,'username','') or ''),str(getattr(u,'full_name','') or ''))
def _draft_from_session(v):
    if not isinstance(v,dict):return None
    if not all(str(v.get(k) or '').strip() for k in ('issue_key','issue_label','request_token')):return None
    return ServiceRequestDraft(str(v['issue_key']),str(v['issue_label']),str(v.get('detail') or ''),str(v['request_token']))
def _store_draft(d,draft): d[SERVICE_REQUEST_SESSION_KEY]={'issue_key':draft.issue_key,'issue_label':draft.issue_label,'detail':draft.detail,'request_token':draft.request_token}
def _rfcity_category_product_view(category):
    v=rfcity_category_view(category); return ServiceView(v.kind,v.text,((ServiceChoice('⬅️ 返回富力导航','v3u:service:rfcity'),),))
def _missing_view(service,uid): return tenant_home_view(service,uid)

def _requires_binding(action): return action in {'repair','property','tenant_lease','tenant_guide','tenant_handover','tenant_deposit','tenant_renew','tenant_terminate','tenant_renew_submit','tenant_terminate_submit'} or action.startswith(('issue:','slot:','tenant_handover_pdf','tenant_deposit_pdf'))

async def handle_v3_service_callback(update,context,*,service:TenantService,effects:ServiceEffectExecutor|None=None,advisor_url:str=''):
    q=getattr(update,'callback_query',None); raw=str(getattr(q,'data','') or '') if q else ''
    if q is None or not raw.startswith('v3u:service:'):return TelegramServiceOutcome(False)
    action=raw[len('v3u:service:'):].strip(); data=getattr(context,'user_data',None)
    if not isinstance(data,dict):raise ValueError('telegram_user_data_missing_for_service')
    user=_lead_user(update); binding=service.active_binding(user.user_id)
    await q.answer()
    if _requires_binding(action) and binding is None:
        await render_service_view(q,_missing_view(service,user.user_id),advisor_url=advisor_url); return TelegramServiceOutcome(True,action,True)
    tenant_views={'tenant':tenant_home_view,'tenant_lease':lease_view,'tenant_guide':guide_view,'tenant_handover':handover_view,'tenant_deposit':deposit_view,'tenant_renew':renew_view,'tenant_terminate':terminate_view}
    if action in tenant_views:
        await render_service_view(q,tenant_views[action](service,user.user_id),advisor_url=advisor_url); return TelegramServiceOutcome(True,action,True)
    if action in {'tenant_renew_submit','tenant_terminate_submit'}:
        view,effect=await submit_request(kind='renew' if action=='tenant_renew_submit' else 'terminate',service=service,effects=effects,update=update,context=context); await render_service_view(q,view,advisor_url=advisor_url); return TelegramServiceOutcome(True,action,True,effect=effect)
    if action=='repair': await render_service_view(q,repair_home_view(),advisor_url=advisor_url); return TelegramServiceOutcome(True,action,True)
    if action=='property': await render_service_view(q,property_view(),advisor_url=advisor_url); return TelegramServiceOutcome(True,action,True)
    if action=='local': await render_service_view(q,local_life_view(),advisor_url=advisor_url); return TelegramServiceOutcome(True,action,True)
    if action=='general': await render_service_view(q,general_prompt_view(),advisor_url=advisor_url); data[SERVICE_GENERAL_WAIT_KEY]=True; data.pop(SERVICE_NEARBY_WAIT_KEY,None); return TelegramServiceOutcome(True,action,True)
    if action=='nearby': await render_service_view(q,nearby_view(),advisor_url=advisor_url); return TelegramServiceOutcome(True,action,True)
    if action=='nearby_other': await render_service_view(q,general_prompt_view(nearby=True),advisor_url=advisor_url); data[SERVICE_NEARBY_WAIT_KEY]=True; data.pop(SERVICE_GENERAL_WAIT_KEY,None); return TelegramServiceOutcome(True,action,True)
    if action=='rfcity': await render_service_view(q,rfcity_home_view(),advisor_url=advisor_url); return TelegramServiceOutcome(True,action,True)
    if action.startswith('rfcity:'): await render_service_view(q,_rfcity_category_product_view(action.split(':',1)[1]),advisor_url=advisor_url); return TelegramServiceOutcome(True,action,True)
    if action.startswith('issue:'):
        draft=service.begin_request(action.split(':',1)[1],request_token=uuid4().hex); await render_service_view(q,issue_prompt_view(draft),advisor_url=advisor_url); _store_draft(data,draft); data.pop(SERVICE_GENERAL_WAIT_KEY,None); data.pop(SERVICE_NEARBY_WAIT_KEY,None); return TelegramServiceOutcome(True,action,True)
    if action.startswith('slot:'):
        draft=_draft_from_session(data.get(SERVICE_REQUEST_SESSION_KEY))
        if draft is None or not draft.detail:return TelegramServiceOutcome(True,action,False)
        submission=service.submit_repair(user_id=user.user_id,draft=draft,slot=action.split(':',1)[1]); effect=await effects.repair(bot=getattr(context,'bot',None),user=user,submission=submission) if effects else None; await render_service_view(q,repair_success_view(urgent=submission.urgent),advisor_url=advisor_url); data.pop(SERVICE_REQUEST_SESSION_KEY,None); return TelegramServiceOutcome(True,action,True,effect,submission.ticket.id)
    return TelegramServiceOutcome(False)

async def handle_v3_service_text(update,context,*,service:TenantService,effects:ServiceEffectExecutor|None=None,advisor_url:str=''):
    data=getattr(context,'user_data',None); msg=getattr(update,'effective_message',None)
    if not isinstance(data,dict) or msg is None:return TelegramServiceOutcome(False)
    text=str(getattr(msg,'text','') or '').strip(); draft=_draft_from_session(data.get(SERVICE_REQUEST_SESSION_KEY))
    if draft is not None and not draft.detail:
        user=_lead_user(update)
        if service.active_binding(user.user_id) is None: data.pop(SERVICE_REQUEST_SESSION_KEY,None); await msg.reply_text('当前账号没有有效租约绑定，请先联系中文顾问核对。'); return TelegramServiceOutcome(True,'repair_denied',True)
        if len(text)<4: await msg.reply_text('请简单描述发生了什么，例如：空调可以启动，但一直不制冷。'); return TelegramServiceOutcome(True,'repair_detail',True)
        updated=service.with_detail(draft,text); await _reply(msg,slot_view(updated),advisor_url=advisor_url); _store_draft(data,updated); return TelegramServiceOutcome(True,'repair_detail',True)
    nearby=bool(data.get(SERVICE_NEARBY_WAIT_KEY)); general=bool(data.get(SERVICE_GENERAL_WAIT_KEY))
    if not nearby and not general:return TelegramServiceOutcome(False)
    if len(text)<2: await msg.reply_text('请简单说一下需要什么帮助。'); return TelegramServiceOutcome(True,'nearby_text' if nearby else 'general_text',True)
    await _reply(msg,general_success_view(nearby=nearby),advisor_url=advisor_url); effect=await effects.general(bot=getattr(context,'bot',None),user=_lead_user(update),details=text,nearby=nearby) if effects else None; data.pop(SERVICE_NEARBY_WAIT_KEY,None); data.pop(SERVICE_GENERAL_WAIT_KEY,None); return TelegramServiceOutcome(True,'nearby_text' if nearby else 'general_text',True,effect)

__all__=['SERVICE_GENERAL_WAIT_KEY','SERVICE_NEARBY_WAIT_KEY','SERVICE_REQUEST_SESSION_KEY','TelegramServiceOutcome','build_service_keyboard','handle_v3_service_callback','handle_v3_service_text','render_service_view','_service_home_with_tenant_entry','_tenant_binding_view']
