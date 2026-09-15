"""Bound-tenant V1 product surface for the production V3 User Bot."""
from __future__ import annotations
from html import escape as he
from pathlib import Path
import sqlite3
from telegram import InputFile
from .assurance_views import assurance_asset_bundle
from .lead_service import LeadUser
from .service_flow import TenantService
from .service_views import ServiceChoice, ServiceView

def tenant_home_view(service:TenantService,user_id:int)->ServiceView:
    b=service.active_binding(user_id)
    if b is None:
        return ServiceView('tenant_missing','🛠 <b>入住服务</b>\n\n当前账号还没有绑定有效租约。\n\n如果已经通过侨联入住，请联系中文顾问核对并绑定入住档案。',((ServiceChoice('💬 中文顾问','v3u:home:contact'),),(ServiceChoice('🏠 返回首页','v3u:t:home'),)))
    return ServiceView('tenant_home',f'🛠 <b>入住服务</b>\n\n🏠 当前租约：<b>{he(b.property_name or "已绑定住房")}</b>\n\n这里可以查看租约、处理报修和物业事项，也可以查看租赁服务指南。\n\n续租或退租时，侨联会按当前租约继续衔接处理。',((ServiceChoice('🔧 报修','v3u:service:repair'),ServiceChoice('🏢 物业沟通','v3u:service:property')),(ServiceChoice('📋 我的租约','v3u:service:tenant_lease'),ServiceChoice('📄 租赁服务指南','v3u:service:tenant_guide')),(ServiceChoice('🔄 续租','v3u:service:tenant_renew'),ServiceChoice('🚪 退租','v3u:service:tenant_terminate')),(ServiceChoice('💬 中文顾问','v3u:home:contact'),),(ServiceChoice('🏠 返回首页','v3u:t:home'),)))

def lease_view(service,user_id):
    b=service.require_active_binding(user_id); end=b.lease_end_date or b.contract_end_date or '待确认'; rent=f'${b.monthly_rent:g}/月' if b.monthly_rent else '待确认'; day=f'每月 {b.rent_day} 号' if b.rent_day else '待确认'
    return ServiceView('tenant_lease',f'📋 <b>我的租约</b>\n\n🏠 房源｜{he(b.property_name or "-")}\n💰 月租｜{rent}\n🔐 押金｜{b.deposit_months}个月\n📅 交租日｜{day}\n⏳ 合同到期｜{he(end)}\n\n当前状态｜有效租约',((ServiceChoice('🔄 续租','v3u:service:tenant_renew'),ServiceChoice('🚪 退租','v3u:service:tenant_terminate')),(ServiceChoice('📄 租赁服务指南','v3u:service:tenant_guide'),),(ServiceChoice('🛠 返回入住服务','v3u:service:tenant'),ServiceChoice('💬 中文顾问','v3u:home:contact'))))

def guide_view(service,user_id):
    service.require_active_binding(user_id)
    return ServiceView('tenant_guide','📄 <b>租赁服务指南</b>\n\n租房过程中，几个重要节点建议提前留好记录。\n\n<b>1. 签约前</b>\n确认租金、押金、水电、物业、网络及其他费用。\n\n<b>2. 入住时</b>\n记录房屋现状、家具家电、水电表和钥匙/门卡。\n\n<b>3. 退租时</b>\n按合同和入住留档逐项核对房屋、费用及押金。',((ServiceChoice('📋 入住交接清单','v3u:service:tenant_handover'),ServiceChoice('🔐 退租与押金说明','v3u:service:tenant_deposit')),(ServiceChoice('🔄 续租','v3u:service:tenant_renew'),ServiceChoice('🚪 退租','v3u:service:tenant_terminate')),(ServiceChoice('⬅️ 返回已入住用户','v3u:service:tenant'),)))

def handover_view(service,user_id):
    service.require_active_binding(user_id)
    return ServiceView('tenant_handover','📋 <b>入住交接清单</b>\n\n用于入住当天逐项检查并留档。\n\n建议验房时边检查、边拍照记录，包括：\n• 房屋整体和已有瑕疵\n• 家具家电状态\n• 水表、电表读数\n• 钥匙、门卡数量\n• 双方确认的费用和其他事项\n\n需要保存到手机时，再点击下方下载。',((ServiceChoice('📥 下载入住交接清单','v3u:service:tenant_handover_pdf'),),(ServiceChoice('🔐 查看退租与押金说明','v3u:service:tenant_deposit'),),(ServiceChoice('⬅️ 返回租赁服务指南','v3u:service:tenant_guide'),)))

def deposit_view(service,user_id):
    service.require_active_binding(user_id)
    return ServiceView('tenant_deposit','🔐 <b>退租与押金说明</b>\n\n退租前建议先核对合同中的通知期、押金退还条件和扣费约定。\n\n退租交接时，结合入住留档逐项确认：\n• 房屋及家具家电状态\n• 水电、物业及其他费用\n• 钥匙 / 门卡交还\n• 押金扣费项目及依据\n\n最终押金退还金额以合同约定和实际交接核对结果为准。需要保存完整说明时，再点击下方下载。',((ServiceChoice('📥 下载退租与押金说明','v3u:service:tenant_deposit_pdf'),),(ServiceChoice('📋 查看入住交接清单','v3u:service:tenant_handover'),),(ServiceChoice('⬅️ 返回租赁服务指南','v3u:service:tenant_guide'),)))

def renew_view(service,user_id):
    b=service.require_active_binding(user_id); end=b.lease_end_date or b.contract_end_date or '待确认'
    return ServiceView('tenant_renew',f'🔄 <b>续租</b>\n\n🏠 当前房源：{he(b.property_name or "-")}\n📅 到期日：{he(end)}\n\n想继续住这套吗？\n\n确认后，中文顾问会核对新的租期和价格。',((ServiceChoice('✅ 我想续租','v3u:service:tenant_renew_submit'),),(ServiceChoice('💬 中文顾问','v3u:home:contact'),),(ServiceChoice('⬅️ 返回我的租约','v3u:service:tenant_lease'),)))

def terminate_view(service,user_id):
    b=service.require_active_binding(user_id); end=b.lease_end_date or b.contract_end_date or '待确认'
    return ServiceView('tenant_terminate',f'🚪 <b>退租</b>\n\n🏠 当前房源：{he(b.property_name or "-")}\n📅 合同到期：{he(end)}\n\n准备退租前，建议先确认合同中的通知期、押金退还条件和费用结算方式。\n\n提交退租后，中文顾问会确认预计退租时间、房屋交接和押金核对安排。',((ServiceChoice('✅ 我要退租','v3u:service:tenant_terminate_submit'),),(ServiceChoice('🔐 退租与押金说明','v3u:service:tenant_deposit'),),(ServiceChoice('💬 中文顾问','v3u:home:contact'),),(ServiceChoice('⬅️ 返回我的租约','v3u:service:tenant_lease'),)))

def _renewal_tracking(service,user,binding)->bool:
    try:
        with sqlite3.connect(str(service.repository.db_path)) as conn:
            exists=conn.execute("SELECT id FROM renewal_tracking WHERE binding_id=? AND user_id=? AND renewal_status IN ('pending','contacted') AND user_response='renew' ORDER BY id DESC LIMIT 1",(binding.id,user.user_id)).fetchone()
            if exists:return False
            conn.execute("INSERT INTO renewal_tracking(binding_id,user_id,listing_id,renewal_status,user_response,advisor_notes,contacted_at,completed_at,created_at) VALUES(?,?,?,'pending','renew','','','',CURRENT_TIMESTAMP)",(binding.id,user.user_id,binding.property_name or ''));conn.commit();return True
    except sqlite3.OperationalError:return True

async def submit_request(*,kind,service,effects,update,context):
    obj=update.effective_user; user=LeadUser(int(obj.id),str(getattr(obj,'username','') or ''),str(getattr(obj,'full_name','') or '')); b=service.require_active_binding(user.user_id)
    if kind=='renew':_renewal_tracking(service,user,b)
    effect=await effects.tenancy_request(bot=getattr(context,'bot',None),user=user,binding=b,kind=kind) if effects else None
    duplicate=bool(effect and effect.lead.status=='skipped')
    text=('💬 <b>续租正在跟进</b>\n\n中文顾问会继续确认新的租期和价格。' if duplicate else '✅ <b>续租申请已收到</b>\n\n中文顾问会确认新的租期和价格，再反馈结果。') if kind=='renew' else ('💬 <b>退租正在跟进</b>\n\n中文顾问会继续确认退租交接和押金核对安排。' if duplicate else '✅ <b>退租计划已收到</b>\n\n中文顾问会确认预计退租日期、合同通知期、房屋交接、水电费用及押金核对事项。')
    return ServiceView('tenant_submit',text,((ServiceChoice('💬 中文顾问','v3u:home:contact'),),(ServiceChoice('📋 返回我的租约','v3u:service:tenant_lease'),))),effect

async def send_pdf(*,kind,service,user_id,repo_root,context,chat_id):
    service.require_active_binding(user_id);bundle=assurance_asset_bundle(Path(repo_root),kind)
    if not bundle.pdf_path.is_file():raise FileNotFoundError(str(bundle.pdf_path))
    with bundle.pdf_path.open('rb') as fh:await context.bot.send_document(chat_id=chat_id,document=InputFile(fh,filename=bundle.filename))

__all__=['tenant_home_view','lease_view','guide_view','handover_view','deposit_view','renew_view','terminate_view','submit_request','send_pdf']