import sqlite3
from types import SimpleNamespace
import pytest

from v3_core.storage.bootstrap import initialize_v3_storage
from v3_core.storage.service_repository import SQLiteTenantServiceRepository
from v3_core.user_bot.admin_notifications import AdminNotificationResult
from v3_core.user_bot.lead_service import LeadRecordResult
from v3_core.user_bot.service_effects import ServiceEffectExecutor
from v3_core.user_bot.service_flow import TenantService
from v3_core.user_bot.telegram_service_handler import (
    SERVICE_GENERAL_WAIT_KEY, SERVICE_REQUEST_SESSION_KEY,
    handle_v3_service_callback, handle_v3_service_text,
)


class FakeMessage:
    def __init__(self,text="",chat_id=1001,message_id=5):
        self.text=text; self.photo=[]; self.chat_id=chat_id; self.message_id=message_id; self.calls=[]
    async def reply_text(self,*args,**kwargs):
        self.calls.append(("reply_text",args,kwargs))
        return SimpleNamespace(message_id=99,chat_id=self.chat_id)


class FakeQuery:
    def __init__(self,data,*,fail_edit=False):
        self.data=data; self.fail_edit=fail_edit; self.message=FakeMessage(); self.calls=[]
    async def answer(self,*args,**kwargs): self.calls.append(("answer",args,kwargs))
    async def edit_message_text(self,*args,**kwargs):
        self.calls.append(("edit_text",args,kwargs))
        if self.fail_edit: raise RuntimeError("telegram_edit_failed")
    async def edit_message_caption(self,*args,**kwargs):
        self.calls.append(("edit_caption",args,kwargs))
        if self.fail_edit: raise RuntimeError("telegram_edit_failed")


class FakeBot:
    def __init__(self): self.calls=[]
    async def send_message(self,**kwargs):
        self.calls.append(("send_message",kwargs))
        return SimpleNamespace(message_id=77,chat_id=kwargs["chat_id"])
    async def edit_message_text(self,**kwargs):
        self.calls.append(("edit_message_text",kwargs))


class CountingLeadService:
    def __init__(self): self.calls=[]
    def record(self,*,user,request,created_at):
        self.calls.append((user,request,created_at))
        return LeadRecordResult(lead_id=len(self.calls),action=request.action,source=request.source)


class CountingAdmins:
    def __init__(self): self.calls=[]
    async def send(self,bot,notification):
        self.calls.append((bot,notification))
        return AdminNotificationResult((10,),(10,),())


def _user(): return SimpleNamespace(id=123,username="alice",full_name="Alice")
def _callback_update(query): return SimpleNamespace(callback_query=query,effective_user=_user(),effective_chat=SimpleNamespace(id=1001))
def _text_update(message): return SimpleNamespace(effective_message=message,effective_user=_user(),effective_chat=SimpleNamespace(id=1001))
def _context(user_data=None): return SimpleNamespace(bot=FakeBot(),user_data={} if user_data is None else user_data)


def _service(tmp_path,*,binding=True):
    db=tmp_path/"v3.db"; initialize_v3_storage(db)
    if binding:
        with sqlite3.connect(str(db)) as conn:
            conn.execute("""INSERT INTO tenant_bindings_v3
                (user_id,binding_code,property_name,status,created_at)
                VALUES (123,'BIND-123','富力城 A3-1208','active','2026-09-09 03:00:00')""")
            conn.commit()
    return TenantService(SQLiteTenantServiceRepository(db),now=lambda:"2026-09-09 03:14:00")


@pytest.mark.asyncio
async def test_issue_state_mutates_only_after_render_success(tmp_path):
    service=_service(tmp_path); context=_context(); query=FakeQuery("v3u:service:issue:repair_ac",fail_edit=True)
    with pytest.raises(RuntimeError,match="telegram_edit_failed"):
        await handle_v3_service_callback(_callback_update(query),context,service=service)
    assert SERVICE_REQUEST_SESSION_KEY not in context.user_data


@pytest.mark.asyncio
async def test_repair_description_advances_to_optional_media_not_time(tmp_path):
    service=_service(tmp_path); context=_context()
    query=FakeQuery("v3u:service:issue:repair_ac")
    await handle_v3_service_callback(_callback_update(query),context,service=service)
    token=context.user_data[SERVICE_REQUEST_SESSION_KEY]["request_token"]
    msg=FakeMessage("空调可以启动，但一直不制冷。")
    outcome=await handle_v3_service_text(_text_update(msg),context,service=service)
    assert outcome.handled
    state=context.user_data[SERVICE_REQUEST_SESSION_KEY]
    assert state["request_token"]==token and state["stage"]=="media"
    rendered=msg.calls[-1][1][0]
    assert "补充图片或视频" in rendered
    callbacks=[b.callback_data for row in msg.calls[-1][2]["reply_markup"].inline_keyboard for b in row]
    assert callbacks==["v3u:service:repair_media_skip","v3u:service:repair_modify_desc"]


@pytest.mark.asyncio
async def test_unbound_repair_collects_full_need_then_uses_real_handoff(tmp_path):
    service=_service(tmp_path,binding=False); context=_context()
    leads=CountingLeadService(); admins=CountingAdmins()
    effects=ServiceEffectExecutor(leads=leads,admins=admins,now=lambda:"2026-09-09 03:14:01")

    await handle_v3_service_callback(_callback_update(FakeQuery("v3u:service:issue:repair_ac")),context,service=service,effects=effects)
    await handle_v3_service_text(_text_update(FakeMessage("空调可以启动，但一直不制冷。")),context,service=service,effects=effects)
    await handle_v3_service_callback(_callback_update(FakeQuery("v3u:service:repair_media_skip")),context,service=service,effects=effects)
    slot=FakeQuery("v3u:service:slot:today")
    await handle_v3_service_callback(_callback_update(slot),context,service=service,effects=effects)
    assert context.user_data[SERVICE_REQUEST_SESSION_KEY]["stage"]=="confirm"

    confirm=FakeQuery("v3u:service:repair_confirm")
    outcome=await handle_v3_service_callback(_callback_update(confirm),context,service=service,effects=effects)
    assert outcome.handled and outcome.ticket_id is None
    assert len(leads.calls)==1 and len(admins.calls)==1
    assert "报修已记录" in confirm.calls[-1][1][0]
    assert SERVICE_REQUEST_SESSION_KEY not in context.user_data
    with sqlite3.connect(str(service.repository.db_path)) as conn:
        assert conn.execute("SELECT COUNT(*) FROM repair_tickets_v3").fetchone()[0]==0


@pytest.mark.asyncio
async def test_bound_repair_creates_real_ticket_only_on_confirm(tmp_path):
    service=_service(tmp_path,binding=True); context=_context()
    await handle_v3_service_callback(_callback_update(FakeQuery("v3u:service:issue:repair_door")),context,service=service)
    await handle_v3_service_text(_text_update(FakeMessage("门禁刷卡没有反应。")),context,service=service)
    await handle_v3_service_callback(_callback_update(FakeQuery("v3u:service:repair_media_skip")),context,service=service)
    await handle_v3_service_callback(_callback_update(FakeQuery("v3u:service:slot:today")),context,service=service)
    with sqlite3.connect(str(service.repository.db_path)) as conn:
        assert conn.execute("SELECT COUNT(*) FROM repair_tickets_v3").fetchone()[0]==0
    confirm=FakeQuery("v3u:service:repair_confirm")
    outcome=await handle_v3_service_callback(_callback_update(confirm),context,service=service)
    assert outcome.ticket_id is not None
    assert "报修已记录" in confirm.calls[-1][1][0]
    with sqlite3.connect(str(service.repository.db_path)) as conn:
        assert conn.execute("SELECT COUNT(*) FROM repair_tickets_v3").fetchone()[0]==1


@pytest.mark.asyncio
async def test_general_text_consumed_only_with_explicit_wait_state(tmp_path):
    service=_service(tmp_path); msg=FakeMessage("需要帮忙联系物业"); context=_context()
    untouched=await handle_v3_service_text(_text_update(msg),context,service=service)
    assert not untouched.handled
    query=FakeQuery("v3u:service:general")
    await handle_v3_service_callback(_callback_update(query),context,service=service)
    assert context.user_data[SERVICE_GENERAL_WAIT_KEY] is True