
from types import SimpleNamespace
import pytest

from v3_core.user_bot.admin_notifications import AdminNotificationResult
from v3_core.user_bot.appointment_history import AppointmentHistoryView
from v3_core.user_bot.contact_effects import ContactEffectResult
from v3_core.user_bot.lead_effects import LeadEffectResult
from v3_core.user_bot.telegram_home_handler import handle_v3_home_callback
from v3_core.user_bot.transition_session import AWAITING_KEYWORD_SESSION_KEY, SEARCH_PREF_SESSION_KEY
from v3_core.user_bot.transition_views import TransitionViewService


class EmptyInventory:
    def resolve(self, public_listing_id): return None


class FakeMessage:
    def __init__(self, *, fail_reply=False):
        self.photo=[]
        self.fail_reply=fail_reply
        self.calls=[]
    async def reply_text(self, text, **kwargs):
        self.calls.append(("reply_text", text, kwargs))
        if self.fail_reply:
            raise RuntimeError("telegram_send_failed")
        return SimpleNamespace(message_id=99, chat_id=1001)


class FakeQuery:
    def __init__(self, data, *, message=None, fail_edit=False):
        self.data=data; self.message=message or FakeMessage(); self.fail_edit=fail_edit; self.calls=[]
    async def answer(self,*args,**kwargs): self.calls.append(("answer",args,kwargs))
    async def edit_message_text(self,*args,**kwargs):
        self.calls.append(("edit_text",args,kwargs))
        if self.fail_edit: raise RuntimeError("telegram_edit_failed")
    async def edit_message_caption(self,*args,**kwargs):
        self.calls.append(("edit_caption",args,kwargs))
        if self.fail_edit: raise RuntimeError("telegram_edit_failed")


class FakeHistory:
    def __init__(self): self.calls=[]
    def build(self,user_id):
        self.calls.append(user_id)
        return AppointmentHistoryView(text="<b>我的预约</b>\n\nQL-RF-A2B3",items=(),history_count=0)


class FakeContactEffects:
    def __init__(self): self.calls=[]
    async def execute_general(self,*,bot,user,source="hub"):
        self.calls.append((bot,user,source))
        return ContactEffectResult(
            lead=LeadEffectResult(status="recorded"),
            admin=AdminNotificationResult((10,),(10,),()),
        )


def _update(query):
    return SimpleNamespace(
        callback_query=query,
        effective_user=SimpleNamespace(id=123,username="alice",full_name="Alice",first_name="Alice",last_name=""),
    )

def _context(): return SimpleNamespace(bot=object(),user_data={})
def _search_views(): return TransitionViewService(EmptyInventory())


@pytest.mark.asyncio
async def test_search_home_edits_existing_panel_then_sets_session():
    message=FakeMessage()
    query=FakeQuery("v3u:home:search",message=message)
    context=_context()
    outcome=await handle_v3_home_callback(_update(query),context,appointment_history=FakeHistory(),search_views=_search_views())
    assert outcome.handled and outcome.rendered
    assert [c[0] for c in query.calls]==["answer","edit_text"]
    assert message.calls==[]
    assert "想找什么样的房子？" in query.calls[-1][1][0]
    assert "BKK1 一房，预算 $600" in query.calls[-1][1][0]
    callbacks=[b.callback_data for row in query.calls[-1][2]["reply_markup"].inline_keyboard for b in row]
    assert callbacks==[
        "v3u:t:search_area","v3u:t:search_budget","v3u:t:search_layout",
        "v3u:home:contact","v3u:t:home",
    ]
    assert context.user_data[AWAITING_KEYWORD_SESSION_KEY]=={"source":"user_search"}
    assert context.user_data[SEARCH_PREF_SESSION_KEY]["source"]=="user_search"


@pytest.mark.asyncio
async def test_search_edit_failure_does_not_create_session():
    message=FakeMessage()
    query=FakeQuery("v3u:home:search",message=message,fail_edit=True)
    context=_context()
    with pytest.raises(RuntimeError,match="telegram_edit_failed"):
        await handle_v3_home_callback(_update(query),context,appointment_history=FakeHistory(),search_views=_search_views())
    assert context.user_data=={}


@pytest.mark.asyncio
async def test_appointments_edit_existing_surface():
    message=FakeMessage()
    query=FakeQuery("v3u:home:appointments",message=message)
    history=FakeHistory()
    outcome=await handle_v3_home_callback(_update(query),_context(),appointment_history=history)
    assert outcome.handled and outcome.rendered
    assert history.calls==[123]
    assert [c[0] for c in query.calls]==["answer","edit_text"]
    assert message.calls==[]
    assert "QL-RF-A2B3" in query.calls[-1][1][0]
    labels=[b.text for row in query.calls[-1][2]["reply_markup"].inline_keyboard for b in row]
    assert labels==["🔍 开始找房","💬 中文顾问","⬅️ 返回首页"]


@pytest.mark.asyncio
async def test_service_home_edits_existing_surface():
    message=FakeMessage()
    query=FakeQuery("v3u:home:service",message=message)
    outcome=await handle_v3_home_callback(_update(query),_context(),appointment_history=FakeHistory())
    assert outcome.handled and outcome.rendered
    assert message.calls==[]
    assert [c[0] for c in query.calls]==["answer","edit_text"]
    assert "侨联服务" in query.calls[-1][1][0]
    labels=[b.text for row in query.calls[-1][2]["reply_markup"].inline_keyboard for b in row]
    assert labels==["📋 我的租约","🛡️ 入住服务","🏠 安心租房","💬 中文顾问","⬅️ 返回首页"]


@pytest.mark.asyncio
async def test_contact_still_runs_effect_before_handoff_render():
    query=FakeQuery("v3u:home:contact")
    effects=FakeContactEffects()
    outcome=await handle_v3_home_callback(
        _update(query),_context(),appointment_history=FakeHistory(),
        contact_effects=effects,advisor_url="https://t.me/advisor",
    )
    assert outcome.handled and outcome.rendered
    assert len(effects.calls)==1
    assert [c[0] for c in query.calls]==["answer","edit_text"]
    assert "中文顾问" in query.calls[-1][1][0]
