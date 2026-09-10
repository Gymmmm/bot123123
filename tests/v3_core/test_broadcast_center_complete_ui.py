from types import SimpleNamespace
from v3_core.publishing.broadcast_admin import BroadcastAdminController

class Service:
    def ensure_defaults(self): pass
    def config(self): return SimpleNamespace(enabled=True, template_key='market', fx_offset=0.1, send_time='09:30')
    def template_keys(self): return ('market','weekend')
    def template_label(self,key): return {'market':'行情早报','weekend':'周末找房'}[key]

def labels(markup):
    return [b.text for row in markup.inline_keyboard for b in row]

def test_complete_broadcast_center_exposes_existing_controls():
    c=BroadcastAdminController(service=Service(), channel_chat_id='-1001', timezone_name='Asia/Phnom_Penh')
    got=labels(c._center_keyboard())
    for expected in ('🧩 选择模板','✏️ 编辑文案','💱 调整汇率','🔘 底部按钮','👁 预览','📤 立即发送','🕒 广播时间','⏸ 关闭定时','⬅️ 返回发布后台'):
        assert expected in got
    times=labels(c._time_keyboard())
    assert all(x in times for x in ('09:30','12:30','18:30','其他时间','⬅️ 返回广播中心'))

def test_broadcast_footer_copy_uses_current_product_labels():
    got=labels(BroadcastAdminController._button_keyboard())
    assert '🔍 帮我找房' in got
    assert '💬 联系我们' in got
    assert '🔍 租房找房' not in got
    assert '💬 租房置业咨询' not in got
