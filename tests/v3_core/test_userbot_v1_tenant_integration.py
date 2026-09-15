from pathlib import Path
import sqlite3
import pytest
from v3_core.storage.service_repository import DDL,SQLiteTenantServiceRepository
from v3_core.user_bot.home_views import build_home_view
from v3_core.user_bot.service_flow import TenantService,ServiceRequestDraft
from v3_core.user_bot.tenant_v1 import tenant_home_view,guide_view,handover_view,deposit_view,renew_view,terminate_view

def _db(tmp_path:Path):
    p=tmp_path/'q.db'
    with sqlite3.connect(p) as c:
        c.executescript(DDL)
        c.executescript("""CREATE TABLE renewal_tracking(id INTEGER PRIMARY KEY AUTOINCREMENT,binding_id INTEGER NOT NULL,user_id INTEGER NOT NULL,listing_id TEXT NOT NULL DEFAULT '',renewal_status TEXT NOT NULL DEFAULT 'pending',user_response TEXT NOT NULL DEFAULT '',advisor_notes TEXT NOT NULL DEFAULT '',contacted_at TEXT NOT NULL DEFAULT '',completed_at TEXT NOT NULL DEFAULT '',created_at TEXT NOT NULL);""")
    return p

def _labels(view): return [c.label for row in view.rows for c in row]
def test_v3_home_is_v1_surface():
    labels=_labels(build_home_view(channel_url='https://t.me/x',advisor_url='https://t.me/a'))
    assert labels==['🔍 开始找房','📅 我的预约','🛠 入住服务','🏠 关于侨联','💬 中文顾问','📢 金边华人租房频道']
    assert '侨联保障' not in ''.join(labels)
def test_unbound_tenant_has_no_privileged_actions(tmp_path):
    s=TenantService(SQLiteTenantServiceRepository(_db(tmp_path))); labels=_labels(tenant_home_view(s,100))
    assert labels==['💬 中文顾问','🏠 返回首页']
    for bad in ('报修','物业','我的租约','续租','退租'): assert bad not in ''.join(labels)
def test_bound_tenant_gets_full_service_surface(tmp_path):
    p=_db(tmp_path)
    with sqlite3.connect(p) as c: c.execute("INSERT INTO tenant_bindings_v3(user_id,binding_code,property_name,lease_end_date,rent_day,monthly_rent,deposit_months,status,created_at) VALUES(1,'B1','测试房源','2027-01-01',5,800,2,'active','2026-09-15')"); c.commit()
    s=TenantService(SQLiteTenantServiceRepository(p)); labels=_labels(tenant_home_view(s,1))
    for expected in ('🔧 报修','🏢 物业沟通','📋 我的租约','📄 租赁服务指南','🔄 续租','🚪 退租'): assert expected in labels
    assert '换房' not in ''.join(labels)
    assert '📥 下载入住交接清单' in _labels(handover_view(s,1)); assert '📥 下载退租与押金说明' in _labels(deposit_view(s,1))
    assert '换房' not in renew_view(s,1).text+terminate_view(s,1).text+guide_view(s,1).text
def test_expired_or_other_user_binding_cannot_repair(tmp_path):
    p=_db(tmp_path)
    with sqlite3.connect(p) as c: c.execute("INSERT INTO tenant_bindings_v3(user_id,binding_code,property_name,status,created_at) VALUES(1,'B1','旧房','inactive','2026-09-15')"); c.commit()
    s=TenantService(SQLiteTenantServiceRepository(p)); draft=ServiceRequestDraft('repair_ac','空调','不制冷','tok')
    with pytest.raises(PermissionError): s.submit_repair(user_id=1,draft=draft,slot='today')
    with pytest.raises(PermissionError): s.submit_repair(user_id=2,draft=draft,slot='today')
