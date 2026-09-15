"""Additive V3 persistence for tenant bindings and repair tickets."""
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
import sqlite3

DDL = """
CREATE TABLE IF NOT EXISTS tenant_bindings_v3 (
 id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER NOT NULL,binding_code TEXT NOT NULL UNIQUE,
 property_name TEXT NOT NULL DEFAULT '',lease_end_date TEXT NOT NULL DEFAULT '',rent_day INTEGER,
 monthly_rent REAL NOT NULL DEFAULT 0,contract_start_date TEXT NOT NULL DEFAULT '',contract_end_date TEXT NOT NULL DEFAULT '',
 deposit_months INTEGER NOT NULL DEFAULT 2,contract_notes TEXT NOT NULL DEFAULT '',status TEXT NOT NULL DEFAULT 'active',created_at TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS idx_tenant_bindings_v3_user_status ON tenant_bindings_v3(user_id,status,id DESC);
CREATE TABLE IF NOT EXISTS repair_tickets_v3 (
 id INTEGER PRIMARY KEY AUTOINCREMENT,request_token TEXT NOT NULL UNIQUE,user_id INTEGER NOT NULL,binding_id INTEGER,
 property_name TEXT NOT NULL DEFAULT '',issue_key TEXT NOT NULL DEFAULT '',issue_type TEXT NOT NULL DEFAULT '',description TEXT NOT NULL DEFAULT '',
 time_slot TEXT NOT NULL DEFAULT '',status TEXT NOT NULL DEFAULT 'new',created_at TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS idx_repair_tickets_v3_user_status ON repair_tickets_v3(user_id,status,id DESC);
"""

@dataclass(frozen=True)
class TenantBinding:
    id: int
    user_id: int
    property_name: str
    status: str = "active"
    lease_end_date: str = ""
    rent_day: int | None = None
    monthly_rent: float = 0
    contract_start_date: str = ""
    contract_end_date: str = ""
    deposit_months: int = 2

@dataclass(frozen=True)
class RepairTicket:
    id:int; request_token:str; user_id:int; binding_id:int|None; property_name:str; issue_key:str; issue_type:str; description:str; time_slot:str; status:str; created_at:str

@dataclass(frozen=True)
class RepairTicketWrite:
    ticket: RepairTicket
    created: bool

class SQLiteTenantServiceRepository:
    DDL=DDL
    def __init__(self,db_path:str|Path): self.db_path=Path(db_path).expanduser().resolve()
    def _connect(self):
        conn=sqlite3.connect(str(self.db_path),timeout=30); conn.row_factory=sqlite3.Row; conn.execute("PRAGMA busy_timeout=30000"); return conn
    @staticmethod
    def _ticket(row):
        return RepairTicket(int(row['id']),str(row['request_token'] or ''),int(row['user_id']),int(row['binding_id']) if row['binding_id'] is not None else None,str(row['property_name'] or ''),str(row['issue_key'] or ''),str(row['issue_type'] or ''),str(row['description'] or ''),str(row['time_slot'] or ''),str(row['status'] or 'new'),str(row['created_at'] or ''))
    @staticmethod
    def _binding(row):
        return TenantBinding(id=int(row['id']),user_id=int(row['user_id']),property_name=str(row['property_name'] or ''),status=str(row['status'] or 'active'),lease_end_date=str(row['lease_end_date'] or ''),rent_day=int(row['rent_day']) if row['rent_day'] is not None else None,monthly_rent=float(row['monthly_rent'] or 0),contract_start_date=str(row['contract_start_date'] or ''),contract_end_date=str(row['contract_end_date'] or ''),deposit_months=int(row['deposit_months'] or 0))
    def get_active_binding(self,user_id:int):
        with self._connect() as conn:
            row=conn.execute("""SELECT id,user_id,property_name,status,lease_end_date,rent_day,monthly_rent,contract_start_date,contract_end_date,deposit_months FROM tenant_bindings_v3 WHERE user_id=? AND status='active' ORDER BY id DESC LIMIT 1""",(int(user_id),)).fetchone()
        return self._binding(row) if row else None
    def create_repair_ticket(self,*,request_token,user_id,binding,issue_key,issue_type,description,time_slot,created_at):
        token=str(request_token or '').strip(); uid=int(user_id or 0)
        if not token: raise ValueError('service_request_token_required')
        if uid<=0: raise ValueError('service_user_id_required')
        issue_key=str(issue_key or '').strip(); issue_type=str(issue_type or '').strip(); description=str(description or '').strip(); time_slot=str(time_slot or '').strip(); created_at=str(created_at or '').strip()
        if not all((issue_key,issue_type,description,time_slot,created_at)): raise ValueError('service_ticket_fields_required')
        binding_id=int(binding.id) if binding else None; property_name=str(binding.property_name or '') if binding else ''
        with self._connect() as conn:
            conn.execute('BEGIN IMMEDIATE'); existing=conn.execute('SELECT * FROM repair_tickets_v3 WHERE request_token=? LIMIT 1',(token,)).fetchone()
            if existing is not None: conn.commit(); return RepairTicketWrite(self._ticket(existing),False)
            cur=conn.execute("""INSERT INTO repair_tickets_v3(request_token,user_id,binding_id,property_name,issue_key,issue_type,description,time_slot,status,created_at) VALUES(?,?,?,?,?,?,?,?,'new',?)""",(token,uid,binding_id,property_name,issue_key,issue_type,description,time_slot,created_at)); row=conn.execute('SELECT * FROM repair_tickets_v3 WHERE id=?',(int(cur.lastrowid),)).fetchone(); conn.commit()
        if row is None: raise RuntimeError('service_ticket_insert_missing_row')
        return RepairTicketWrite(self._ticket(row),True)

__all__=['DDL','RepairTicket','RepairTicketWrite','SQLiteTenantServiceRepository','TenantBinding']
