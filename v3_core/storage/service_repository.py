"""V3 service persistence with production tenant bindings and V3 repair tickets."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import sqlite3

DDL = """
CREATE TABLE IF NOT EXISTS tenant_bindings_v3 (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    binding_code TEXT NOT NULL UNIQUE,
    property_name TEXT NOT NULL DEFAULT '',
    lease_end_date TEXT NOT NULL DEFAULT '',
    rent_day INTEGER,
    monthly_rent REAL NOT NULL DEFAULT 0,
    contract_start_date TEXT NOT NULL DEFAULT '',
    contract_end_date TEXT NOT NULL DEFAULT '',
    deposit_months INTEGER NOT NULL DEFAULT 2,
    contract_notes TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'active',
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_tenant_bindings_v3_user_status
    ON tenant_bindings_v3(user_id, status, id DESC);

CREATE TABLE IF NOT EXISTS repair_tickets_v3 (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    request_token TEXT NOT NULL UNIQUE,
    user_id INTEGER NOT NULL,
    binding_id INTEGER,
    property_name TEXT NOT NULL DEFAULT '',
    issue_key TEXT NOT NULL DEFAULT '',
    issue_type TEXT NOT NULL DEFAULT '',
    description TEXT NOT NULL DEFAULT '',
    time_slot TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'new',
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_repair_tickets_v3_user_status
    ON repair_tickets_v3(user_id, status, id DESC);
"""


@dataclass(frozen=True)
class TenantBinding:
    id: int
    user_id: int
    property_name: str
    status: str = "active"
    binding_code: str = ""
    lease_end_date: str = ""
    rent_day: int | None = None
    monthly_rent: float = 0.0
    contract_start_date: str = ""
    contract_end_date: str = ""
    deposit_months: int | None = None
    contract_notes: str = ""


@dataclass(frozen=True)
class RepairTicket:
    id: int
    request_token: str
    user_id: int
    binding_id: int | None
    property_name: str
    issue_key: str
    issue_type: str
    description: str
    time_slot: str
    status: str
    created_at: str


@dataclass(frozen=True)
class RepairTicketWrite:
    ticket: RepairTicket
    created: bool


class SQLiteTenantServiceRepository:
    DDL = DDL

    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path).expanduser().resolve()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    @staticmethod
    def _binding(row: sqlite3.Row) -> TenantBinding:
        keys = set(row.keys())
        def value(name: str, default=None):
            return row[name] if name in keys else default
        return TenantBinding(
            id=int(value("id", 0) or 0),
            user_id=int(value("user_id", 0) or 0),
            property_name=str(value("property_name", "") or ""),
            status=str(value("status", "active") or "active"),
            binding_code=str(value("binding_code", "") or ""),
            lease_end_date=str(value("lease_end_date", "") or ""),
            rent_day=int(value("rent_day")) if value("rent_day") is not None else None,
            monthly_rent=float(value("monthly_rent", 0) or 0),
            contract_start_date=str(value("contract_start_date", "") or ""),
            contract_end_date=str(value("contract_end_date", "") or ""),
            deposit_months=int(value("deposit_months")) if value("deposit_months") is not None else None,
            contract_notes=str(value("contract_notes", "") or ""),
        )

    @staticmethod
    def _ticket(row: sqlite3.Row) -> RepairTicket:
        return RepairTicket(
            id=int(row["id"]),
            request_token=str(row["request_token"] or ""),
            user_id=int(row["user_id"]),
            binding_id=int(row["binding_id"]) if row["binding_id"] is not None else None,
            property_name=str(row["property_name"] or ""),
            issue_key=str(row["issue_key"] or ""),
            issue_type=str(row["issue_type"] or ""),
            description=str(row["description"] or ""),
            time_slot=str(row["time_slot"] or ""),
            status=str(row["status"] or "new"),
            created_at=str(row["created_at"] or ""),
        )

    def get_active_binding(self, user_id: int) -> TenantBinding | None:
        with self._connect() as conn:
            row = conn.execute(
                """SELECT id,user_id,binding_code,property_name,lease_end_date,rent_day,
                          monthly_rent,contract_start_date,contract_end_date,deposit_months,
                          contract_notes,status
                   FROM tenant_bindings
                   WHERE user_id=? AND status='active'
                   ORDER BY id DESC LIMIT 1""",
                (int(user_id),),
            ).fetchone()
        return None if row is None else self._binding(row)

    def create_repair_ticket(
        self,
        *,
        request_token: str,
        user_id: int,
        binding: TenantBinding | None,
        issue_key: str,
        issue_type: str,
        description: str,
        time_slot: str,
        created_at: str,
    ) -> RepairTicketWrite:
        clean_token = str(request_token or "").strip()
        clean_user_id = int(user_id or 0)
        if not clean_token:
            raise ValueError("service_request_token_required")
        if clean_user_id <= 0:
            raise ValueError("service_user_id_required")
        if binding is None or int(binding.user_id) != clean_user_id or str(binding.status) != "active":
            raise PermissionError("active_tenant_binding_required")
        issue_key = str(issue_key or "").strip()
        issue_type = str(issue_type or "").strip()
        description = str(description or "").strip()
        time_slot = str(time_slot or "").strip()
        created_at = str(created_at or "").strip()
        if not issue_key or not issue_type or not description or not time_slot or not created_at:
            raise ValueError("service_ticket_fields_required")
        binding_id = int(binding.id)
        property_name = str(binding.property_name or "")
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute(
                "SELECT * FROM repair_tickets_v3 WHERE request_token=? LIMIT 1",
                (clean_token,),
            ).fetchone()
            if existing is not None:
                conn.commit()
                return RepairTicketWrite(ticket=self._ticket(existing), created=False)
            cur = conn.execute(
                """INSERT INTO repair_tickets_v3
                   (request_token,user_id,binding_id,property_name,issue_key,issue_type,
                    description,time_slot,status,created_at)
                   VALUES (?,?,?,?,?,?,?,?,'new',?)""",
                (
                    clean_token, clean_user_id, binding_id, property_name, issue_key,
                    issue_type, description, time_slot, created_at,
                ),
            )
            ticket_id = int(cur.lastrowid)
            row = conn.execute("SELECT * FROM repair_tickets_v3 WHERE id=?", (ticket_id,)).fetchone()
            conn.commit()
        if row is None:
            raise RuntimeError("service_ticket_insert_missing_row")
        return RepairTicketWrite(ticket=self._ticket(row), created=True)


__all__ = ["DDL", "RepairTicket", "RepairTicketWrite", "SQLiteTenantServiceRepository", "TenantBinding"]
