"""Pure tenant-service domain and repair submission boundary for V3 User Bot."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Callable

from v3_core.storage.service_repository import RepairTicket, SQLiteTenantServiceRepository


SERVICE_REQUEST_LABELS = {
    "repair_ac": "空调",
    "repair_water": "热水器 / 漏水排水",
    "repair_power": "灯具 / 电路",
    "repair_door": "门锁 / 门禁",
    "repair_washer": "洗衣机",
    "repair_fridge": "冰箱",
    "repair_network": "网络",
    "repair_furniture": "家具损坏",
    "repair_other": "其他设备",
    "property": "物业协调",
}

SERVICE_SLOT_LABELS = {
    "today": "今天内安排",
    "tomorrow_am": "明天上午",
    "tomorrow_pm": "明天下午",
}

URGENT_ISSUES = frozenset({"repair_water", "repair_power", "repair_door"})


@dataclass(frozen=True)
class ServiceRequestDraft:
    issue_key: str
    issue_label: str
    detail: str = ""
    request_token: str = ""


@dataclass(frozen=True)
class RepairSubmission:
    ticket: RepairTicket
    slot_label: str
    urgent: bool
    created: bool


class TenantService:
    def __init__(
        self,
        repository: SQLiteTenantServiceRepository,
        *,
        now: Callable[[], str] | None = None,
    ):
        self.repository = repository
        self.now = now or (lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    def begin_request(self, issue_key: str, *, request_token: str) -> ServiceRequestDraft:
        clean = str(issue_key or "").strip()
        label = SERVICE_REQUEST_LABELS.get(clean)
        token = str(request_token or "").strip()
        if not label or clean == "property":
            raise ValueError("unsupported_repair_issue")
        if not token:
            raise ValueError("service_request_token_required")
        return ServiceRequestDraft(issue_key=clean, issue_label=label, request_token=token)

    def with_detail(self, draft: ServiceRequestDraft, detail: str) -> ServiceRequestDraft:
        clean = str(detail or "").strip()
        if len(clean) < 4:
            raise ValueError("service_request_detail_too_short")
        return ServiceRequestDraft(
            issue_key=draft.issue_key,
            issue_label=draft.issue_label,
            detail=clean[:800],
            request_token=draft.request_token,
        )

    def submit_repair(
        self,
        *,
        user_id: int,
        draft: ServiceRequestDraft,
        slot: str,
    ) -> RepairSubmission:
        clean_slot = str(slot or "").strip()
        slot_label = SERVICE_SLOT_LABELS.get(clean_slot)
        if slot_label is None:
            raise ValueError("unsupported_service_slot")
        if not str(draft.detail or "").strip():
            raise ValueError("service_request_detail_required")
        if not str(draft.request_token or "").strip():
            raise ValueError("service_request_token_required")
        binding = self.repository.get_active_binding(int(user_id))
        write = self.repository.create_repair_ticket(
            request_token=draft.request_token,
            user_id=int(user_id),
            binding=binding,
            issue_key=draft.issue_key,
            issue_type=draft.issue_label,
            description=f"{draft.detail}\n希望时间：{slot_label}",
            time_slot=clean_slot,
            created_at=self.now(),
        )
        return RepairSubmission(
            ticket=write.ticket,
            slot_label=slot_label,
            urgent=draft.issue_key in URGENT_ISSUES,
            created=write.created,
        )


__all__ = [
    "RepairSubmission",
    "SERVICE_REQUEST_LABELS",
    "SERVICE_SLOT_LABELS",
    "ServiceRequestDraft",
    "TenantService",
    "URGENT_ISSUES",
]
