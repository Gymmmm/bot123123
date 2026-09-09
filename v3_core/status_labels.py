"""Single source of truth for V3 user-visible statuses."""
from __future__ import annotations

REVIEW_STATUS_LABELS = {
    "pending": "待审核", "approved": "已批准事实", "hold": "暂缓", "rejected": "已拒绝",
}
PACKAGE_STATUS_LABELS = {
    "package_ready": "待冻结发布包", "approved": "待发布", "published": "已发布", "superseded": "已替换",
}
DELIVERY_STATE_LABELS = {
    "prepared": "待发送", "sending": "发送中", "sent": "已发送待提交", "committed": "已提交",
    "failed_before_send": "发送前失败", "unknown": "状态未知",
}
APPOINTMENT_STATUS_LABELS = {
    "pending": ("🟡", "等待确认"), "assigned": ("🟡", "等待确认"),
    "contacted": ("🟡", "等待确认"), "confirmed": ("🟢", "预约已确认"),
    "done": ("🔵", "看房已完成"), "cancelled": ("⚪", "已取消"),
}

def status_label(mapping, value: object, default: str = "待确认"):
    return mapping.get(str(value or "").strip().lower(), default)

