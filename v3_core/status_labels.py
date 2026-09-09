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

# Reused by User Bot details, new channel posts and published-message status sync.
# The third item is whether the current inventory state may expose booking UI.
LISTING_STATUS_PRESENTATION = {
    "active": ("🟢", "当前可预约", True),
    "reserved": ("🟡", "已有预约 · 仍可预约", True),
    "pending": ("🔵", "房态确认中", False),
    "rented": ("🔴", "已租出", False),
    "inactive": ("⚫", "已下架", False),
    "offline": ("⚫", "已下架", False),
}


def status_label(mapping, value: object, default: str = "待确认"):
    return mapping.get(str(value or "").strip().lower(), default)


def listing_status_presentation(value: object) -> tuple[str, str, bool]:
    clean = str(value or "").strip().lower()
    return LISTING_STATUS_PRESENTATION.get(clean, LISTING_STATUS_PRESENTATION["pending"])


def listing_status_line(value: object, public_listing_id: object = "") -> str:
    icon, label, _ = listing_status_presentation(value)
    public_id = str(public_listing_id or "").strip()
    return f"{icon} {label}" + (f"　{public_id}" if public_id else "")


def listing_is_bookable(value: object) -> bool:
    return bool(listing_status_presentation(value)[2])


__all__ = [
    "APPOINTMENT_STATUS_LABELS",
    "DELIVERY_STATE_LABELS",
    "LISTING_STATUS_PRESENTATION",
    "PACKAGE_STATUS_LABELS",
    "REVIEW_STATUS_LABELS",
    "listing_is_bookable",
    "listing_status_line",
    "listing_status_presentation",
    "status_label",
]
