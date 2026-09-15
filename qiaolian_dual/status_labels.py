"""Single source of truth for user-visible publication statuses."""

STATUS_LABELS = {
    "pending": "待审核",
    "approved": "已通过",
    "rejected": "已驳回",
    "draft": "草稿",
    "ready": "待发布",
    "published": "已发布",
}


def status_label(value: object, default: str = "待确认") -> str:
    key = str(value or "").strip().lower()
    return STATUS_LABELS.get(key, default)


__all__ = ["STATUS_LABELS", "status_label"]
