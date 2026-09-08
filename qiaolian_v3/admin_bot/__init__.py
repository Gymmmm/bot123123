from __future__ import annotations

ADMIN_TITLE = '🏠 侨联发布后台'
ADMIN_HOME_ROWS = (
    (('➕ 导入房源', 'manual_intake'), ('🏘 房源管理', 'listing_management')),
    (('⚠️ 异常待处理', 'review'), ('📡 采集管理', 'collector_management')),
    (('📢 广播中心', 'broadcast'), ('📚 发布记录', 'publication_history')),
    (('⚙️ 设置', 'settings'),),
)
ADMIN_ROUTES = {route: label for row in ADMIN_HOME_ROWS for label, route in row}


def home_screen() -> dict[str, object]:
    return {'title': ADMIN_TITLE, 'rows': ADMIN_HOME_ROWS, 'routes': tuple(ADMIN_ROUTES)}


__all__ = ['ADMIN_TITLE', 'ADMIN_HOME_ROWS', 'ADMIN_ROUTES', 'home_screen']
