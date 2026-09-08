from __future__ import annotations

BRAND = '侨联小管家'
HOME_ROWS = (
    (('🔍 帮我找房', 'find_home'), ('📅 我的预约', 'my_appointments')),
    (('🛡 侨联保障', 'guarantee'), ('🛠 入住服务', 'move_in_services')),
    (('房源频道', 'channel'), ('💬 联系我们', 'contact_us')),
)
REACHABLE_SERVICES = (
    'property_coordination', 'life_services', 'nearby_needs', 'local:rfcity',
    'lease', 'repair',
)


def start_screen() -> dict[str, object]:
    return {'brand': BRAND, 'rows': HOME_ROWS, 'services': REACHABLE_SERVICES}


__all__ = ['BRAND', 'HOME_ROWS', 'REACHABLE_SERVICES', 'start_screen']
