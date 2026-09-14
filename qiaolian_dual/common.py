APPOINTMENT_MODE_LABELS = {'offline': '实地看房', 'video': '实时视频看房'}
APPOINTMENT_TIME_LABELS = {'am': '上午 09:00–12:00', 'pm': '下午 14:00–17:00', 'evening': '晚上 17:00–19:00'}
APPOINTMENT_FOCUS_LABELS = {'ac': '空调型号和老旧程度', 'appliances': '冰箱/洗衣机/家具使用痕迹', 'light_noise': '采光、噪音、窗外环境', 'water': '水压、热水、排水', 'fee_contract': '费用和合同细节'}
APPOINTMENT_FOCUS_ORDER = ['ac', 'appliances', 'light_noise', 'water', 'fee_contract']
APPOINTMENT_STATUS_LABELS = {'pending': '待确认', 'assigned': '顾问联系中', 'contacted': '顾问联系中', 'confirmed': '已确认', 'done': '已完成', 'cancelled': '已取消'}
LEASE_REMINDER_DAYS = (7,)
SERVICE_REQUEST_LABELS = {'repair_ac': '空调', 'repair_water': '热水器 / 漏水排水', 'repair_power': '灯具 / 电路', 'repair_door': '门锁 / 门禁', 'repair_washer': '洗衣机', 'repair_fridge': '冰箱', 'repair_network': '网络', 'repair_furniture': '家具损坏', 'repair_other': '其他设备', 'property': '物业协调'}
PREF_CONDITION_LABELS = {'budget': '预算优先', 'area': '区域优先', 'utility': '必须民水民电', 'parking': '停车方便', 'quiet': '安静不吵', 'sunlight': '采光好', 'pet': '可养宠物', 'furnished': '拎包入住', 'chinese_owner': '中国房东', 'amenity': '电梯/泳池'}
FIND_AREA_CODE_MAP = {
    'rf': '富力城', 'pp': '炳发城', 'ph': '太子幸福广场',
    'bkk1': 'BKK1', 'bkk23': 'BKK2 / BKK3', 'tk': 'TK/7月区', 'koh': '钻石岛',
    'aeon1': '永旺1', 'russian': '俄罗斯市场', 'chroy': '水净华', 'sen': '森速',
    'jinjie': '金街附近',
    'a4': 'BKK1', 'a8': '森速', 'a6': 'TK/7月区', 'a7': '洪森大道',
    'a41': 'BKK2', 'a42': 'BKK3', 'a0': '不限',
}
_FIND_AREA_ICONS = {'rf': '🏙', 'pp': '🌆', 'ph': '🌟', 'bkk1': '📍', 'tk': '🗺', 'koh': '💎'}
FIND_AREA_OPTIONS: list[tuple[str, str]] = [
    (code, f"{_FIND_AREA_ICONS[code]} {get_display_location(FIND_AREA_CODE_MAP[code])}")
    for code in ('rf', 'pp', 'ph', 'bkk1', 'tk', 'koh')
]
