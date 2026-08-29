"""从 user_bot.py 拆分出的职责模块。"""
from __future__ import annotations

from .common import *
def _search_type_button_rows() -> list[list[InlineKeyboardButton]]:
    return [[InlineKeyboardButton('🏢 公寓', callback_data='findtype:住宅'), InlineKeyboardButton('🏡 别墅/排屋', callback_data='findtype:别墅/排屋')], [InlineKeyboardButton('🏬 商铺/办公', callback_data='findtype:商铺/办公'), InlineKeyboardButton('不限类型', callback_data='findtype:any')]]

def search_entry_keyboard() -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    rows.extend(_search_type_button_rows())
    rows.append([InlineKeyboardButton('🏠 返回首页', callback_data='home')])
    return InlineKeyboardMarkup(rows)

def guided_search_keyboard() -> InlineKeyboardMarkup:
    rows = list(_search_type_button_rows())
    rows.append([InlineKeyboardButton('🏠 返回首页', callback_data='home')])
    return InlineKeyboardMarkup(rows)

def find_area_keyboard() -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    row: list[InlineKeyboardButton] = []
    for idx, (code, label) in enumerate(FIND_AREA_OPTIONS, start=1):
        row.append(InlineKeyboardButton(label, callback_data=f'findarea:{code}'))
        if idx % 2 == 0:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton('🔍 其他区域', callback_data='findarea:other'), InlineKeyboardButton('⬅️ 返回', callback_data='home')])
    return InlineKeyboardMarkup(rows)

def _budget_options_for_goal(goal: str) -> list[tuple[str, str, int | None, int | None]]:
    key = goal if goal in FIND_BUDGET_OPTIONS else 'any'
    return FIND_BUDGET_OPTIONS.get(key, FIND_BUDGET_OPTIONS['any'])

def find_budget_keyboard(goal: str) -> InlineKeyboardMarkup:
    options = _budget_options_for_goal(goal)
    rows: list[list[InlineKeyboardButton]] = []
    row: list[InlineKeyboardButton] = []
    for idx, (code, label, _, _) in enumerate(options, start=1):
        row.append(InlineKeyboardButton(label, callback_data=f'findbudget:{code}'))
        if idx % 2 == 0:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton('⬅️ 上一步', callback_data='findback:area'), InlineKeyboardButton('🏠 首页', callback_data='home')])
    return InlineKeyboardMarkup(rows)

def _decode_budget_choice(goal: str, code: str) -> tuple[str, int | None, int | None]:
    for opt_code, label, bmin, bmax in _budget_options_for_goal(goal):
        if opt_code == code:
            return (label, bmin, bmax)
    return ('不限', None, None)

def appointment_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton('📍 实地看房', callback_data='appointment_menu:offline'), InlineKeyboardButton('📹 视频看房', callback_data='appointment_menu:video')], [InlineKeyboardButton('📅 我的预约', callback_data='appointment_menu:list'), InlineKeyboardButton('💬 联系中文顾问', callback_data='appointment_menu:contact')], [InlineKeyboardButton('⬅️ 返回首页', callback_data='home')]])

def precise_filter_keyboard(selected: set[str] | None=None) -> InlineKeyboardMarkup:
    picked = selected or set()
    def _btn(key: str) -> InlineKeyboardButton:
        label = PREF_CONDITION_LABELS.get(key, key)
        prefix = '✅ ' if key in picked else '▫️ '
        return InlineKeyboardButton(f'{prefix}{label}', callback_data=f'pref:toggle:{key}')
    rows = [[_btn('budget'), _btn('area')], [_btn('utility'), _btn('parking')], [_btn('quiet'), _btn('sunlight')], [_btn('pet'), _btn('furnished')], [_btn('chinese_owner'), _btn('amenity')], [InlineKeyboardButton('✅ 提交条件', callback_data='pref:submit'), InlineKeyboardButton('♻️ 清空', callback_data='pref:clear')], [InlineKeyboardButton('🏠 返回首页', callback_data='home')]]
    return InlineKeyboardMarkup(rows)

def service_hub_keyboard(user_id: int | None=None) -> InlineKeyboardMarkup:
    binding = db.get_active_binding(user_id) if user_id else None
    if not binding:
        return InlineKeyboardMarkup([
            [InlineKeyboardButton('🔧 报修', callback_data='service:repair_hub'), InlineKeyboardButton('🏢 物业沟通', callback_data='service_request:property')],
            [InlineKeyboardButton('🗺️ 周边生活', callback_data='service:local_life')],
            [InlineKeyboardButton('🔗 绑定我的租约', callback_data='profile:repeat')],
            [InlineKeyboardButton('💬 联系中文顾问', callback_data='service:contact')],
            [InlineKeyboardButton('⬅️ 返回首页', callback_data='home')],
        ])
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('📋 我的租约', callback_data='contract:view')],
        [InlineKeyboardButton('🔧 报修', callback_data='service:repair_hub'), InlineKeyboardButton('🏢 物业沟通', callback_data='service_request:property')],
        [InlineKeyboardButton('🗺️ 周边生活', callback_data='service:local_life')],
        [InlineKeyboardButton('💬 联系中文顾问', callback_data='service:contact')],
        [InlineKeyboardButton('⬅️ 返回首页', callback_data='home')],
    ])

def service_repair_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton('❄️ 空调/家电', callback_data='service_request:repair_ac'), InlineKeyboardButton('🚿 漏水/堵水', callback_data='service_request:repair_water')], [InlineKeyboardButton('💡 灯具/电路', callback_data='service_request:repair_power'), InlineKeyboardButton('🚪 门锁/门窗', callback_data='service_request:repair_door')], [InlineKeyboardButton('📦 家具损坏', callback_data='service_request:repair_furniture'), InlineKeyboardButton('🔧 其他问题', callback_data='service_request:repair_other')], [InlineKeyboardButton('⬅️ 返回入住服务', callback_data='service:hub')]])

def service_detail_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton('💬 联系中文顾问', callback_data='service:contact')], [InlineKeyboardButton('⬅️ 返回入住服务', callback_data='service:hub')]])

def local_life_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('🍴 餐厅', callback_data='rfcity:restaurant'), InlineKeyboardButton('🛒 超市', callback_data='rfcity:supermarket')],
        [InlineKeyboardButton('🥤 饮品', callback_data='rfcity:drinks'), InlineKeyboardButton('🚛 快递', callback_data='rfcity:logistics')],
        [InlineKeyboardButton('💆 休闲', callback_data='rfcity:recreation'), InlineKeyboardButton('📋 全部分类', callback_data='local:rfcity')],
        [InlineKeyboardButton('⬅️ 返回入住后服务', callback_data='service:hub')],
    ])

def rfcity_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton('🍴 餐厅小吃', callback_data='rfcity:restaurant'), InlineKeyboardButton('🔥 烧烤夜宵', callback_data='rfcity:bbq')], [InlineKeyboardButton('🥤 奶茶饮品', callback_data='rfcity:drinks'), InlineKeyboardButton('🛒 超市便利', callback_data='rfcity:supermarket')], [InlineKeyboardButton('🏨 酒店租房', callback_data='rfcity:hotel'), InlineKeyboardButton('🏋️ 休闲生活', callback_data='rfcity:recreation')], [InlineKeyboardButton('🚛 快递物流', callback_data='rfcity:logistics'), InlineKeyboardButton('👨‍💻 富力物业', callback_data='rfcity:property')], [InlineKeyboardButton('⬅️ 返回周边生活', callback_data='service:local_life')]])

def rfcity_back_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton('🏙 返回富力周边', callback_data='local:rfcity')]])

def merchant_join_keyboard() -> InlineKeyboardMarkup:
    from .keyboards_common import _advisor_tg_url
    advisor_url = _advisor_tg_url()
    rows: list[list[InlineKeyboardButton]] = []
    if advisor_url:
        rows.append([InlineKeyboardButton('📩 提交商家信息', url=advisor_url)])
        rows.append([InlineKeyboardButton('💬 联系侨联合作', url=advisor_url)])
    else:
        rows.append([InlineKeyboardButton('📩 提交商家信息', callback_data='service:contact')])
        rows.append([InlineKeyboardButton('💬 联系侨联合作', callback_data='service:contact')])
    rows.append([InlineKeyboardButton('🏙 返回富力周边', callback_data='local:rfcity')])
    return InlineKeyboardMarkup(rows)
