"""从 user_bot.py 拆分出的职责模块。"""
from __future__ import annotations

from .common import *


def _search_type_button_rows() -> list[list[InlineKeyboardButton]]:
    return [
        [InlineKeyboardButton('🏢 公寓', callback_data='findtype:住宅'), InlineKeyboardButton('🏡 别墅', callback_data='findtype:别墅')],
        [InlineKeyboardButton('🏘 排屋', callback_data='findtype:排屋'), InlineKeyboardButton('🏪 商铺', callback_data='findtype:商铺')],
        [InlineKeyboardButton('💼 办公室', callback_data='findtype:办公'), InlineKeyboardButton('不限类型', callback_data='findtype:any')],
    ]


def search_entry_keyboard() -> InlineKeyboardMarkup:
    return guided_search_keyboard()


def guided_search_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('📍 按区域', callback_data='hub:area'), InlineKeyboardButton('💰 按预算', callback_data='hub:budget')],
        [InlineKeyboardButton('🏠 按户型', callback_data='hub:layout'), InlineKeyboardButton('🏘 当前可约', callback_data='hub:available')],
        [InlineKeyboardButton('⬅️ 返回首页', callback_data='home')],
    ])


def find_area_keyboard() -> InlineKeyboardMarkup:
    options = [
        ('bkk1', 'BKK1'), ('bkk23', 'BKK2/3'),
        ('koh', '钻石岛'), ('rf', '富力城'),
        ('aeon1', '永旺1'), ('tk', 'TK'),
        ('russian', '俄市'), ('pp', '炳发城'),
        ('chroy', '水净华'), ('sen', '森速'),
    ]
    rows = []
    for index in range(0, len(options), 2):
        rows.append([
            InlineKeyboardButton(options[index][1], callback_data=f'findarea:{options[index][0]}'),
            InlineKeyboardButton(options[index + 1][1], callback_data=f'findarea:{options[index + 1][0]}'),
        ])
    rows.append([InlineKeyboardButton('📍 其他区域', callback_data='findarea:other')])
    rows.append([InlineKeyboardButton('⬅️ 返回', callback_data='home_smart_search')])
    return InlineKeyboardMarkup(rows)


def _budget_options_for_goal(goal: str) -> list[tuple[str, str, int | None, int | None]]:
    return [
        ('b1', '$400以内', None, 400),
        ('b2', '$400–600', 400, 600),
        ('b3', '$600–800', 600, 800),
        ('b4', '$800–1200', 800, 1200),
        ('b5', '$1200–1500', 1200, 1500),
        ('b6', '$1500+', 1500, None),
    ]


def find_budget_keyboard(goal: str) -> InlineKeyboardMarkup:
    options = _budget_options_for_goal(goal)
    rows = []
    for index in range(0, len(options), 2):
        rows.append([
            InlineKeyboardButton(options[index][1], callback_data=f'findbudget:{options[index][0]}'),
            InlineKeyboardButton(options[index + 1][1], callback_data=f'findbudget:{options[index + 1][0]}'),
        ])
    rows.append([InlineKeyboardButton('✍️ 自己输入', callback_data='findbudget:other')])
    rows.append([InlineKeyboardButton('⬅️ 返回', callback_data='home_smart_search')])
    return InlineKeyboardMarkup(rows)


def _decode_budget_choice(goal: str, code: str) -> tuple[str, int | None, int | None]:
    for opt_code, label, bmin, bmax in _budget_options_for_goal(goal):
        if opt_code == code:
            return (label, bmin, bmax)
    return ('不限', None, None)


def appointment_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('📅 预约看房', callback_data='appointment_menu:offline')],
        [InlineKeyboardButton('📅 我的预约', callback_data='appointment_menu:list'), InlineKeyboardButton('💬 联系我们', callback_data='appointment_menu:contact')],
        [InlineKeyboardButton('⬅️ 返回首页', callback_data='home')],
    ])


def precise_filter_keyboard(selected: set[str] | None=None) -> InlineKeyboardMarkup:
    picked = selected or set()
    def _btn(key: str) -> InlineKeyboardButton:
        label = PREF_CONDITION_LABELS.get(key, key)
        prefix = '✅ ' if key in picked else '▫️ '
        return InlineKeyboardButton(f'{prefix}{label}', callback_data=f'pref:toggle:{key}')
    rows = [
        [_btn('budget'), _btn('area')], [_btn('utility'), _btn('parking')],
        [_btn('quiet'), _btn('sunlight')], [_btn('pet'), _btn('furnished')],
        [_btn('chinese_owner'), _btn('amenity')],
        [InlineKeyboardButton('✅ 提交条件', callback_data='pref:submit'), InlineKeyboardButton('♻️ 清空', callback_data='pref:clear')],
        [InlineKeyboardButton('🏠 返回首页', callback_data='home')],
    ]
    return InlineKeyboardMarkup(rows)


def service_hub_keyboard(user_id: int | None=None) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('🔧 设备报修', callback_data='service:repair_hub'), InlineKeyboardButton('🏢 物业协调', callback_data='service_request:property')],
        [InlineKeyboardButton('📦 生活服务', callback_data='service:local_life'), InlineKeyboardButton('💬 其他帮助', callback_data='service:general')],
        [InlineKeyboardButton('⬅️ 返回首页', callback_data='home')],
    ])

def service_repair_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('❄️ 空调', callback_data='service_request:repair_ac'), InlineKeyboardButton('🚿 热水器', callback_data='service_request:repair_water')],
        [InlineKeyboardButton('🧺 洗衣机', callback_data='service_request:repair_washer'), InlineKeyboardButton('🧊 冰箱', callback_data='service_request:repair_fridge')],
        [InlineKeyboardButton('📶 网络', callback_data='service_request:repair_network'), InlineKeyboardButton('🔐 门锁/门禁', callback_data='service_request:repair_door')],
        [InlineKeyboardButton('🔧 其他设备', callback_data='service_request:repair_other')],
        [InlineKeyboardButton('⬅️ 返回', callback_data='service:hub')],
    ])

def service_detail_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('💬 联系我们', callback_data='service:contact')],
        [InlineKeyboardButton('⬅️ 返回入住服务', callback_data='service:hub')],
    ])


def local_life_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('🧹 保洁家政', callback_data='service:contact'), InlineKeyboardButton('🚚 搬家协助', callback_data='hub:rental:moving')],
        [InlineKeyboardButton('🗺 周边推荐', callback_data='service:nearby'), InlineKeyboardButton('💬 其他需求', callback_data='service:general')],
        [InlineKeyboardButton('⬅️ 返回', callback_data='service:hub')],
    ])


def nearby_area_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('🏙 富力城导航', callback_data='local:rfcity')],
        [InlineKeyboardButton('📍 其他区域', callback_data='local:other')],
        [InlineKeyboardButton('⬅️ 返回生活服务', callback_data='service:local_life')],
    ])

def rfcity_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('🍴 餐厅小吃', callback_data='rfcity:restaurant'), InlineKeyboardButton('🔥 烧烤夜宵', callback_data='rfcity:bbq')],
        [InlineKeyboardButton('🥤 奶茶饮品', callback_data='rfcity:drinks'), InlineKeyboardButton('🛒 超市便利', callback_data='rfcity:supermarket')],
        [InlineKeyboardButton('🏨 酒店租房', callback_data='rfcity:hotel'), InlineKeyboardButton('🏋️ 休闲生活', callback_data='rfcity:recreation')],
        [InlineKeyboardButton('🚛 快递物流', callback_data='rfcity:logistics'), InlineKeyboardButton('👨‍💻 富力物业', callback_data='rfcity:property')],
        [InlineKeyboardButton('⬅️ 返回生活服务', callback_data='service:local_life')],
    ])


def rfcity_back_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton('⬅️ 返回生活服务', callback_data='service:local_life')]])


def merchant_join_keyboard() -> InlineKeyboardMarkup:
    from .keyboards_common import _advisor_tg_url
    advisor_url = _advisor_tg_url()
    rows: list[list[InlineKeyboardButton]] = []
    if advisor_url:
        rows.append([InlineKeyboardButton('📩 提交商家信息', url=advisor_url)])
        rows.append([InlineKeyboardButton('💬 联系我们', url=advisor_url)])
    else:
        rows.append([InlineKeyboardButton('📩 提交商家信息', callback_data='service:contact')])
        rows.append([InlineKeyboardButton('💬 联系我们', callback_data='service:contact')])
    rows.append([InlineKeyboardButton('⬅️ 返回生活服务', callback_data='service:local_life')])
    return InlineKeyboardMarkup(rows)
