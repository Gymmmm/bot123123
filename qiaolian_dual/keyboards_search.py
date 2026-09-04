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
    """找房首页只保留四个筛选入口 + 返回，避免手机端按钮过多。"""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('📍 按区域找', callback_data='hub:area'), InlineKeyboardButton('💰 按预算找', callback_data='hub:budget')],
        [InlineKeyboardButton('🏠 按类型找', callback_data='hub:layout'), InlineKeyboardButton('🏘 当前可预约', callback_data='hub:available')],
        [InlineKeyboardButton('⬅️ 返回首页', callback_data='home')],
    ])


def find_area_keyboard() -> InlineKeyboardMarkup:
    options = [
        ('bkk1', 'BKK1'), ('bkk23', 'BKK2 / BKK3'),
        ('koh', '钻石岛'), ('rf', '富力城'),
        ('aeon1', '永旺1'), ('tk', 'TK / 堆谷'),
        ('russian', '俄罗斯市场'), ('pp', '炳发城'),
        ('chroy', '水净华'), ('sen', '森速'),
    ]
    rows = []
    for index in range(0, len(options), 2):
        rows.append([
            InlineKeyboardButton(options[index][1], callback_data=f'findarea:{options[index][0]}'),
            InlineKeyboardButton(options[index + 1][1], callback_data=f'findarea:{options[index + 1][0]}'),
        ])
    rows.append([InlineKeyboardButton('✍️ 其他位置', callback_data='findarea:other')])
    rows.append([InlineKeyboardButton('⬅️ 返回找房', callback_data='home_smart_search')])
    return InlineKeyboardMarkup(rows)


def _budget_options_for_goal(goal: str) -> list[tuple[str, str, int | None, int | None]]:
    return [
        ('b1', '$400以内', None, 400),
        ('b2', '$400–600', 400, 600),
        ('b3', '$600–800', 600, 800),
        ('b4', '$800–1200', 800, 1200),
        ('b5', '$1200–1500', 1200, 1500),
        ('b6', '$1500以上', 1500, None),
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
    rows.append([InlineKeyboardButton('⬅️ 返回找房', callback_data='home_smart_search')])
    return InlineKeyboardMarkup(rows)


def _decode_budget_choice(goal: str, code: str) -> tuple[str, int | None, int | None]:
    for opt_code, label, bmin, bmax in _budget_options_for_goal(goal):
        if opt_code == code:
            return (label, bmin, bmax)
    return ('不限', None, None)


def appointment_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('📅 预约看房', callback_data='appointment_menu:offline')],
        [InlineKeyboardButton('📅 我的预约', callback_data='appointment_menu:list'), InlineKeyboardButton('💬 联系中文顾问', callback_data='appointment_menu:contact')],
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
    """入住服务不再向客户暴露自助绑定租约入口。"""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('🔧 报修', callback_data='service:repair_hub'), InlineKeyboardButton('🏢 物业沟通', callback_data='service_request:property')],
        [InlineKeyboardButton('🗺 周边生活', callback_data='service:local_life'), InlineKeyboardButton('📋 我的租约', callback_data='contract:view')],
        [InlineKeyboardButton('💬 联系中文顾问', callback_data='service:contact')],
        [InlineKeyboardButton('⬅️ 返回首页', callback_data='home')],
    ])


def service_repair_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('❄️ 空调 / 家电', callback_data='service_request:repair_ac')],
        [InlineKeyboardButton('💧 漏水 / 排水', callback_data='service_request:repair_water')],
        [InlineKeyboardButton('🔌 灯具 / 电路', callback_data='service_request:repair_power')],
        [InlineKeyboardButton('🔐 门锁 / 门窗', callback_data='service_request:repair_door')],
        [InlineKeyboardButton('🪑 家具损坏', callback_data='service_request:repair_furniture')],
        [InlineKeyboardButton('💬 其他问题', callback_data='service_request:repair_other')],
        [InlineKeyboardButton('⬅️ 返回入住服务', callback_data='service:hub')],
    ])


def service_detail_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('💬 联系中文顾问', callback_data='service:contact')],
        [InlineKeyboardButton('⬅️ 返回入住服务', callback_data='service:hub')],
    ])


def local_life_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('🍴 餐厅', callback_data='rfcity:restaurant'), InlineKeyboardButton('🛒 超市', callback_data='rfcity:supermarket')],
        [InlineKeyboardButton('🥤 饮品', callback_data='rfcity:drinks'), InlineKeyboardButton('🚛 快递', callback_data='rfcity:logistics')],
        [InlineKeyboardButton('💆 休闲', callback_data='rfcity:recreation'), InlineKeyboardButton('📋 全部分类', callback_data='local:rfcity')],
        [InlineKeyboardButton('⬅️ 返回入住服务', callback_data='service:hub')],
    ])


def rfcity_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('🍴 餐厅小吃', callback_data='rfcity:restaurant'), InlineKeyboardButton('🔥 烧烤夜宵', callback_data='rfcity:bbq')],
        [InlineKeyboardButton('🥤 奶茶饮品', callback_data='rfcity:drinks'), InlineKeyboardButton('🛒 超市便利', callback_data='rfcity:supermarket')],
        [InlineKeyboardButton('🏨 酒店租房', callback_data='rfcity:hotel'), InlineKeyboardButton('🏋️ 休闲生活', callback_data='rfcity:recreation')],
        [InlineKeyboardButton('🚛 快递物流', callback_data='rfcity:logistics'), InlineKeyboardButton('👨‍💻 富力物业', callback_data='rfcity:property')],
        [InlineKeyboardButton('⬅️ 返回周边生活', callback_data='service:local_life')],
    ])


def rfcity_back_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton('⬅️ 返回周边生活', callback_data='local:rfcity')]])


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
    rows.append([InlineKeyboardButton('⬅️ 返回周边生活', callback_data='local:rfcity')])
    return InlineKeyboardMarkup(rows)
