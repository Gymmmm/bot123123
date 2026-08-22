"""从 user_bot.py 拆分出的职责模块。"""
from __future__ import annotations

from .common import *

async def lease_reminder_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """租约到期提醒（30/7/3 天）；按日志去重，默认每天早上触发一次。"""
    from .admin_contract import _binding_end_date
    from .results_admin import _notify_admins
    from .session_deeplink import now_ts
    now = datetime.now()
    for days_before in LEASE_REMINDER_DAYS:
        target_date = (now + timedelta(days=days_before)).strftime('%Y-%m-%d')
        remind_type = f'{days_before}days'
        try:
            bindings = db.list_bindings_expiring_on(target_date)
        except Exception:
            logger.exception('查询到期租约失败: target=%s', target_date)
            continue
        for binding in bindings:
            try:
                user_id = int(binding.get('user_id') or 0)
                binding_id = int(binding.get('id') or 0)
            except (TypeError, ValueError):
                continue
            if user_id <= 0 or binding_id <= 0:
                continue
            if not db.is_lease_reminder_enabled(user_id):
                continue
            if db.has_reminder_sent(binding_id=binding_id, remind_type=remind_type, remind_date=target_date):
                continue
            property_name = str(binding.get('property_name') or '-')
            end_date = _binding_end_date(binding) or target_date
            rent_raw = binding.get('monthly_rent')
            try:
                rent_value = float(rent_raw or 0)
            except (TypeError, ValueError):
                rent_value = 0
            rent_line = f'${int(rent_value)}/月' if rent_value > 0 else '待确认'
            name = str(binding.get('first_name') or '您好')
            text = f'⏰ <b>租约到期提醒</b>\n\n{he(name)}，您的租约即将到期：\n🏠 房号：{he(property_name)}\n💰 月租：{he(rent_line)}\n📅 到期日：{he(end_date)}\n⚠️ 剩余：<b>{days_before} 天</b>\n\n如果准备续租或换房，点下面按钮我们马上跟进。'
            keyboard = InlineKeyboardMarkup([[InlineKeyboardButton('✅ 我要续租', callback_data=f'contract:renew_yes:{binding_id}'), InlineKeyboardButton('🏠 我想换房', callback_data='contract:change')], [InlineKeyboardButton('💬 联系我们', callback_data='appointment_menu:contact')]])
            try:
                await context.bot.send_message(chat_id=user_id, text=text, parse_mode=ParseMode.HTML, reply_markup=keyboard)
                db.log_reminder_sent(binding_id=binding_id, user_id=user_id, lease_end_date=end_date, remind_for_date=target_date, remind_type=remind_type, sent_at=now_ts())
                await _notify_admins(context, title=f'到期提醒已发送（{days_before}天）', lines=[f'用户ID：{he(str(user_id))}', f'房号：{he(property_name)}', f'到期：{he(end_date)}', f'类型：{he(remind_type)}'])
            except Exception:
                logger.exception('发送租约提醒失败: user_id=%s binding_id=%s target=%s', user_id, binding_id, target_date)

async def rent_day_reminder_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """每月交租提醒：在交租日前 7 天早上发送一次，按日志去重。"""
    from .admin_contract import _binding_end_date
    from .results_admin import _notify_admins
    from .session_deeplink import now_ts
    now = datetime.now()
    target = now + timedelta(days=7)
    rent_day = target.day
    remind_date = target.strftime('%Y-%m-%d')
    remind_type = 'rent_7days'
    try:
        bindings = db.list_bindings_with_rent_day(rent_day)
    except Exception:
        logger.exception('查询交租日租约失败: rent_day=%s', rent_day)
        return
    for binding in bindings:
        try:
            user_id = int(binding.get('user_id') or 0)
            binding_id = int(binding.get('id') or 0)
        except (TypeError, ValueError):
            continue
        if user_id <= 0 or binding_id <= 0:
            continue
        if not db.is_lease_reminder_enabled(user_id):
            continue
        if db.has_reminder_sent(binding_id=binding_id, remind_type=remind_type, remind_date=remind_date):
            continue
        property_name = str(binding.get('property_name') or '-')
        rent_raw = binding.get('monthly_rent')
        try:
            rent_value = float(rent_raw or 0)
        except (TypeError, ValueError):
            rent_value = 0
        rent_line = f'${int(rent_value)}/月' if rent_value > 0 else '待确认'
        name = str(binding.get('first_name') or '您好')
        text = f'💰 <b>交租提醒</b>\n\n{he(name)}，您在【{he(property_name)}】的租金将于 <b>{he(remind_date)}（{rent_day} 号）</b> 到期。\n\n💵 月租：{he(rent_line)}\n\n请提前安排转账，如有疑问可联系顾问。'
        keyboard = InlineKeyboardMarkup([[InlineKeyboardButton('💬 联系我们', callback_data='appointment_menu:contact')]])
        try:
            await context.bot.send_message(chat_id=user_id, text=text, parse_mode=ParseMode.HTML, reply_markup=keyboard)
            db.log_reminder_sent(binding_id=binding_id, user_id=user_id, lease_end_date=_binding_end_date(binding) or '', remind_for_date=remind_date, remind_type=remind_type, sent_at=now_ts())
            await _notify_admins(context, title='交租提醒已发送', lines=[f'用户ID：{he(str(user_id))}', f'房号：{he(property_name)}', f'交租日：{rent_day} 号（{he(remind_date)}）', f'月租：{he(rent_line)}'])
        except Exception:
            logger.exception('发送交租提醒失败: user_id=%s binding_id=%s remind_date=%s', user_id, binding_id, remind_date)
