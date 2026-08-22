"""从 user_bot.py 拆分出的职责模块。"""
from __future__ import annotations

from .common import *

async def cmd_deal_done(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    from .admin_contract import _is_admin_user
    from .session_deeplink import _deep_link, now_ts
    user = update.effective_user
    if not _is_admin_user(getattr(user, 'id', 0)):
        await update.effective_message.reply_text('❌ 无权限。该命令仅限管理员使用。')
        return MAIN
    args = list(context.args or [])
    if len(args) < 2:
        await update.effective_message.reply_text('用法：\n/deal_done <user_id> <binding_code> [property_name] [lease_end_date]\n\n示例：\n/deal_done 123456789 BIND20260421 QL-001 2026-12-31')
        return MAIN
    try:
        target_user_id = int(args[0])
    except (TypeError, ValueError):
        await update.effective_message.reply_text('❌ user_id 无效，请传数字。')
        return MAIN
    binding_code = str(args[1]).strip()
    if not binding_code:
        await update.effective_message.reply_text('❌ binding_code 不能为空。')
        return MAIN
    remaining = [str(x).strip() for x in args[2:] if str(x).strip()]
    lease_end_date = ''
    if remaining and re.fullmatch('\\d{4}-\\d{2}-\\d{2}', remaining[-1]):
        lease_end_date = remaining.pop()
    property_name = ' '.join(remaining).strip() or '待补充'
    try:
        binding_id = db.create_binding(user_id=target_user_id, binding_code=binding_code, property_name=property_name, lease_end_date=lease_end_date, rent_day=None, created_at=now_ts(), status='pending')
    except sqlite3.IntegrityError:
        await update.effective_message.reply_text(f'❌ 绑定码 `{binding_code}` 已存在，请换一个。', parse_mode=ParseMode.MARKDOWN)
        return MAIN
    except Exception:
        logger.exception('创建 tenant binding 失败')
        await update.effective_message.reply_text('❌ 创建绑定失败，请稍后重试。')
        return MAIN
    deep_link = _deep_link(f't_bind_{binding_code}')
    bind_kb = InlineKeyboardMarkup([[InlineKeyboardButton('🔗 绑定租后管家', url=deep_link)]])
    push_ok = True
    try:
        await context.bot.send_message(chat_id=target_user_id, text='🎉 恭喜入住！\n\n入住后如需报修、续租或物业沟通，\n请点击下方按钮完成租后管家绑定（约 30 秒）。', reply_markup=bind_kb)
    except Exception:
        push_ok = False
        logger.exception('向用户推送 t_bind 链接失败: user_id=%s', target_user_id)
    try:
        db.create_lead({'user_id': target_user_id, 'username': '', 'display_name': '', 'source': 'admin_cmd', 'action': 'deal_done', 'listing_id': property_name, 'area': '', 'property_type': '', 'budget_min': None, 'budget_max': None, 'payload': {'binding_id': binding_id, 'binding_code': binding_code, 'lease_end_date': lease_end_date, 'created_by_admin_id': getattr(user, 'id', 0), 'push_ok': push_ok}, 'message_id': None, 'post_token': '', 'caption_variant': '', 'created_at': now_ts()})
    except Exception:
        logger.exception('写入 deal_done leads 失败: user_id=%s', target_user_id)
    await update.effective_message.reply_text(f"✅ 已创建成交绑定任务\n- user_id: `{target_user_id}`\n- binding_code: `{binding_code}`\n- property: `{property_name}`\n- lease_end: `{lease_end_date or '-'}\n- push: `{('ok' if push_ok else 'failed')}`\n\n用户绑定入口：{deep_link}", parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True)
    return MAIN

async def cmd_lead_response(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    from .admin_contract import _is_admin_user
    from .session_deeplink import now_ts
    user = update.effective_user
    if not _is_admin_user(getattr(user, 'id', 0)):
        await update.effective_message.reply_text('❌ 无权限。该命令仅限管理员使用。')
        return MAIN
    args = list(context.args or [])
    if len(args) < 2:
        await update.effective_message.reply_text('用法：\n/lead_response <lead_id> <agent_id> [response_at]\n\n示例：\n/lead_response 123 agent_zhang 2026-04-22 15:30:00')
        return MAIN
    try:
        lead_id = int(args[0])
    except (TypeError, ValueError):
        await update.effective_message.reply_text('❌ lead_id 无效，请传数字。')
        return MAIN
    agent_id = str(args[1]).strip()
    if not agent_id:
        await update.effective_message.reply_text('❌ agent_id 不能为空。')
        return MAIN
    response_at = ' '.join(args[2:]).strip() or now_ts()
    ok = db.mark_lead_responded(lead_id, agent_id=agent_id, response_at=response_at)
    if not ok:
        await update.effective_message.reply_text(f'⚠️ 未找到 lead_id={lead_id}，未更新。')
        return MAIN
    await update.effective_message.reply_text(f'✅ 已记录响应：lead_id={lead_id}, agent_id={agent_id}, response_at={response_at}')
    return MAIN

async def cmd_push_local(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    from .admin_contract import _is_admin_user
    user = update.effective_user
    if not _is_admin_user(getattr(user, 'id', 0)):
        await update.effective_message.reply_text('❌ 无权限。该命令仅限管理员使用。')
        return MAIN
    args = list(context.args or [])
    if len(args) < 2:
        await update.effective_message.reply_text('用法：\n/push_local <小区关键词> <消息内容>\n\n示例：\n/push_local 富力城 【楼下新开】重庆火锅，营业时间 10:00-22:00，欢迎光临！')
        return MAIN
    keyword = args[0].strip()
    message_body = ' '.join(args[1:]).strip()
    if not keyword or not message_body:
        await update.effective_message.reply_text('❌ 小区关键词和消息内容不能为空。')
        return MAIN
    try:
        bindings = db.list_active_bindings_by_property(keyword)
    except Exception:
        logger.exception('查询小区租客失败: keyword=%s', keyword)
        await update.effective_message.reply_text('❌ 查询租客失败，请检查日志。')
        return MAIN
    if not bindings:
        await update.effective_message.reply_text(f'⚠️ 未找到小区「{keyword}」的活跃租客，未发送任何消息。')
        return MAIN
    sent = 0
    failed = 0
    for binding in bindings:
        try:
            uid = int(binding.get('user_id') or 0)
        except (TypeError, ValueError):
            uid = 0
        if uid <= 0:
            failed += 1
            continue
        try:
            await context.bot.send_message(chat_id=uid, text=message_body, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
            sent += 1
        except Exception:
            logger.exception('定向推送失败: user_id=%s', uid)
            failed += 1
    await update.effective_message.reply_text(f'✅ 定向推送完成\n小区：{keyword}\n成功：{sent} 人 | 失败/跳过：{failed} 人')
    return MAIN

async def cmd_push_all(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    from .admin_contract import _is_admin_user
    user = update.effective_user
    if not _is_admin_user(getattr(user, 'id', 0)):
        await update.effective_message.reply_text('❌ 无权限。该命令仅限管理员使用。')
        return MAIN
    args = list(context.args or [])
    if not args:
        await update.effective_message.reply_text('用法：\n/push_all <消息内容>\n\n示例：\n/push_all 【侨联通知】本月起全面升级租后管家服务，敬请期待。')
        return MAIN
    message_body = ' '.join(args).strip()
    if not message_body:
        await update.effective_message.reply_text('❌ 消息内容不能为空。')
        return MAIN
    try:
        bindings = db.list_all_active_bindings()
    except Exception:
        logger.exception('查询全部活跃租客失败')
        await update.effective_message.reply_text('❌ 查询租客失败，请检查日志。')
        return MAIN
    if not bindings:
        await update.effective_message.reply_text('⚠️ 当前无活跃绑定租客，未发送任何消息。')
        return MAIN
    sent = 0
    failed = 0
    for binding in bindings:
        try:
            uid = int(binding.get('user_id') or 0)
        except (TypeError, ValueError):
            uid = 0
        if uid <= 0:
            failed += 1
            continue
        try:
            await context.bot.send_message(chat_id=uid, text=message_body, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
            sent += 1
        except Exception:
            logger.exception('全量推送失败: user_id=%s', uid)
            failed += 1
    await update.effective_message.reply_text(f'✅ 全量推送完成\n成功：{sent} 人 | 失败/跳过：{failed} 人')
    return MAIN
