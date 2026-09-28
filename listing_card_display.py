"""找房结果卡片式展示辅助函数"""

async def send_listing_card(
    bot,
    chat_id: int,
    listing: dict,
    index: int = 0,
    total: int = 1
) -> None:
    """发送单个房源卡片（带图片）"""
    listing_id = listing.get('listing_id', '')
    title = listing.get('title', '房源')
    area = listing.get('area', '金边')
    community = listing.get('community', '')
    price = listing.get('price', 0)
    layout = listing.get('layout', '')
    size_sqm = listing.get('size_sqm', '')
    available_date = listing.get('available_date', '')
    highlights = listing.get('highlights', '')
    media_file = listing.get('media_file_id', '')

    # 构建卡片文案
    caption_parts = []

    # 标题
    if community:
        caption_parts.append(f"🏠 <b>{he(area)} · {he(community)}</b>")
    else:
        caption_parts.append(f"🏠 <b>{he(title)}</b>")

    # 价格和户型
    price_line = f"💰 <b>${price}/月</b>"
    if layout:
        price_line += f"  🛏 {he(layout)}"
    if size_sqm:
        price_line += f" · {he(size_sqm)}㎡"
    caption_parts.append(price_line)

    # 入住日期
    if available_date:
        caption_parts.append(f"📅 可入住：{he(available_date)}")

    # 亮点
    if highlights:
        caption_parts.append("")
        caption_parts.append("✨ <b>亮点</b>")
        for line in highlights.strip().split('\n')[:3]:
            if line.strip():
                caption_parts.append(f"• {he(line.strip())}")

    caption = "\n".join(caption_parts)

    # 按钮
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📋 查看详情", callback_data=f"listing:open:{listing_id}"),
            InlineKeyboardButton("📅 预约看房", callback_data=f"listing:appoint:{listing_id}")
        ]
    ])

    # 发送图片或纯文本
    if media_file and os.path.exists(media_file):
        try:
            with open(media_file, 'rb') as photo:
                await bot.send_photo(
                    chat_id=chat_id,
                    photo=photo,
                    caption=caption,
                    parse_mode=ParseMode.HTML,
                    reply_markup=keyboard
                )
        except Exception as e:
            logger.error(f"发送图片失败: {e}，降级为文本")
            await bot.send_message(
                chat_id=chat_id,
                text=caption,
                parse_mode=ParseMode.HTML,
                reply_markup=keyboard
            )
    else:
        await bot.send_message(
            chat_id=chat_id,
            text=caption,
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard
        )


async def send_find_results_as_cards(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    matches: list[dict],
    match_mode: str = "strict"
) -> None:
    """以卡片形式发送找房结果"""
    chat_id = update.effective_chat.id
    bot = context.bot

    # 先发送结果总览
    count = len(matches)
    if count == 0:
        await update.effective_message.reply_text(
            "暂时没有完全符合条件的房源。\n\n"
            "你可以调整预算或区域，也可以直接告诉顾问你的要求。",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("💬 联系顾问", callback_data="keyword:handoff")],
                [InlineKeyboardButton("✏️ 修改条件", callback_data="home_smart_search")],
                [InlineKeyboardButton("🏠 返回首页", callback_data="home")]
            ]),
            parse_mode=ParseMode.HTML
        )
        return

    # 结果说明
    if match_mode == "strict":
        intro = f"为你找到 <b>{count}</b> 套比较合适的房源："
    elif match_mode in {"no_type", "no_area", "budget_only"}:
        intro = f"已放宽条件，为你找到 <b>{count}</b> 套接近的房源："
    else:
        intro = f"为你找到 <b>{count}</b> 套房源："

    await update.effective_message.reply_text(intro, parse_mode=ParseMode.HTML)

    # 发送前3套房源卡片
    display_count = min(3, count)
    for i, listing in enumerate(matches[:display_count]):
        await send_listing_card(bot, chat_id, listing, i + 1, display_count)
        await asyncio.sleep(0.5)  # 避免发送过快

    # 如果有更多结果
    if count > 3:
        more_keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton(f"查看剩余 {count - 3} 套", callback_data="find:show_more")],
            [InlineKeyboardButton("💬 让顾问帮我挑", callback_data="keyword:handoff")],
            [InlineKeyboardButton("🏠 返回首页", callback_data="home")]
        ])
        await bot.send_message(
            chat_id=chat_id,
            text=f"还有 {count - 3} 套房源未展示。",
            reply_markup=more_keyboard
        )
    else:
        final_keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("✏️ 修改条件", callback_data="home_smart_search")],
            [InlineKeyboardButton("💬 联系顾问", callback_data="keyword:handoff")],
            [InlineKeyboardButton("🏠 返回首页", callback_data="home")]
        ])
        await bot.send_message(
            chat_id=chat_id,
            text="以上是符合条件的房源。",
            reply_markup=final_keyboard
        )
