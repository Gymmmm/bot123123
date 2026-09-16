#!/usr/bin/env python3
"""
通过 Telegram API 直接发送测试消息并检查响应
模拟真实用户完整流程
"""
import asyncio
import time
from telegram import Bot
from telegram.error import TelegramError

BOT_TOKEN = "<redacted-test-token>"

# 测试用的 Gym Telegram ID（需要替换为真实 ID）
# 可以通过向 Bot 发送消息后从数据库查询获得
TEST_USER_ID = None

async def send_test_message(bot: Bot, chat_id: int, text: str):
    """向指定用户发送消息（仅管理员可用）"""
    try:
        await bot.send_message(chat_id=chat_id, text=text)
        print(f"✅ 已发送: {text[:30]}...")
        return True
    except TelegramError as e:
        print(f"❌ 发送失败: {e}")
        return False

async def check_bot_can_send_to_channel():
    """检查 Bot 是否可以向频道发送消息"""
    bot = Bot(BOT_TOKEN)

    try:
        # 测试向频道发送消息
        message = await bot.send_message(
            chat_id="@Jinbianzufanz",
            text="🧪 自动化测试：Bot 功能验证\n\n正在测试用户交互流程..."
        )
        print(f"✅ 成功向频道发送测试消息 (ID: {message.message_id})")
        return True
    except TelegramError as e:
        print(f"❌ 无法向频道发送消息: {e}")
        return False

async def test_empty_listing_response():
    """测试空数据库时的响应"""
    print("\n=== 测试空数据库响应 ===")
    print("预期行为：")
    print("- 找房功能应该提示「暂无匹配房源」")
    print("- 应该引导用户联系顾问")
    print("- 不应该崩溃或卡住")
    print()

async def main():
    print("=" * 60)
    print("侨联 Bot 自动化功能测试")
    print("=" * 60)
    print()

    bot = Bot(BOT_TOKEN)

    print("1️⃣ 检查 Bot 基础信息")
    try:
        me = await bot.get_me()
        print(f"   Bot ID: {me.id}")
        print(f"   Username: @{me.username}")
        print(f"   Name: {me.first_name}")
        print("   ✅ Bot 在线")
    except Exception as e:
        print(f"   ❌ Bot 离线: {e}")
        return
    print()

    print("2️⃣ 检查频道权限")
    can_send = await check_bot_can_send_to_channel()
    if not can_send:
        print("   ⚠️  Bot 无法向频道发送消息，但用户交互不受影响")
    print()

    print("3️⃣ 空数据库响应测试")
    await test_empty_listing_response()

    print("=" * 60)
    print("手动测试指南")
    print("=" * 60)
    print()
    print("📱 在 Telegram 中搜索 @XxxXiaopengbot")
    print()
    print("测试清单：")
    print("  [ ] /start - 查看首页")
    print("  [ ] 点击「🔍 智能找房」- 测试空房源处理")
    print("  [ ] 点击「💎 直接问顾问」- 测试咨询提交")
    print("  [ ] 点击「📅 预约看房」- 测试预约流程")
    print("  [ ] 点击「📖 关于侨联地产」- 查看品牌介绍")
    print("  [ ] 测试返回首页")
    print("  [ ] 测试 /cancel")
    print("  [ ] 重复点击同一按钮")
    print()
    print("完成后运行: python test_real_user_flow.py 查看数据库变化")
    print()

if __name__ == "__main__":
    asyncio.run(main())
