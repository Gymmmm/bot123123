#!/usr/bin/env python3
"""
真实用户流程自动化测试
通过直接查询数据库和日志来验证 Bot 响应
"""
import asyncio
import sqlite3
import time
from datetime import datetime

DB_PATH = "data/qiaolian_dual_bot.db"

def check_db_state():
    """检查数据库当前状态"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    results = {
        "users": 0,
        "listings": 0,
        "favorites": 0,
        "appointments": 0,
        "leads": 0,
    }

    for table in results.keys():
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            results[table] = cursor.fetchone()[0]
        except:
            results[table] = "ERROR"

    conn.close()
    return results

def get_recent_users():
    """获取最近注册的用户"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        cursor.execute("""
            SELECT user_id, username, first_name, created_at
            FROM users
            ORDER BY created_at DESC
            LIMIT 5
        """)
        users = cursor.fetchall()
    except Exception as e:
        users = []
        print(f"查询用户失败: {e}")

    conn.close()
    return users

def get_recent_leads():
    """获取最近的咨询记录"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        cursor.execute("""
            SELECT id, user_id, lead_type, contact_info, message, created_at
            FROM leads
            ORDER BY created_at DESC
            LIMIT 5
        """)
        leads = cursor.fetchall()
    except Exception as e:
        leads = []
        print(f"查询咨询失败: {e}")

    conn.close()
    return leads

def get_recent_appointments():
    """获取最近的预约记录"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        cursor.execute("""
            SELECT id, user_id, listing_id, mode, preferred_date, preferred_time, status, created_at
            FROM appointments
            ORDER BY created_at DESC
            LIMIT 5
        """)
        appointments = cursor.fetchall()
    except Exception as e:
        appointments = []
        print(f"查询预约失败: {e}")

    conn.close()
    return appointments

def main():
    print("=" * 60)
    print("侨联 Bot 真实用户流程验证")
    print("=" * 60)
    print()

    print("📊 当前数据库状态：")
    state = check_db_state()
    for table, count in state.items():
        print(f"  {table}: {count}")
    print()

    print("=" * 60)
    print("🧪 测试步骤（需要手动在 Telegram 中操作）")
    print("=" * 60)
    print()

    print("1️⃣ 测试 /start 首页")
    print("   - 在 Telegram 搜索 @XxxXiaopengbot")
    print("   - 发送 /start")
    print("   - 检查是否收到首页欢迎消息和按钮")
    print()

    print("2️⃣ 测试智能找房")
    print("   - 点击【🔍 智能找房】")
    print("   - 选择房型（住宅/别墅/商铺）")
    print("   - 选择区域")
    print("   - 选择预算")
    print("   - 查看是否正确处理空数据库（应提示暂无匹配）")
    print()

    print("3️⃣ 测试直接问顾问")
    print("   - 返回首页，点击【💎 直接问顾问】")
    print("   - 输入咨询内容（例如：我想在BKK1找一房）")
    print("   - 检查是否要求提供联系方式")
    print("   - 输入微信号或手机号")
    print("   - 查看是否成功提交")
    print()

    print("4️⃣ 测试预约看房")
    print("   - 返回首页，点击【📅 预约看房】")
    print("   - 选择方式（实地/视频）")
    print("   - 选择关注点")
    print("   - 选择日期")
    print("   - 选择时间")
    print("   - 提供联系方式")
    print("   - 查看是否成功提交")
    print()

    print("5️⃣ 测试返回/取消/重复点击")
    print("   - 在任意流程中点击【🏠 返回首页】")
    print("   - 在任意流程中点击【⬅️ 返回上一步】")
    print("   - 重复点击同一按钮")
    print("   - 在输入阶段发送 /cancel")
    print()

    print("=" * 60)
    print("⏰ 等待 30 秒后检查数据库变化...")
    print("   请在这 30 秒内完成上述测试")
    print("=" * 60)
    print()

    time.sleep(30)

    print("\n" + "=" * 60)
    print("📈 测试后数据库状态")
    print("=" * 60)
    print()

    new_state = check_db_state()
    print("📊 记录数量：")
    for table, count in new_state.items():
        old_count = state.get(table, 0)
        if old_count != count:
            print(f"  {table}: {old_count} → {count} (+{count - old_count if isinstance(count, int) and isinstance(old_count, int) else 'N/A'})")
        else:
            print(f"  {table}: {count}")
    print()

    print("👥 最近用户：")
    users = get_recent_users()
    if users:
        for user in users:
            print(f"  - {user[0]} @{user[1]} ({user[2]}) - {user[3]}")
    else:
        print("  无新增用户")
    print()

    print("💬 最近咨询：")
    leads = get_recent_leads()
    if leads:
        for lead in leads:
            print(f"  - ID:{lead[0]} User:{lead[1]} Type:{lead[2]} Contact:{lead[3]} - {lead[5]}")
            print(f"    消息: {lead[4][:50]}...")
    else:
        print("  无新增咨询")
    print()

    print("📅 最近预约：")
    appointments = get_recent_appointments()
    if appointments:
        for appt in appointments:
            print(f"  - ID:{appt[0]} User:{appt[1]} Listing:{appt[2]} Mode:{appt[3]}")
            print(f"    日期:{appt[4]} 时间:{appt[5]} 状态:{appt[6]} - {appt[7]}")
    else:
        print("  无新增预约")
    print()

    print("=" * 60)
    print("✅ 验证完成")
    print("=" * 60)

if __name__ == "__main__":
    main()
