# 侨联地产 v2.0 系统验收清单（实装版）

> ## 运行收口规则（2026-04）
> - 本机唯一主仓：`/Users/a1/projects/qiaolian_dual_bots_local`
> - 本机唯一启动入口：`python run_integrated_stack.py`
> - 可选参数：
>   - `--with-publisher`（同时启动发布 Bot）
>   - `--with-collector`（同时启动采集器）
> - 禁止从其它同类目录并行启动（如 `qiaolian_dual_autopilot`），避免 Token 抢占、DB 重复处理、状态错乱。
> - 在开始验收前，先执行：`./stop_all_qiaolian_bots.sh`
>
> 推荐顺序：
> 1. `./stop_all_qiaolian_bots.sh`
> 2. `./start_main_stack.sh --with-publisher`
> 3. 再执行本验收清单

**版本**: v2.0  
**验收日期**: _________  
**验收人**: _________  
**服务器**: `132.243.218.75`  
**项目路径**: `/opt/qiaolian_dual_bots`

---

## 1. 验收前准备

### 1.1 服务状态

```bash
ssh root@132.243.218.75
cd /opt/qiaolian_dual_bots
systemctl status qiaolian-user-bot.service --no-pager
systemctl status qiaolian-publisher-bot.service --no-pager
systemctl status qiaolian-collector.service --no-pager
systemctl status qiaolian-admin-web.service --no-pager
```

检查项：
- [ ] 4 个服务均为 `active (running)`
- [ ] 无频繁重启
- [ ] 最近 15 分钟无 `failed`

### 1.2 数据库备份

```bash
cd /opt/qiaolian_dual_bots
mkdir -p data/backups
cp data/qiaolian_dual_bot.db data/backups/qiaolian_$(date +%Y%m%d_%H%M%S).db
ls -lh data/backups | tail -n 3
```

检查项：
- [ ] 已完成备份
- [ ] 备份文件大小正常

### 1.3 结构核验（以线上当前实装为准）

```bash
sqlite3 data/qiaolian_dual_bot.db << 'EOF_SQL'
SELECT name FROM sqlite_master
WHERE type='table'
AND name IN ('renewal_tracking','lease_reminder_logs','publish_analytics','system_config')
ORDER BY name;

SELECT name FROM sqlite_master
WHERE type='view'
AND name IN ('ab_test_performance','renewal_conversion')
ORDER BY name;

PRAGMA table_info(tenant_bindings);
PRAGMA table_info(leads);
.quit
EOF_SQL
```

注意：当前线上 `tenant_bindings` 仍保留老字段风格，重点看：
- `lease_end_date`（老）/`contract_end_date`（新）至少其一可用
- `property_name`（老）/`listing_id`（新）至少其一可用

检查项：
- [ ] 4 个新表存在
- [ ] 2 个视图存在
- [ ] `tenant_bindings` 可用于到期日判断
- [ ] `leads` 可记录续租线索

---

## 2. 验收1：老客户续租全流程

### 2.1 准备测试用户

```bash
sqlite3 data/qiaolian_dual_bot.db << 'EOF_SQL'
SELECT user_id, username, first_name, last_active_at
FROM users
ORDER BY last_active_at DESC
LIMIT 20;
.quit
EOF_SQL
```

记录：`TEST_USER_ID=__________`

### 2.2 创建测试租约（7天后到期）

> 按当前线上字段写入：`property_name + lease_end_date + created_at`

```bash
sqlite3 data/qiaolian_dual_bot.db << 'EOF_SQL'
INSERT INTO tenant_bindings (
  user_id, binding_code, property_name, lease_end_date, rent_day, status, created_at
) VALUES (
  TEST_USER_ID,
  'TEST_BIND_001',
  'TEST_RENEWAL_001',
  date('now','+7 days'),
  1,
  'active',
  datetime('now','-358 days')
);

SELECT id, user_id, property_name, lease_end_date,
       CAST(julianday(date(lease_end_date)) - julianday(date('now')) AS INTEGER) AS days_left
FROM tenant_bindings
WHERE user_id=TEST_USER_ID AND status='active'
ORDER BY id DESC LIMIT 1;
.quit
EOF_SQL
```

检查项：
- [ ] 测试租约创建成功
- [ ] `days_left = 7`
- [ ] 记录 `binding_id=__________`

### 2.3 Telegram 交互验证

操作：给 `@qiaolian_user_bot` 发任意消息。

检查项：
- [ ] 识别为老客户并展示租约卡片
- [ ] 出现 `我的租约 / 续租咨询 / 我想换房` 等按钮
- [ ] 剩余天数显示正确（约 7 天）

### 2.4 续租确认验证

操作：点击 `续租咨询` → `确认续租`。

数据库核验：

```bash
sqlite3 data/qiaolian_dual_bot.db << 'EOF_SQL'
SELECT id,binding_id,user_id,listing_id,renewal_status,user_response,created_at
FROM renewal_tracking
WHERE user_id=TEST_USER_ID
ORDER BY id DESC LIMIT 3;

SELECT id,user_id,listing_id,action,source,created_at
FROM leads
WHERE user_id=TEST_USER_ID
ORDER BY id DESC LIMIT 5;
.quit
EOF_SQL
```

检查项：
- [ ] `renewal_tracking` 新增记录
- [ ] `renewal_status='pending'`
- [ ] `leads` 新增续租动作记录
- [ ] 管理员端收到续租通知

### 2.5 到期提醒验证（3 天）

```bash
sqlite3 data/qiaolian_dual_bot.db << 'EOF_SQL'
INSERT INTO tenant_bindings (
  user_id, binding_code, property_name, lease_end_date, rent_day, status, created_at
) VALUES (
  TEST_USER_ID,
  'TEST_BIND_003',
  'TEST_3DAY_REMINDER',
  date('now','+3 days'),
  1,
  'active',
  datetime('now','-362 days')
);

SELECT id,user_id,property_name,lease_end_date
FROM tenant_bindings
WHERE user_id=TEST_USER_ID AND status='active'
ORDER BY id DESC LIMIT 3;
.quit
EOF_SQL
```

检查提醒日志：

```bash
sqlite3 data/qiaolian_dual_bot.db << 'EOF_SQL'
SELECT id,binding_id,user_id,remind_type,COALESCE(remind_date,remind_for_date) AS remind_date,sent_at
FROM lease_reminder_logs
WHERE user_id=TEST_USER_ID
ORDER BY id DESC LIMIT 10;
.quit
EOF_SQL
```

检查项：
- [ ] 收到 3 天提醒
- [ ] `lease_reminder_logs` 记录写入
- [ ] 同一天不会重复发送同类型提醒

---

## 3. 验收2：A/B测试发布与数据追踪

### 3.1 权重配置核验

```bash
sqlite3 data/qiaolian_dual_bot.db "SELECT key,value,description FROM system_config WHERE key='caption_variant_weights';"
```

检查项：
- [ ] 配置存在
- [ ] 权重为 `a/b/c`

### 3.2 发布后写入核验

```bash
sqlite3 data/qiaolian_dual_bot.db << 'EOF_SQL'
SELECT id,listing_id,area,monthly_rent,caption_variant,publish_hour,published_at
FROM publish_analytics
ORDER BY id DESC LIMIT 20;
.quit
EOF_SQL
```

检查项：
- [ ] 每次发布有 `publish_analytics` 记录
- [ ] `caption_variant` 为 `a|b|c`

### 3.3 分布核验（样本>=10）

```bash
sqlite3 data/qiaolian_dual_bot.db << 'EOF_SQL'
SELECT caption_variant, COUNT(*) AS cnt,
       ROUND(COUNT(*)*100.0/SUM(COUNT(*)) OVER(),1) AS pct
FROM publish_analytics
WHERE published_at >= datetime('now','-7 days')
GROUP BY caption_variant
ORDER BY caption_variant;
.quit
EOF_SQL
```

检查项：
- [ ] `a/b/c` 三种均出现
- [ ] 大致接近 4:3:3（样本增大后更稳定）

### 3.4 视图核验

```bash
sqlite3 data/qiaolian_dual_bot.db "SELECT * FROM ab_test_performance;"
sqlite3 data/qiaolian_dual_bot.db "SELECT * FROM renewal_conversion;"
```

检查项：
- [ ] 视图可查询
- [ ] 字段结构正确

---

## 4. 验收3：分析系统 + autopilot 命令

### 4.1 脚本单独运行

```bash
cd /opt/qiaolian_dual_bots
./.venv/bin/python analytics/channel_analytics_integrated.py 7
./.venv/bin/python analytics/channel_analytics_integrated.py --json | head -n 60
```

检查项：
- [ ] 文本报表输出正常
- [ ] JSON 输出正常

### 4.2 `/analytics` 命令

在管理员 Telegram 对话中执行：
- `/analytics`
- `/analytics 30`

检查项：
- [ ] 命令响应正常
- [ ] 天数参数生效
- [ ] 输出无异常堆栈

---

## 5. 验收4：采集工具

### 5.1 帮助与模板

```bash
cd /opt/qiaolian_dual_bots
./.venv/bin/python tools/property_intake.py --help
cat tools/property_intake_template.csv
```

### 5.2 CSV 导入（建议先用临时库）

```bash
cd /opt/qiaolian_dual_bots
cp data/qiaolian_dual_bot.db /tmp/qiaolian_intake_verify.db
./.venv/bin/python tools/property_intake.py --db /tmp/qiaolian_intake_verify.db --csv tools/property_intake_template.csv
sqlite3 /tmp/qiaolian_intake_verify.db "SELECT id,source_name,source_post_id,parse_status FROM source_posts ORDER BY id DESC LIMIT 5;"
rm -f /tmp/qiaolian_intake_verify.db
```

检查项：
- [ ] 导入成功
- [ ] 新增记录 `parse_status='pending'`
- [ ] 可进入现有 `ai_parser` 链路

---

## 6. 稳定性与健康检查

### 6.1 日志

```bash
journalctl -u qiaolian-user-bot.service -n 120 --no-pager
journalctl -u qiaolian-publisher-bot.service -n 120 --no-pager
journalctl -u qiaolian-user-bot.service -p err -n 80 --no-pager
journalctl -u qiaolian-publisher-bot.service -p err -n 80 --no-pager
```

### 6.2 数据库完整性

```bash
sqlite3 data/qiaolian_dual_bot.db "PRAGMA integrity_check;"
```

检查项：
- [ ] `integrity_check` 返回 `ok`
- [ ] 无连续异常

---

## 7. 验收后清理

```bash
sqlite3 data/qiaolian_dual_bot.db << 'EOF_SQL'
DELETE FROM renewal_tracking WHERE listing_id LIKE 'TEST_%';
DELETE FROM leads WHERE listing_id LIKE 'TEST_%';
DELETE FROM tenant_bindings WHERE property_name LIKE 'TEST_%' OR binding_code LIKE 'TEST_BIND_%';
DELETE FROM lease_reminder_logs
WHERE binding_id NOT IN (SELECT id FROM tenant_bindings);

SELECT COUNT(*) AS remain_bindings FROM tenant_bindings
WHERE property_name LIKE 'TEST_%' OR binding_code LIKE 'TEST_BIND_%';
.quit
EOF_SQL
```

检查项：
- [ ] 测试数据已清理
- [ ] 无残留测试脏数据

---

## 8. 验收报告

| 项目 | 结果 | 备注 |
|---|---|---|
| 老客户识别 | ☐通过 ☐失败 | |
| 续租流程 | ☐通过 ☐失败 | |
| 提醒任务 | ☐通过 ☐失败 | |
| A/B变体分发 | ☐通过 ☐失败 | |
| publish_analytics记录 | ☐通过 ☐失败 | |
| `/analytics` 报表 | ☐通过 ☐失败 | |
| 采集工具 CSV 导入 | ☐通过 ☐失败 | |
| 服务稳定性 | ☐通过 ☐失败 | |
| 数据库完整性 | ☐通过 ☐失败 | |

**通过项**: ___ / 9  
**结论**: ☐ 可上线  ☐ 需修复后复验

---

## 9. 快速命令

```bash
# 最近发布
sqlite3 data/qiaolian_dual_bot.db "SELECT listing_id,caption_variant,datetime(published_at,'localtime') FROM publish_analytics ORDER BY id DESC LIMIT 10;"

# A/B分布
sqlite3 data/qiaolian_dual_bot.db "SELECT caption_variant,COUNT(*) FROM publish_analytics GROUP BY caption_variant;"

# 最近续租
sqlite3 data/qiaolian_dual_bot.db "SELECT id,binding_id,user_id,listing_id,renewal_status,created_at FROM renewal_tracking ORDER BY id DESC LIMIT 10;"

# 提醒日志
sqlite3 data/qiaolian_dual_bot.db "SELECT id,binding_id,user_id,remind_type,sent_at FROM lease_reminder_logs ORDER BY id DESC LIMIT 10;"
```

**最后更新**: 2026-04-20
