# 侨联地产系统整合总方案（可执行修正版）

版本: v2.0 (Execution Ready)
更新: 2026-04-20
目标: 采集 -> 发布 -> 用户承接 -> 留存续租 -> 数据分析 全链路打通

---

## 1. 当前代码基线（已核对）

- 用户机器人: `qiaolian_dual/user_bot.py`
- 发布引擎: `meihua_publisher.py`
- 运营/调度机器人: `autopilot_publish_bot.py`
- 封面生成: `cover_generator.py`
- CSV 采集: `tools/collect_houses_csv.py`
- CSV 发布: `tools/publish_houses_csv.py`
- 主数据库: `data/qiaolian_dual_bot.db`
- 建表入口: `scripts/bootstrap_db.py`

已存在核心表:
- `source_posts`, `drafts`, `media_assets`, `posts`, `publish_logs`
- `users`, `leads`, `appointments`, `tenant_bindings`, `repair_tickets`, `subscriptions`

---

## 2. 与原始草案的关键修正

1. 数据库名不是 `data/qiaolian.db`，当前实际是 `data/qiaolian_dual_bot.db`。
2. `source_posts` 没有 `message_text/notes` 字段，采集工具必须写 `raw_text/raw_images_json`。
3. 发布模块文件在项目根目录，不在 `qiaolian_dual/meihua_publisher.py`。
4. 当前用户侧线索 `action` 实际使用 `consult_click/appointment_submit/...`，报表口径需兼容。
5. 迁移脚本需要可重复执行（幂等），不能直接多次 `ALTER TABLE`。

---

## 3. 分阶段实施（建议顺序）

### 阶段 A: 数据库升级（先做）

新增能力:
- 合同/续租追踪
- 提醒日志
- 发布分析
- 系统配置

执行方式:
```bash
cd /Users/a1/projects/qiaolian_dual_bots_local
python3 scripts/migrate_v2_retention.py --db data/qiaolian_dual_bot.db
```

回滚建议:
```bash
cp data/qiaolian_dual_bot.db data/qiaolian_dual_bot.db.bak_$(date +%Y%m%d_%H%M%S)
```

### 阶段 B: User Bot 增强

目标:
- 老用户看到“租约/续租/换房”入口
- 到期提醒从单点改为 30/7/3 天
- 续租确认写 `renewal_tracking` 并同步管理员

落点文件:
- `qiaolian_dual/user_bot.py`
- `qiaolian_dual/db.py`

### 阶段 C: 发布引擎 A/B 变体 + Analytics

目标:
- 发布文案保持现有风格但支持 A/B/C 变体
- 每次发布落库到 `publish_analytics`

落点文件:
- `meihua_publisher.py`

### 阶段 D: 运营分析命令

目标:
- 增加 `/analytics` 命令
- 汇总发布、咨询、预约、续租转换

落点文件:
- `autopilot_publish_bot.py`
- `analytics/channel_analytics_integrated.py`（新增）

### 阶段 E: 采集入口统一

目标:
- 新增交互录入工具（写入 `source_posts.raw_text/raw_images_json`）
- 与 `ai_parser.py` 现有解析链对齐

落点文件:
- `tools/property_intake.py`（新增）

---

## 4. 验收标准（上线前）

### 功能验收
- 老用户进入首页可看到租约信息
- 续租按钮可形成 `renewal_tracking` 记录
- 到期提醒 30/7/3 天仅发送一次（有日志去重）
- 发布后 `publish_analytics` 有记录
- `/analytics` 返回可读报表

### 数据验收
- `tenant_bindings` 新字段已存在并可写
- `leads` 新字段已存在并兼容旧数据
- 2 个视图可查询: `ab_test_performance`, `renewal_conversion`

### 稳定性验收
- `./scripts/smoke_local.sh` 通过
- `./scripts/smoke_server.sh --host <ip> --user root` 通过
- 四服务 `active`: user/publisher/collector/admin-web

---

## 5. 推荐上线顺序（低风险）

1. 只跑迁移脚本，不改业务逻辑
2. 先上 analytics 落库（只写不读）
3. 再开启 `/analytics` 读取
4. 最后切换用户端“续租入口”

这样即使中途暂停，也不会影响现有发布链。

---

## 6. 日常运营动作（精简版）

每日:
- 看 `/analytics 7`
- 看 `/pending` + `/ops`
- 跟进 30 天内到期合同

每周:
- 调整 caption 变体权重（A/B/C）
- 复盘热门区域和价格段

每月:
- 归档旧 analytics（>90 天）
- 检查续租转化率

---

## 7. 下一步建议（直接执行）

- 现在先执行阶段 A（迁移）+ 验证。
- 验证通过后再进入阶段 B/C（代码改造）。

