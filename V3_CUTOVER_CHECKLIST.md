# V3 Production Cut-over Checklist

> 本文档用于 `Gymmmm/bot123123` V3 最终上线。任何一步失败都停止切换，不带病上线。
>
> 固定生产回滚基线：`8e4605cf5cc21dfec3ce30729654b09e39de9abf`
>
> 候选版本必须使用 release candidate 分支所指向的**精确 SHA**，不得使用漂移中的开发分支 HEAD、master 或服务器遗留目录作为上线依据。

## 0. 不可变规则

- 上线前不修改现有生产 systemd unit。
- 不在未备份数据库上执行初始化或迁移动作。
- V3 runtime 不允许自行创建/迁移 schema；唯一显式初始化入口是 `run_v3_preflight.py --initialize`。
- `CHANNEL_DISCUSSION_ENABLED` 必须保持关闭。
- 售房只入库：`offer_type=sale`、`publication_policy=store_only`，不得进入租房 Telegram Publisher。
- 新租房频道帖子只允许 `details / photos / book` 三个公共动作。
- 不允许 CSV 直接发布。
- 不允许在未确认 Telegram 外部结果时自动重发。
- 同一来源帖子内容变化必须复用原 listing/public ID/offer/review 身份并重新进入 review，不得生成第二套房源。
- 已发布的公共事实继续以 frozen publication package 为准；新事实未重新审批前不得穿透旧发布包。

## 1. 冻结候选版本

记录以下值：

```bash
REPO=/opt/qiaolian_v3
CANDIDATE_SHA=<release/v3-production-candidate-20260909 的精确 SHA>
ROLLBACK_SHA=8e4605cf5cc21dfec3ce30729654b09e39de9abf
```

核对：

```bash
cd "$REPO"
git fetch origin --prune
git checkout --detach "$CANDIDATE_SHA"
git rev-parse HEAD
```

`git rev-parse HEAD` 必须与 `CANDIDATE_SHA` 完全一致。

## 2. 记录当前生产状态

在任何停止/切换动作之前保存：

```bash
date -Is
hostname
systemctl status qiaolian-user-bot.service --no-pager
systemctl status qiaolian-publisher-bot.service --no-pager
systemctl status qiaolian-collector.service --no-pager
```

同时记录当前生产目录、当前代码 SHA、数据库实际路径和 `.env` 来源。不要根据历史文档猜数据库路径。

## 3. 数据库备份

确认实际 `DB_PATH` / `SQLITE_PATH` 后再备份。SQLite 推荐先停止所有会写该数据库的待切换进程，或使用 SQLite 在线备份命令；不得只复制主 `.db` 而忽略正在活动的 WAL 状态。

备份至少保留：

- 原数据库完整可恢复副本
- 当前 `.env` / 服务环境配置副本
- 当前生产 SHA：`8e4605cf5cc21dfec3ce30729654b09e39de9abf`
- 当前 systemd unit 内容及启用状态

备份验证必须至少确认文件存在、大小非零，并可由 SQLite 打开。

## 4. 安装候选依赖

在候选版本自己的 Python 环境中执行：

```bash
python -m pip install -r requirements.txt
python -m compileall -q \
  v3_core \
  run_v3_collector.py \
  run_v3_canonical_worker.py \
  run_v3_publisher_bot.py \
  run_v3_user_bot.py \
  run_v3_preflight.py
```

任何 import/compile 错误都停止上线。

## 5. 显式初始化 V3 additive schema

确认候选进程尚未对外运行，再执行：

```bash
python run_v3_preflight.py --initialize --component all
```

`--initialize` 只允许增加 V3 schema。执行后立即再跑一次**只读 preflight**：

```bash
python run_v3_preflight.py --component all
```

必须输出整体：

```json
{"ok": true}
```

或等价的 `ok: true` 结果。

只读 preflight 必须确认：

- database 可读
- 所有 REQUIRED_V3_TABLES 存在
- Collector 配置有效，至少有启用的数据源
- `TG_API_ID` 为有效数字
- Publisher token / admins / channel 配置存在
- User Bot token / username / admins / channel URL / 顾问联系方式存在
- 四套正式封面模板存在
- cover output 可写
- assurance PNG/PDF 资产存在
- `CHANNEL_DISCUSSION_ENABLED` 未开启
- V3 Collector / Canonical Worker / Publisher / User Bot 四个运行入口存在

任意一项不通过：停止，不启动 V3 Telegram 进程。

## 6. 上线前数据库安全核对

只读核查至少包括：

```sql
SELECT COUNT(*) FROM source_posts;
SELECT COUNT(*) FROM listings_v3;
SELECT COUNT(*) FROM listing_offers;
SELECT COUNT(*) FROM review_items;
SELECT COUNT(*) FROM publication_packages_v3;
SELECT COUNT(*) FROM publication_delivery_attempts_v3;
SELECT COUNT(*) FROM publication_instances;
```

并核对：

```sql
SELECT COUNT(*)
FROM listing_offers
WHERE offer_type='sale'
  AND publication_policy<>'store_only';
```

结果必须为 `0`。

再核对：

```sql
SELECT COUNT(*)
FROM listing_offers
WHERE offer_type='sale'
  AND publishable<>0;
```

结果必须为 `0`。

## 7. 切换前确认实际 V3 进程定义

本仓库当前只锁定 Python 入口：

- `run_v3_collector.py`
- `run_v3_canonical_worker.py`
- `run_v3_publisher_bot.py`
- `run_v3_user_bot.py`

**不要凭空假设 V3 systemd unit 名称。**

正式切换前，先检查服务器准备使用的 unit / supervisor / shell wrapper 的：

- `WorkingDirectory`
- `ExecStart`
- Python/venv 路径
- `.env` / EnvironmentFile
- DB_PATH / SQLITE_PATH
- restart policy
- logs
- user/group

所有路径必须指向冻结的 `CANDIDATE_SHA` 目录，而不是开发工作树。

## 8. 切换顺序

只有在以上步骤全部通过，并收到明确上线指令后执行。

推荐顺序：

1. 停止旧 Collector，阻止切换窗口继续写入旧链。
2. 停止旧 Publisher。
3. 停止旧 User Bot。
4. 再确认数据库备份完成。
5. 启动 V3 Canonical Worker。
6. 启动 V3 Publisher。
7. 启动 V3 User Bot。
8. 最后启动 V3 Collector。

不要让旧 Collector/Publisher 与 V3 Collector/Publisher 同时消费同一来源并向同一频道发布。

## 9. 切换后最小生产验收

### 9.1 进程

四个 V3 进程均应存活，无重启循环、ImportError、SQLite schema error 或 Telegram polling/webhook 冲突。

### 9.2 Collector → Canonical → Inventory

使用一条受控租房测试源确认：

```text
Telegram source
→ source_posts
→ canonical_records
→ listings_v3
→ listing_offers(rent)
→ review_items(pending)
```

在人工审批前不得发布。

### 9.3 售房隔离

使用一条受控 sale 测试源确认：

```text
sale
→ source_posts
→ canonical_records
→ listings_v3
→ listing_offers(sale, store_only, publishable=0)
→ STOP
```

频道不得出现售房帖子。

### 9.4 审批和冻结发布包

对一条受控 rent：

1. review approve
2. build package
3. package approve
4. publish

确认发布前 package 已冻结，审批后不得原地重建。

### 9.5 Telegram 频道契约

受控发布必须只有：

- 1 张最终封面
- 1 个 caption
- 1 个 inline keyboard
- `🏠 房源详情`
- `📸 更多实拍`
- `📅 预约看房`

不得出现 discussion photos 路径或第四个公共动作。

### 9.6 User Bot 深链

逐个实际点击验证：

- `details`
- `photos`
- `book`

要求：

- 只接受已正式发布的 V3 rent inventory
- 展示 `QL-*`，不暴露 `l_*`
- details 的事实来自 frozen package snapshot
- photos 来自 frozen gallery
- book 按实时房态判断可预约性

### 9.7 重复发布保护

同一已发布 package 再次执行发布准备时必须被拦截；不得生成第二条 Telegram 帖子。

如果 Telegram send 后结果未知，状态必须进入 `unknown`，不得自动二次发送。

### 9.8 同源更新

把受控测试源的租金从 A 改成 B，再让 Collector/Worker 处理。必须确认：

- `source_posts` 不新增第二条同源记录
- listing_id 不变
- public `QL-*` ID 不变
- offer_id 不变
- review_id 不变
- canonical revision 增加
- offer 变为 `publishable=0`
- block reason 回到 `review_required`
- review 回到 `pending`
- 新事实重新审批前，旧已发布详情仍显示旧 frozen snapshot

## 10. Daily Broadcast 验收

在 Publisher 后台验证：

- 广播中心可打开
- preview 不发送频道消息
- manual send 只发送一次
- live weather / FX 获取失败时有安全 fallback
- `find_home / latest / advisor` 三个按钮均能进入 V3 User Bot
- scheduled send 同一天只能 claim 一次
- Telegram 结果不明时当天不会自动二发

建议正式开启定时广播前先保持 `daily_broadcast_enabled=0`，手工预览和发送验收通过后再开启。

## 11. 观察窗口

切换后至少观察：

- V3 进程日志
- `review_items` 队列
- `publication_delivery_attempts_v3` 中 `unknown` / `failed_before_send`
- `publication_instances` 是否一 package 一外部帖子
- Telegram 频道是否有重复帖子
- User Bot details/photos/book/search/预约是否正常
- Collector 是否出现异常重复 ingest

未通过观察窗口前，固定生产 SHA仍作为正式 rollback target。

## 12. 回滚

任何下列情况立即回滚：

- V3 preflight 不通过
- V3 进程反复崩溃
- 重复频道发布
- sale 进入租房频道
- package / publication identity 异常
- User Bot 公共深链错误暴露未发布库存
- DB 写入异常或无法确认一致性

回滚原则：

1. 先停止所有 V3 写进程。
2. 保留 V3 日志和故障数据库副本用于追查。
3. 如数据库已产生不兼容/错误写入，恢复上线前备份；不要在未知状态数据库上直接混跑旧生产。
4. checkout 固定生产 SHA：

```bash
git checkout --detach 8e4605cf5cc21dfec3ce30729654b09e39de9abf
```

5. 按上线前记录恢复原生产服务定义、环境和启动顺序。
6. 验证原三项生产服务和 Telegram 行为恢复。

## 13. 最终 Go / No-Go

只有以下全部为 YES 才允许 GO：

- [ ] release candidate 精确 SHA 的 GitHub `v3-core` CI 为 success
- [ ] 目标服务器 `run_v3_preflight.py --component all` 为 ok
- [ ] 数据库已验证备份
- [ ] 候选代码 SHA 已在服务器核对
- [ ] V3 进程定义/环境/DB 路径已人工核准
- [ ] production rollback SHA 已记录
- [ ] discussion disabled
- [ ] sale isolation 核准
- [ ] dry-run/preview 不碰 Telegram send
- [ ] 受控 rent 发布 + details/photos/book 验收通过
- [ ] 重复发送保护验收通过
- [ ] 同源更新复用身份并重新 review 验收通过

任何一项为 NO：`NO-GO`。
