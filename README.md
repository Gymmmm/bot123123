# 侨联地产双 Bot 项目

这个项目当前按一条生产主线运行：

- **`collector_bot.py`**：持续采集源频道房源
- **`run_pipeline_autopilot.py`**：解析、打分、补封面、推送 `ready`
- **`v2/run_publisher_bot_v2.py`**：管理员预览、定时发布、发频道按钮
- **`run_user_bot.py`**：承接频道流量、预约、咨询、留资
- **`run_integrated_stack.py`**：开发/短时联调时**一键**拉起**用户 Bot**（默认）；需要时再 `--with-publisher` 带 v2 发布、`--with-collector` 带采集；生产仍建议独立 systemd/launchd

这些入口共用一个 SQLite 数据库，优先使用这一套，不再建议启旧发布链。

### 与旧「找房助手」单文件的关系

| 找房助手（旧） | 本仓库对应 |
|----------------|------------|
| C 端找房 / 深链 / 留资 / 预约 / 管家 | `qiaolian_dual/user_bot.py`（`run_user_bot.py`） |
| 按钮发帖、频道、槽位、讨论组桥 | `v2/run_publisher_bot_v2.py` + `meihua_publisher.py` 等 |
| JSON 内嵌中介轮询 | 未逐字迁移；留资与顾问在 `user_bot` + `.env`（`ADVISOR_*`）侧 |

## 一眼看结构

先看这两处：

- 结构导航：`docs/PROJECT_STRUCTURE.md`
- 运行入口：`docs/runtime_bot_map.md`

脚本调用约定：

- 优先使用 `scripts/*.py` 作为实际实现入口
- 根目录同名脚本仅保留兼容 wrapper（避免旧命令失效）

## 目录

```text
qiaolian_dual_bots/
├── .env.example
├── requirements.txt
├── collector_bot.py
├── run_pipeline_autopilot.py
├── run_user_bot.py
├── run_integrated_stack.py
├── scripts/dev/                 # 开发/排查脚本归档
├── scripts/ops/                 # 一次性运维脚本归档
├── tests/                      # 统一测试脚本（test_*.py）
├── v2/run_publisher_bot_v2.py
├── meihua_publisher.py
├── autopilot_publish_bot.py   # legacy helper only, kept for v2 compatibility
└── qiaolian_dual/
    ├── config.py
    ├── db.py
    ├── messages.py
    ├── user_bot.py
    └── utils.py
```

## 快速开始

1. 安装依赖

```bash
pip install -r requirements.txt
```

2. 复制配置

```bash
cp .env.example .env
```

3. 填写 `.env`

至少配置这些：

```env
USER_BOT_TOKEN=
PUBLISHER_BOT_TOKEN=
USER_BOT_USERNAME=
CHANNEL_ID=@your_channel
CHANNEL_URL=https://t.me/your_channel
ADMIN_IDS=123456789
```

4. 启动采集 Bot

```bash
python collector_bot.py
```

5. 运行流水线

```bash
python run_pipeline_autopilot.py
```

6. 启动用户服务 Bot

```bash
python run_user_bot.py
```

7. 启动频道发布 Bot（生产优先）

```bash
python v2/run_publisher_bot_v2.py
```

8. （可选）本机一键起**用户 Bot**（默认不含发布机）

```bash
python run_integrated_stack.py
```

要带 v2 发布：`python run_integrated_stack.py --with-publisher`。要带采集：`python run_integrated_stack.py --with-collector`（勿与现网双开，见 `INTEGRATION.txt`）。

## 用户服务 Bot 已做好的功能

- `/start`
- 频道帖子 deep link 承接
- 需求找房：类型 → 区域 → 预算
- 关键词快搜：例如 `BKK1 500-800 公寓`
- 房源详情
- 收藏
- 预约现场 / 视频看房
- 咨询顾问
- 入住管家：绑定、报修、续租/退租、周边配套
- `/subscribe`
- `/appointments`

## 频道运营 / 发布链已做好的功能

- 管理员白名单
- `/pending` 预览待发草稿
- 自动质量门槛与 `ready` 队列
- 定时发布到频道
- 主帖 4 图模式：1 张横版封面 + 3 张真实图
- 自动加频道按钮，承接到用户 Bot
- `/slots`、`/send`、基础统计

## 数据表

项目会自动创建这些表：

- `listings`
- `users`
- `favorites`
- `leads`
- `appointments`
- `tenant_bindings`
- `repair_tickets`
- `subscriptions`

## 老用户后台录入（推荐）

当前老用户回流入口采用“后台直接录入 user_id + 房号信息”，不依赖用户手动输入绑定码。

```sql
INSERT INTO tenant_bindings (user_id, binding_code, property_name, lease_end_date, rent_day, status, created_at)
VALUES (8675309, 'SYS-8675309', 'BKK1 The Peak 12A', '2026-12-31', 15, 'active', datetime('now', 'localtime'));
```

说明：
- `user_id`：Telegram 用户 ID（后台维护）
- `property_name`：房号/项目名（用于老客识别展示）
- `rent_day`：每月交租日
- `lease_end_date`：合同到期日期

用户点击「我以前在侨联租过」时，Bot 会直接读取这条记录并展示，作为续租/换房引流入口。

## 建议上线顺序

1. 先启动用户服务 Bot
2. 再启动采集与流水线
3. 最后启动频道发布 Bot
4. 从频道按钮点进用户 Bot 测试：
   - 咨询
   - 收藏
   - 预约
   - 同区域更多

## 运维脚本（新增）

- `scripts/bootstrap_db.py`：统一入口，初始化/修复 schema
- `scripts/check_workflow.py`：只读检查采集/草稿/ready 队列状态
- `scripts/publish_ready_batch.py`：应急批量发送 `ready`
- `scripts/install_macos_launchd.sh`：本机安装 launchd（pipeline + publisher）
- `scripts/disable_macos_launchd.sh`：卸载本机 launchd
- `scripts/server_deploy.sh`：rsync + 依赖更新 + schema + 重启服务（服务器）
- `scripts/smoke_local.sh`：本机一键体检（测试 + workflow + dry-run）
- `scripts/smoke_server.sh`：服务器一键体检（service + workflow + 日志 + token）

服务器全自动建议（生产）：

- 保持 `AUTO_APPROVE=true`（有封面且达分数门槛自动入 `ready`）
- 保持 `qiaolian-publisher-bot.service` 在线（按 `/slots` 定时从 `ready` 发帖）
- 启用 `qiaolian-pipeline.timer`（每 2 分钟跑一次 `run_pipeline_autopilot.py`）

## `houses.csv` 采集与自动发布（推荐）

目标链路：`房源数据表 -> 自动封面图 -> 4图组合 -> Bot 发 TG 频道`

1. 从数据库采集房源到 CSV（已做去重+去噪）：

```bash
./.venv/bin/python tools/collect_houses_csv.py --out data/houses.csv --limit 120
```

如需把“当前 house CSV”重新落回 `source_posts` 进入解析链路，可直接：

```bash
./.venv/bin/python tools/property_intake.py --house-csv
```

默认会：
- 去重（同 `source_post_id` 仅保留最新）
- 去重（同 项目/区域/户型/价格 指纹仅保留一条）
- 过滤无价格条目（避免“价格私聊”文案进频道）

2. 本机先做 dry-run（不发 Telegram）：

```bash
./.venv/bin/python tools/publish_houses_csv.py --csv data/houses.csv --limit 3 --dry-run
```

3. 实网发布到频道：

```bash
./.venv/bin/python tools/publish_houses_csv.py --csv data/houses.csv --limit 10
```

`houses.csv` 首列字段兼容 Excel 手工改表：

`title,area,type,price,image_cover,image2,image3,image4,feature1,feature2,feature3,brand,caption,contact`

补充字段（`project/layout/size/floor/...`）会自动用于生成更完整封面和文案。

如需自定义 HTML 封面模板（占位符模式），可加：

```bash
./.venv/bin/python tools/publish_houses_csv.py --csv data/houses.csv --render-template /absolute/path/template_render.html
```

手工实网脚本已清理，避免干扰自动测试与运行视图。

上线总流程清单见：

- `docs/release_checklist.md`

## 注意

- 发布 Bot 只给管理员使用
- 两个 Bot 的 Token 必须不同
- `USER_BOT_USERNAME` 要填对，否则频道帖子按钮不会跳对
- 旧 `run_publisher_bot.py` / `qiaolian_dual/publisher_bot.py` 已移除
- `autopilot_publish_bot.py` 只是 v2 的兼容 helper，不再作为独立服务运行
