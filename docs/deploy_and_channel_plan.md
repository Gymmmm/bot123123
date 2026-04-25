# Deploy + Channel + Bot 交互方案（可直接执行）

## 1) 单点运行原则（必须）

- 生产只在服务器运行：`/opt/qiaolian_dual_bots`
- 本机只开发和短时测试，不和生产同 token 双开。
- 同一个 `PUBLISHER_BOT_TOKEN` 只允许一个 long-poll 进程。

## 2) 服务拓扑

- 采集：`collector_bot.py`
- 流水线：`run_pipeline_autopilot.py`（pending -> drafts -> cover -> ready）
- 发布：`v2/run_publisher_bot_v2.py`（ready -> channel published）
- 用户 Bot：`run_user_bot.py`（咨询/预约/留资/服务）

## 3) 频道方案（建议标准）

- 每条房源频道主帖：封面 + 实拍图（主帖最多 4 张）
- 主帖固定文案结构：区域/户型 + 价格 + 亮点 + 风险提醒 + CTA
- CTA 统一走按钮深链，不在正文放杂乱链接
- 深链 payload 建议：
  - `consult__<post_token>__<listing_id>`
  - `appoint__<post_token>__<listing_id>`
  - `more`

## 4) 用户交互路径（目标闭环）

1. 用户在频道点「咨询这套」
2. 跳到用户 Bot `/start <payload>`
3. Bot 展示该房源卡片 + 引导留资
4. 用户提交联系方式/看房时间
5. 系统写入 `leads` / `appointments`
6. 管理员在后台跟进

## 5) 本机 smoke（不触发生产风险）

```bash
cd /Users/a1/projects/qiaolian_dual_bots_local
.venv/bin/python -m unittest discover -p 'test_*.py' -v
.venv/bin/python scripts/check_workflow.py
```

## 6) 部署到服务器

```bash
cd /Users/a1/projects/qiaolian_dual_bots_local
./scripts/server_deploy.sh --host <SERVER_IP> --user <SSH_USER>
```

如首次部署或权限受限，可先：

```bash
./scripts/server_deploy.sh --host <SERVER_IP> --user <SSH_USER> --skip-restart
```

然后在服务器手工执行 `systemctl restart ...`。

## 7) Telegram 实网验收（上线前最后一步）

- 使用“测试 token + 测试频道 + 测试 DB_PATH”做 1 轮完整链路：
  - `/start`
  - 频道发帖
  - 点击咨询按钮
  - 留资写库
  - 预约写库
- 验收通过后再切生产 token。
