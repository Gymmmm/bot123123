# 发布机器人上线清单（固定流程）

## A. 本机准备

```bash
cd /Users/a1/projects/qiaolian_dual_bots_local
./scripts/smoke_local.sh
```

必须通过：
- 自动测试通过
- workflow 可读
- dry-run 可读

## B. 部署到服务器

```bash
cd /Users/a1/projects/qiaolian_dual_bots_local
./scripts/server_deploy.sh --host 132.243.218.75 --user root
```

## C. 服务器健康检查

```bash
cd /Users/a1/projects/qiaolian_dual_bots_local
./scripts/smoke_server.sh --host 132.243.218.75 --user root
```

必须通过：
- 四个服务 active
- 无最新 readonly / OperationalError
- `scripts/check_workflow.py` 可正常输出

## D. Telegram 业务验收（人工）

1. 在 Telegram 给用户 Bot 发送 `/start`
2. 给发布 Bot 发送 `/start`、`/pending`
3. 验证频道按钮 deep-link 可跳回用户 Bot
4. 验证咨询/预约入库（`leads` / `appointments`）

## E. 回滚预案

- 若发布后异常：
  1. `systemctl status` 定位服务
  2. `journalctl -u <service> -n 200 --no-pager` 查看错误
  3. 必要时恢复上个版本代码并重启服务
