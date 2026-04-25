# 侨联本机运行规范（唯一入口）

本目录是本机侨联双 Bot 的唯一主运行目录。

## 只保留这一套

- 主仓：`/Users/a1/projects/qiaolian_dual_bots_local`
- 启动入口：`run_integrated_stack.py`

不要在其它同类目录并行启动（例如 `qiaolian_dual_autopilot`），否则会出现：

- 同一 Telegram Token 抢占 `getUpdates`
- 同一 DB 被多套 pipeline 并发写入
- 发布队列重复入队或状态混乱

## 标准操作

### 1) 先清场

```bash
cd /Users/a1/projects/qiaolian_dual_bots_local
./stop_all_qiaolian_bots.sh
```

### 2) 启动主栈

```bash
cd /Users/a1/projects/qiaolian_dual_bots_local
./start_main_stack.sh --with-publisher
```

可选：

- 仅用户 Bot：`./start_main_stack.sh`
- 用户 + 发布 + 采集：`./start_main_stack.sh --with-publisher --with-collector`

## 目录角色

- `qiaolian_dual_bots_local`：生产/本机唯一运行主线
- `qiaolian_dual_autopilot`：历史/实验目录，仅作参考，默认不运行
