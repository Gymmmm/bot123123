# 窗口 A — Issue #25 频道 CTA / 同步键盘交接

## 分支与基线

- 分支：`agent/c01c02-channel-cta-A`
- 精确基线：`90180b834c5b13cb5e96aa51b24a081956bb18d2`
- compare：ahead 4 / behind 0；merge-base 精确等于基线。

## 修改内容

1. `v3_core/publishing/telegram_adapter.py`
   - 频道发布详情 CTA：`🏠 房源详情` → `🏠 租赁详情`
   - `📸 更多实拍`、`📅 预约看房` 保持不变。
   - 预约按钮仍沿用 `inventory_status_bookable()` 门控，不改变库存语义。

2. `v3_core/user_bot/channel_status_sync.py`
   - 已发布频道状态同步键盘详情 CTA：`🏠 房源详情` → `🏠 租赁详情`
   - 与发布侧保持一致。
   - 预约门控逻辑保持原样。

3. `tests/v3_core/test_telegram_adapter.py`
   - 旧断言 `🏠 房源详情` 改为 `🏠 租赁详情`。

4. `tests/v3_core/test_user_bot_channel_status_sync.py`
   - 同步键盘旧断言 `🏠 房源详情` 改为 `🏠 租赁详情`。

5. `v3_core/publishing/channel_contract.py`
   - 审核后无需修改：该文件只定义 details/photos/book payload 与 URL contract，不包含用户可见 CTA 标签。

6. `tests/v3_core/test_channel_pure_contracts.py`
   - 审核后无需修改：现有测试验证 action payload/URL contract，不含旧 CTA 文案。

## PR #51 候选提交审查

以下候选均仅包含窗口 A 允许文件中的单行 CTA/测试变更，已按其等价 hunk 手工移植，没有整包 cherry-pick/merge PR #51：

- `8a078a1412fd84437b947222c6c754c026bb679e`
- `62253c483f437afe7c23b4635e7a0672387c2f31`
- `cb51d7f8e3931333c6f2b0068533c4ea780eb8d6`

## 本分支 commits

- `455c47978930815318662128a7c2344277f74756` — feat(channel): lock publish details CTA to 租赁详情
- `63125c086051edb594e9569d6bb3c4a3df794ad6` — feat(channel): lock synced details CTA to 租赁详情
- `a0bb2c425d2ca17f7edeef46452ab2e6be48dce0` — test(channel): align adapter CTA expectation
- `d11d0ea3e9643602468124d81204cb8c369aa813` — test(channel): align sync keyboard expectation

## 测试

要求命令：

```bash
python -m compileall v3_core/publishing/telegram_adapter.py v3_core/publishing/channel_contract.py v3_core/user_bot/channel_status_sync.py
pytest -q tests/v3_core/test_telegram_adapter.py tests/v3_core/test_channel_pure_contracts.py tests/v3_core/test_user_bot_channel_status_sync.py
```

本执行环境无法解析 `github.com`，普通 `git clone/worktree` 失败；GitHub connector 能读写仓库，但不能把完整仓库 materialize 到本地运行 pytest，也没有可用的 workflow-dispatch 动作。因此上述两条命令在本窗口运行环境中 **未验证**，不得宣称全绿。

静态/仓库级验证：

- 分支从精确基线创建：PASS。
- compare merge-base == 精确基线：PASS。
- diff 仅 4 个允许文件：PASS。
- 生产代码发布侧与同步侧均已锁定 `🏠 租赁详情`：PASS（静态）。
- 预约按钮可约性门控未改：PASS（diff 审核）。
- `channel_contract.py` 无旧用户可见标签：PASS（审核，无需改）。
- 指定 pytest/compileall：未验证（环境限制）。

## 冲突

- 无 cherry-pick/merge 冲突。
- 采用手工移植 PR #51 中经过审查的单行 hunk，避免带入其他窗口文件。

## 最终状态

- 功能静态实现：PASS
- 定向运行测试：未验证
- 综合验收：未达到“定向测试全绿”这一项，窗口 E 集成前必须在完整仓库环境补跑指定两条命令。
- 未发现需窗口 E 修复的越界业务问题。
- 未部署、未发正式频道。
- 未修改生产数据库。
- 未合并 `chief/issue25-cta-appt-10commits`。
