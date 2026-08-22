# 侨联地产双 Bot · 阻塞项清单

## 当前生产阻塞

截至 2026-08-20 08:08（服务器时间），没有发现阻塞生产的 P0 或 P1 问题。source53 已真实发布，User Bot 区域搜索、opaque Deep Link、预约、幂等和房态门禁均通过；三项 systemd 服务均为 active。

## 已关闭问题

| 问题 | 状态 | 关闭依据 |
|---|---|---|
| 频道公开 URL 暴露 l_44 | 已关闭 | package v6 使用 `book__ql59f2c65422e79c`，公开频道 URL 无 l_44 |
| approved package hash mismatch | 已关闭 | package 构建器和 Publisher 均纳入 public_token；v6 发布成功 |
| 未发布房源提前可见 | 已关闭 | active + published draft + 成功 Telegram post 的联合门禁；房态矩阵通过 |
| 重复预约 | 已关闭 | 同一真实预约流程重复执行后 appointments 数量保持 1 |
| 未发布 source53 不可预约 | 已关闭 | 发布后 listing 激活且 User Bot 重启，opaque Deep Link 进入预约方式页 |

## 非阻塞观察项

User Bot 重启前日志保留一条 Telegram 网络层 `httpx.ReadError`，发生在旧进程处理请求时；重启后服务正常，没有出现持续网络错误。PTB `per_message=False` warning 仍存在，但不影响处理、预约或服务启动，本轮未进行高风险 ConversationHandler 重构。测试会话与生产 Telethon session 发生过一次 SQLite session lock；原因是测试脚本与正在运行的 Collector 共用了同一 Telethon SQLite session，后续使用只读复制会话完成验证，没有影响 Collector 生产服务。后续测试应始终使用复制会话。

## 暂不处理的业务数据

source posts 51/52 因缺少可靠具体区域继续保持 pending，不猜测 DV11 Vila town 或 DV07 Chip Mong 的 canonical area，不伪造 approved package，也不修改历史 legacy 帖子。
