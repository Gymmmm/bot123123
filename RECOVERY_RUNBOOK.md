# 批量替换与房态恢复

基线：PR #70 合并版本 a459f27。已完成的替换通过持久游标保留，不从零开始。

- 发送前 Bot 连接失败：游标不变，5/10/20/40/60 秒退避重试。
- prepared / sending / sent / unknown 或发送后提交异常：不盲目重试，先核对原消息与 durable receipt。
- 历史 blocked 仅可使用 `--recover-before-send` 恢复：要求当前目标未绑定候选、报告 results 为空且无 stopped、全库不存在未结算尝试。
- 服务重启可恢复 ready / waiting / waiting_source；executing 必须核对，不推测是否已发送。
- 来源扫描结束但旧帖尚未完成时继续 waiting_source，补采入队后自动接续，不误报 complete。需要核对的门禁使用退出码 78，服务不循环重启未知发送。
- 来源不够时，原采集程序可用 `BACKFILL_BEFORE_MESSAGE_ID` 从最旧已采集相册的 anchor 之前继续；`BACKFILL_READ_ONLY_SESSION=1` 使用内存授权只读采集，不写运行中的 collector session，不请求登录。解析、媒体与 ready 队列仍走原链路。
- 房态变更由 SQLite 同事务触发持久 outbox。Publisher 每 20 秒处理最多 10 套，同步同房源的所有已发布消息。
- 断网保留 outbox，10 秒起退避，最多 600 秒；重试始终读取最新房态；not modified 视为成功。修订号防止删除并发的新变更。
- 发布候选版本包含事实、来源、房态、审核状态。修正房态会重新检查未发布异常项；已发布、ignored、sending、unknown 不自动重发；销售只存档规则保留。

不创建新频道帖子、不新增备份、不删除预约/线索/账号配置。按钮保持 3 个、两行 2+1。

部署前：完整 V3 测试、编译、diff 检查、精确 SHA 与生产 DB preflight。部署后核对四服务、outbox trigger、48 条原回执及新增替换回执，真实 Telegram 文案/媒体/按钮。
