# 侨联地产双 Bot · FINAL PRODUCTION STATUS

**验收日期：** 2026-08-20（GMT+7）  
**生产目录：** `/opt/qiaolian_dual_bots`  
**唯一验收样本：** source post 53 → listing `l_44` → draft `DRF_786d27c1-0192-4b5d-9983-4f991a659a22` → approved package `PKG_4f991a659a22_v6` → Telegram `@Jinbianzufanz/2865`。

## 最终结论

本轮明确区域真实房源已经完成一次真实生产发布和用户闭环验收。房源原始区域为**钻石岛**，Parser、normalized area、数据库、封面、频道正文和 User Bot 搜索结果均显示“钻石岛”。冻结包 v6 的 opaque public token 为 `ql59f2c65422e79c`；频道公开预约链接为 `book__ql59f2c65422e79c`，未在公开 URL 中暴露内部 `l_44`。

真实发布记录为频道消息 `2865`，发布状态为 `published`，媒体组 `14297698791745517`。approved package 的 source identity 固定了 source post 53、grouped id `14015044076223581` 和 9 个媒体资产；审计确认这些媒体资产均属于 source post 53。Publisher 重复调用被明确拒绝，返回 `False`，没有产生第二条频道发布记录。

## 验收矩阵

| 项目 | 结果 | 证据或说明 |
|---|---:|---|
| 实际发布套数 | 1 | source53 / l_44，Telegram message 2865 |
| 普通公寓真实发布 | 通过 | 本轮正式样本为钻石岛 6房8卫高价公寓，`property_type` 仍为 apartment；未路由为 villa |
| 别墅真实发布 | 未在本轮新增 | 历史链路已有别墅样本；本轮只发布一套明确区域样本，避免扩大测试面 |
| 封面与正文 | 通过 | 公开预览显示“钻石岛大平层｜6房8卫｜$6,000/月”；无模板变量、无内部 listing id |
| 主图与媒体组 | 通过 | cover 与 image_02–image_09 来自冻结包；source identity 9/9 资产属于 source post 53 |
| 区域搜索 | 通过 | User Bot 选择“钻石岛”与 `$2000+` 后返回 1 套：钻石岛｜6房8卫｜$6000/月 |
| opaque Deep Link | 通过 | `https://t.me/XxxXiaopengbot?start=book__ql59f2c65422e79c` 可打开 User Bot；start 后进入预约方式页 |
| 真实预约 | 通过 | 线下看房 → 08-21 → 下午 14:00–17:00 → 提交；appointments id 15 入库，状态 pending |
| 预约幂等 | 通过 | 重复执行同一预约流程后记录数保持 1，复用预约 id 15；未重复新增预约 |
| 房态门禁 | 通过 | pending、reserved、rented、inactive 均返回“暂不可预约”；active 进入预约方式页 |
| 管理命令 | 通过 | 实际执行 `/listing_status l_44`、`/listing_set l_44 reserved`、`/listing_set l_44 active`，回复与数据库一致 |
| Approved package 冻结 | 通过 | package v6 status approved；Publisher 读取冻结包，重复发布被跳过 |
| content hash | 通过 | package content_hash 为 `e9cfa3ad72f63cf8e54205408e85b53d39c2cc26b10a9a21ab81b1b45b0ecabe` |
| 三项生产服务 | 通过 | collector、publisher-bot、user-bot 当前均 active |
| 当前阻塞问题 | 无 P0/P1 | 仅保留一条早于 User Bot 重启的 Telegram 临时 `httpx.ReadError` 历史日志；重启后未再出现同类错误 |

## 公开内容安全检查

真实频道帖子正文显示区域“钻石岛”、租金 `$6,000/月`、户型“6房8卫”、押付“押2付1”、合同“1年”，并提供“预约看房”和“咨询这套”入口。公开 URL 使用 opaque token；内部 `l_44` 仅存在于服务端 callback data 和数据库，不出现在频道链接或公开 caption 中。冻结正文不存在 `{{...}}` 模板 token，也没有“测试”“甄选”等内部测试标记。

## 状态与恢复说明

发布成功后 Publisher 将业务房态从 pending 激活为 active；管理员测试完成后已将 l_44 恢复为 active。此前 User Bot 进程在本轮代码变更前启动，导致一次旧进程返回不可预约；重启 qiaolian-user-bot.service 后，opaque Deep Link 正常进入预约方式页。预约确认幂等已补强并重启 User Bot，当前三项服务均正常运行。

## 生产冻结意见

当前代码、数据库状态和服务配置可作为唯一生产基线。后续发布必须继续遵循：明确 canonical area → draft → package → 人工 approve → Publisher 读取 approved package → Telegram 成功 → posts 回写 → active 可见。51/52 等没有可靠具体区域的房源继续保持 pending，不得猜测区域或绕过审批。
