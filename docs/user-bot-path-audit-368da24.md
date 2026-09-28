# User Bot 路径审计（只读）

基线：生产 `368da24` / 分支 `fix/v3-live-ux-hardening-20260924`  
范围：V3 User Bot 主路径。不改六按钮首页，不重做详情结构。

路由总入口：`v3_core/user_bot/app.py`（callbacks / `handle_v3_start`）。

## 路径表

| 流程 | 入口 | Handler | 消息行为 | 返回去向 | 备注 / 缺陷 |
|------|------|---------|----------|----------|-------------|
| 首页冷启动 | `/start` | `telegram_start_handler.handle_v3_start` | **send** | — | 六按钮首页 |
| 回首页 | `v3u:t:home` | `_render_home_callback` | **edit** | 首页 | |
| 智能找房 | `v3u:home:search` | `telegram_home_handler` | **edit** | `v3u:t:home` | |
| 关于 / 预约说明 / 顾问 / 入住管家 | `v3u:home:*` | `telegram_home_handler` | **edit** | `v3u:t:home` | |
| 周边（首页入口） | `v3u:home:local` | `local_life_view(back_to_home=True)` | **edit** | `v3u:t:home` | |
| 找房筛选（区域/预算/户型） | `v3u:t:search_*` / `*_choice:*` | `transition_actions` + handler | **edit** | `v3u:change_search` | |
| 提交搜索 → 首卡 | layout/budget 提交 | `telegram_search_results` → `render_search_card_response` | 筛选 **edit** 后 **send** 结果卡 | — | 文本→图片需新发，预期行为 |
| 上一套/下一套 | `v3u:card:{i}:{QL}` | `telegram_callback_handler` | **edit** | — | |
| 结果卡「📷 房源详情」 | `v3u:listing:details:{QL}` | `_render_photos`（有图） | **edit** 翻页器 | 有搜索会话→`v3u:card…`；否则首页 | 有图时不是独立文字详情 |
| 结果卡「预约看房」 | `v3u:listing:book:{QL}` | `_send_transition_view` | **send** 新预约面板 | 预约内「返回房源」 | **额外消息**：旧卡留存 |
| 结果卡「换条件」 | `v3u:change_search` | `_send_transition_view` | **send** 新找房入口 | 首页 | **额外消息** |
| 照片翻页 | `v3u:listing:photos:{QL}:{n}` | `_render_photos` | **edit** media | 同上 | |
| 预约步骤 | `v3u:t:appointment_*` | transition handlers | **edit** | 上一步 / 返回房源 | |
| 预约「返回房源」 | `v3u:listing:details:{QL}` | listing callback | 预约为纯文本时可能 **send_photo** | 期望回原卡 | **错误返回 + 可能多发一条** |
| 入住服务 | `v3u:home:service` | service handlers | **edit** | 首页 | |
| 周边（服务入口） | `v3u:service:local` | `telegram_service_handler` | **edit** | `v3u:home:service` | 与首页入口返回目标不同（按入口设计） |
| 频道「📷 房源详情」 | `start=property_{QL}_photos` | `channel_contract` → deeplink → `handle_v3_start` | **send** 翻页器（有图） | 首页 | **已确认**：绑 `_photos`，非独立文字详情 |

## 频道 CTA 结论

频道按钮文案「📷 房源详情」对应 deeplink `property_{id}_photos`（`channel_contract.official_channel_button_spec`）。Bot 有图时打开放片翻页器（caption 含租约要点 + 📸 n/N），不是单独文字详情屏。结果卡同文案按钮行为一致。

## 已确认缺陷与最小修改范围（暂不实施）

| 缺陷 | 位置 | 最小修改 |
|------|------|----------|
| 结果卡点预约/换条件 reply 新消息 | `telegram_callback_handler._send_transition_view` | 在 photo 卡上尽量 edit，勿动首页结构 |
| 预约纯文本面板「返回房源」可能新发 photo | `telegram_callback_handler` + `transition_views` | edit 当前面板或锚定原 listing/card message id |
| 不可预约 channel book 可能连发两条 | `telegram_start_handler._render_unbookable` | 合并为单条 |

本文件为审计交付物，不据此改 UI。
