# 侨联地产 Telegram 双机器人系统最终生产验收

验收日期：2026-08-22（Asia/Phnom_Penh）  
发布标识：`qiaolian-final-verified-20260822-r3`

## 结论

**用户提供的“17 failed”复测结论已作为阻断项全部处理。隔离代码验收现为 156 passed、0 failed、2 skipped。**

本轮没有连接 `50.114.74.120`，没有读取或修改生产数据库、systemd 服务、频道消息或真实 Telegram 数据。代码可以进入生产预检，但仍须先按部署文档完成备份、只读迁移预检、测试频道烟测与五模板视觉确认，才批准切换正式服务。

## 已通过

- 全量 Python `compileall`。
- 全量 pytest：`156 passed, 2 skipped, 66 warnings, 21 subtests passed`。
- 冷启动 `schema_core.sql` 可直接支撑 intake → parser → materializer，不再出现 `no such table: listings`。
- source intake 的持久化边界与自动分类边界分离；真实 Telegram 单图/相册监听仍显式启用自动分类。
- 缺失、未审核或坏媒体 publication package 均 fail closed，并把草稿回退到 `pending`，留下发布日志。
- 用户 Bot 首页、按钮、预约、start 参数、session、分页/详情和拆分前兼容入口回归通过。
- 频道按钮恢复为预约、咨询、更多实拍、类似房源四个移动端动作；美化发布器使用受跟踪的“咨询这套”入口。
- 封面 renderer 在自身边界直接读取 Canonical Facts，并优先于遗留 `area/project/price` 字段；出售不带 `/月`。
- `publish_ready_batch.py` 只选择 draft 与 frozen package 均为 `approved` 的记录。
- CSV 直发、旧 zufang555 直发与 `send-next` 旁路均已禁用；样板发送器只读已审核冻结包的 `post_text` 与封面。
- 原有 area/gate、空值保护、标题去重、联系人清洗、媒体隔离、重复采集、重复发布、冻结保护等回归继续通过。

## 已修复并通过

1. `listings` 表加入核心 schema；materializer 也会幂等创建，修复本地 intake 冷启动失败。
2. 发布器在 approved package 缺失、媒体损坏或发送失败时统一回退 `pending`；scheduler 会修复孤立的 `ready/approved` 队列项。
3. 为拆分后的用户 Bot 增加稳定兼容 facade 与依赖传递，修复预约、深链和按钮流程的行为/测试合同漂移。
4. 首页改为三项高频入口，频道动作改为四个明确按钮；欢迎语、品牌与预约通知格式同步。
5. 持久化 intake 默认只入库为 `pending`；真实监听 handler 显式请求分类打包，避免工具调用产生隐含副作用。
6. HTML 封面字段改为 canonical-first：project、alias、property type、deal type、layout、public location、size、floor、price、QC/ref 与 highlights 同源。
7. 禁止批量/CSV 工具绕过 approved frozen package；准备素材仍可用，但真实发送必须回主发布器。
8. 增加 5 项本轮专项测试，覆盖 canonical 封面优先级、租售单位及批量发布门禁。

## 未通过

无代码测试失败项。

## 无法验证

- 当前容器禁止 Chromium 创建 process-singleton socket，因此 r3 修改后的五模板 Playwright 像素重跑无法在本容器完成。用户提供的独立复测显示修改前五模板均能生成 PNG；r3 的字段映射通过代码级回归，但不能替代上线前视觉烟测。
- 未执行任何真实 Telegram API 发送、频道评论线程、用户账号交互或生产 systemd 启停。
- 未读取生产数据库中的遗留数据分布、文件权限、Token、字体和浏览器安装状态。

## 需要生产环境人工确认

1. 对生产数据库做备份和只读 schema/legacy location 预检。
2. 在测试频道走完整链：采集 → 待审 → 三版预览 → 重做封面 → 审核冻结 → `/send` → 评论 → 咨询/预约。
3. 用五类房源各渲染一张封面，确认中文字体、裁切、水印、项目别名、物业类型、售价/月租单位。
4. 用手机 Telegram 确认 caption 长度、粗细/代码/删除线与四个频道按钮。
5. 核对 Token、Bot username、频道/讨论组 ID、systemd 用户、目录权限与单实例。

## 验收判定

- 隔离代码验收：**通过**。
- 生产直接切换：**尚未执行，需人工预检**。
- 完成人工预检且无阻断项后：**可部署**。
