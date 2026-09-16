# 剩余风险

## 高优先级：上线前必须确认

1. **真实 HTML 封面渲染未在隔离容器执行。** Chromium 已下载，但容器禁止进程 socket；生产主机必须执行 Playwright/中文字体预检并人工查看所有实际路由模板。Pillow 详情图角标已真实渲染并通过视觉检查，但不能替代 HTML 封面验收。
2. **真实 Telegram 发送未执行。** 需在专用测试频道验证媒体组、讨论线程、按钮深链和 public token；同时检查 A/B/C 的粗体、斜体、下划线、代码字体、删除线与手机折行。不得直接拿首条验证发正式频道。
3. **systemd 与主机权限未验证。** 需核对 `qiaolianbot` 用户、目录属主、`.env` 权限、单实例锁和三服务启动顺序。
4. **生产数据库必须先备份再做 additive migration。** 本次只验证全新 DB 和隔离副本，没有对真实库执行迁移。
5. **旧 Canonical location 可能被新 gate 拒绝。** 历史记录若把市场/项目提升为物理 area、用城市作 area、保存 `太子/幸福` 双候选或 hash 已不一致，必须先在 DB 副本审计。未冻结 draft 可重解析/人工修正；approved/published package 不得原地改写。
6. **中文字体是视觉依赖。** 缺 CJK 字体时详情角标会安全回退英文，不会出现方框字，但中文封面仍须安装 Noto CJK 并逐模板查看。

## 中优先级

- 用户独立复测报告中的 17 个失败已全部修复；当前全量为 156 passed、0 failed、2 skipped。两个跳过项必须继续在生产预检中人工覆盖，不能视为自动通过。
- 物业默认版依赖 Canonical `property_type`。未知类型会安全回退 A；生产首周应抽检“排屋/联排/整栋/商办/厂房/土地”的分类命中率。
- `cover_generator.py` 等图像路径使用 Pillow `getdata()`，全量测试共记录 66 条警告；当前不影响结果，但 Pillow 14 前应升级调用。
- `publication_package.py` 的 `datetime.utcnow()` 已被 Python 标记为弃用，建议后续统一 timezone-aware UTC。
- 高棉文付款条件目前作为未确认信息保守处理；不会虚构公开条款，但可能增加人工审核量。
- 手工管理 Bot 仍保留兼容 UI 代码；直发已禁用，但未来应继续收敛无调用的旧格式化/helper 代码。
- taxonomy 对没有“位置/区域/地址”标签的地名只给 Level 1，以避免伪造精确地址；这提高安全性，但来源文案过于简略时仍会增加人工审核量。
- 自动角标选择基于四角的边缘/对比度启发式。自动测试证明不盖中心并保持比例，但极少数四角都很复杂的照片仍应由管理员在预览中确认。

## 低优先级

- Notion 同步为可选功能，本次未连接真实 Notion，不能保证生产凭证和数据库字段映射。
- 真实手机 Telegram 客户端的不同字体和折行只能人工确认；自动测试检查 caption 长度、HTML 标签平衡、结构和按钮/深链语义。

## 已控制的风险

- 发送超时造成重复帖：以 `unknown` 状态阻断自动重试，必须人工对账。
- 重复采集：source identity、dedupe hash 和唯一索引阻断。
- 图片串房：package 固化 source_post 与 media asset IDs，并在发布前复核。
- 旧逻辑覆盖 approved/published package：冻结规则拒绝重建和 supersede。
- 来源联系方式泄露：清洗、canonical 隔离和最终公开输出安全门三层控制。
- 错误优惠价：仅显式“原价/现价”生成删除线；未标注双价格进入人工审核。
- 人为制造折扣：没有实现“默认原价 + $100”，公开价格始终来自 Canonical 来源证据。
- area 词表漂移：管理、解析、搜索和用户 UI 均从 `listing_taxonomy.py` 派生，平行死 taxonomy 已删除。
