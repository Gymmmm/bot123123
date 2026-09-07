# 侨联地产 Telegram 系统｜当前生产全流程审计说明书

**生产基准 SHA：** `8e4605cf5cc21dfec3ce30729654b09e39de9abf`  
**生产分支：** `codex/qiaolian-ui-cleanup-20260829`  
**审计原则：** 以该 SHA 的真实运行代码为准。历史文档、旧代码、未注册 Handler、兼容 callback 不等同于当前主流程。

---

# 0. 最终结论

当前线上系统不是单一 Telegram Bot，而是由三条生产链共同组成：

```text
采集源 / 管理员手工导入
        ↓
Collector / source_posts
        ↓
AI Parser / canonical facts
        ↓
drafts
        ↓
人工审核
        ↓
publication package 冻结包
        ↓
Publisher Bot
        ↓
Telegram 房源频道
        ↓
3 个房源按钮
        ↓
User Bot
        ↓
找房 / 详情 / 更多实拍 / 咨询 / 预约
        ↓
预约 / 租约 / 入住服务 / 报修 / 顾问后台
```

当前生产常驻服务应理解为：

```text
1. qiaolian-collector
   负责采集源 → 原始房源 → 解析入口

2. qiaolian-publisher-bot
   统一管理员 / 发布 Bot
   负责录入、待审、审核、封面、发布、房态、广播、采集源管理

3. qiaolian-user-bot
   面向租客
   负责找房、房源详情、实拍、预约、咨询、入住服务、租约
```

---

# 1. 当前系统需要严格区分的三种代码

整个仓库现在最容易看乱的地方，是三种代码混在一起。

## A. 当前正式主流程

真正由当前 SHA 注册、用户或管理员可以正常走到的流程。

文档正文主要描述这一类。

---

## B. 线上仍可达的兼容流程

新首页已经不主动展示，但旧链接、旧 callback 或某些下级按钮仍然能进入。

例如：

```text
富力城导航
部分旧 Deep Link
旧房源编号 QC/QJ
旧预约 callback
```

这些不能当作新产品主流程，但也不能说已经彻底删除。

---

## C. 仓库里存在但当前没有注册的旧功能

这类代码还在文件里，但当前生产 Bot 不会正常暴露入口。

例如 Publisher `simple_mode=True` 下，一批旧命令并没有注册：

```text
/approve
/reject
/pause
/resume
/slots
/stats
/analytics
/check
/post_menu
/post_index
/tpl
/tpl_use
/tpl_test
```

注意：

部分函数仍可能被其他内部按钮直接调用，因此判断“线上有没有”必须看：

```text
函数存在
≠
Handler 已注册
≠
首页有入口
≠
用户真实可达
```

---

# 2. 房源进入系统

当前主要有两种来源。

---

## 2.1 Telegram 自动采集源

管理员后台存在：

```text
📡 采集源
```

当前 Collector 的真实来源配置使用 `sources.json`。

管理员可以通过类似：

```text
/source_add @频道用户名
```

增加 Telegram 频道采集源。

增加成功后会更新 `sources.json`，并重新启动 Collector。

采集链：

```text
Telegram Channel
      ↓
Telethon Collector
      ↓
source_posts
      ↓
media_assets
      ↓
AI Parser
      ↓
drafts
```

因此以前理解的：

```text
Telethon
↓
raw_message
↓
parse_house.py
↓
houses.csv
```

已经不是当前线上完整生产架构。

当前正式数据核心已经进入 SQLite：

```text
source_posts
media_assets
drafts
listings
publication_packages
posts
appointments
leads
...
```

---

# 3. 管理员手工导入房源

Publisher 首页当前有：

```text
➕ 发布房源
🔵 房态管理

📢 广播中心
📡 采集源

🧪 查看发布效果
📚 发布记录
```

其中：

```text
➕ 发布房源
```

实际进入的是统一 intake 流程。

---

## 3.1 导入页

管理员看到：

```text
📥 微信房源导入

把微信里的房源文字和图片直接发给我。
文字、图片谁先发都可以，多张图可以连续发。

收齐后点 ✅ 完成导入，我会自动合并、解析并生成待审草稿。
```

按钮：

```text
[ ✅ 完成导入 ]
[ ❌ 取消 ]
```

---

## 3.2 文字和图片收集

管理员可以：

```text
先发文字
再发图片

或者

先发图片
再补文字
```

系统会把导入过程中的文字合并。

图片保存进 intake 上下文，然后登记为：

```text
source_post
media_assets
```

而不是单纯把 Telegram file_id 临时拿来发帖。

---

# 4. 完成导入

点击：

```text
✅ 完成导入
```

系统会：

```text
1. 生成 source_post
2. 保存原文字
3. 保存原始图片
4. 创建 source media identity
5. 调用 AIParserModule
6. 解析房源
7. 生成 canonical facts
8. 尝试生成 draft
```

如果成功，管理员看到房源导入结果。

---

# 5. 导入结果卡

当前结果卡大致结构：

```text
✅ 房源导入结果

房源编号：QL-XX-X0X0
解析状态：已解析
图片数量：4
缺失字段：无
当前状态：待审核
```

如果缺失：

```text
暂不能发布：项目/区域、租金、至少4张图片……
```

按钮：

```text
[ 👀 预览房源 ] [ ✏️ 修改内容 ]
[ 📥 暂存 ]
[ 🏠 返回首页 ]
```

只有达到允许发布条件并已经通过审核时，才会出现：

```text
📤 发布到频道
```

---

# 6. 当前发布最低资料要求

当前简单审核队列的核心门禁包括：

```text
canonical facts 必须有效
必须是租赁 deal_type=rent
必须有 public_location_display
必须有 property_type
必须有 layout
租金 > 0
原始实拍至少 4 张
不能已经冻结 published / approved
不能存在 blocking_flags
```

因此：

```text
文字解析出来 ≠ 可发布
```

还必须通过 canonical 和 media gate。

---

# 7. Canonical Facts

当前 Parser 不应该再直接把原始文字字段当作最终公开字段。

正确链路：

```text
raw source
   ↓
parser
   ↓
canonical_facts.v1
   ↓
draft_projection
   ↓
draft / listings / publication package
```

核心字段包括：

```text
project_name
project_alias
property_type
public_location_display
normalized_area
layout
monthly_rent_usd
size
floor
payment_terms
contract_term
available_date
management_fee
internet_fee
water_rate
electric_rate
parking_fee
highlights
...
```

特别需要注意：

```text
项目名
项目别名
物业类型
区域
```

现在应独立表达。

例如：

```text
project_name = 永旺一
project_alias = Aeon1
property_type = 公寓
area = 独立 canonical location
```

不应该再把：

```text
永旺一 Aeon1公寓
```

全部塞进 project_name。

---

# 8. Parser 失败与草稿找不到

管理员曾经看到的：

```text
⚠️ 解析完成但未找到草稿，可用 /intake_pending 查看。
```

这个提示的真实意思不是：

```text
一定没有识别到租金
```

而是：

```text
AI Parser 调用结束以后，
程序再次查询 source_post 对应 draft，
没有找到有效 draft_id。
```

可能原因包括：

```text
canonical facts 门禁失败
parser 没有生成 draft
生成后被质量逻辑拦截
source_post / draft 关联异常
异常被 parser 内部吞掉
字段满足旧 parser 但不满足 canonical schema
```

所以这种问题必须查：

```text
source_posts
normalized_data
parse_status
drafts
parser logs
```

不能只让管理员重新手工改房源。

---

# 9. 房源内部编号与公开编号

系统内部仍以类似：

```text
l_44
l_123
```

作为 listing_id。

这属于内部主键。

公开编号目前采用新的稳定编号：

```text
QL-{位置码}-{4位随机字母数字}
```

例如：

```text
QL-RF-K7M2
QL-BK-A2F7
QL-DD-H4P8
```

位置码示例：

```text
RF = 富力城
BK = BKK1
B2 = BKK2
B3 = BKK3
DD = 钻石岛
AE = 永旺1
A2 = 永旺2
TK = TK / 7月区
SS = 森速
HS = 洪森大道
CH = 水净华
PP = 无法识别时 fallback
```

---

# 10. QC001 / QC0350 / QJ 等旧编号

以下编号仍然支持解析：

```text
QC001
QC0350
QJ001
L_44
```

但属于：

```text
legacy compatibility
```

转换逻辑类似：

```text
QC44 → l_44
QJ44 → l_44
L44  → l_44
```

当前新房源不应该再以：

```text
QC001
QC0350
```

作为新生成编号。

以后公开标准应统一：

```text
QL-XX-X0X0
```

---

# 11. 待审核房源

当前审核队列会过滤出真正可以审核的草稿。

审核页最多显示：

```text
6 套
```

而不是早期代码的 1 套。

管理员看到类似：

```text
📋 待审核 1/6
· 可审核 X 套
· 资料不完整 X 套

🏠 请确认这套房源
{项目/区域}
```

下面是正式封面预览。

---

# 12. 当前封面模板

当前审核阶段提供三种封面：

```text
经典蓝卡
右侧价格牌
黑金高级感
```

选中后会显示：

```text
✅ 经典蓝卡
```

之类的状态。

按钮：

```text
[ 经典蓝卡 ] [ 右侧价格牌 ]
[ 黑金高级感 ]

[ 👀 刷新正式预览 ]

[ ✅ 确认封面，生成发布键 ]

[ ✏️ 修改文案 ] [ 🖼 重新生成封面 ]

[ 🗑 丢弃 ]
```

---

# 13. 当前 Publisher 不是现场直接发

生产安全设计已经改成：

```text
草稿
↓
预览
↓
生成 publication package
↓
人工确认
↓
冻结
↓
approved
↓
publish
```

而不是：

```text
管理员改一下文字
↓
程序现场重新生成封面
↓
直接发频道
```

---

# 14. Publication Package 冻结包

审核通过后，会生成一份冻结的 publication package。

里面固定：

```text
canonical facts snapshot
source identity
source media hashes
cover
main_images
discussion_images
post_text
caption variant
cover template
public token
content hash
```

它的目的就是：

```text
审核时看到什么
=
发布时发什么
```

Publisher 后面只读取冻结包。

---

# 15. 当前频道发布形态

这是当前生产非常重要的一点。

## 当前正式频道房源主帖：

```text
1 张处理后的房源封面
+
Telegram Caption
+
3 个按钮
```

不是：

```text
四图相册
```

也不是：

```text
主帖1张 + 评论区实拍
```

---

# 16. 频道正文当前格式

当前正式正文由：

```text
qiaolian_dual/channel_post.py
```

统一输出。

大致：

```text
🏠 {项目/区域}｜{户型}
💰 ${租金}/月

🏢 {物业类型}｜{面积}｜{楼层}
🔑 {押付}｜租期{租期}

🟢 当前可预约　QL-RF-K7M2

#区域 #户型 #租金段
```

空值不会硬塞：

```text
待确认
—
暂无
未知
面议
随时入住
```

这一类占位词会尽量被清理。

---

# 17. 频道当前三个按钮

固定：

```text
[ 🏠 房源详情 ] [ 📸 更多实拍 ]
[       📅 预约看房       ]
```

Deep Link 进入 User Bot。

目前不在频道主帖直接放：

```text
咨询
收藏
地图
视频看房
看相似
```

---

# 18. 频道评论区

仓库里仍然保留：

```text
send_discussion_three_segments()
discussion map
discussion bridge
```

等旧评论区系统。

但是生产 patch 已明确禁用：

```text
send_discussion_three_segments = no_discussion_segments
```

所以当前新房源：

```text
不会自动把更多实拍发到评论区
不会自动把详情复制到评论区
不会自动发讨论区 CTA
```

当前正式架构是：

```text
频道负责主帖
User Bot 负责详情和实拍
```

---

# 19. User Bot 首页

当前 `/start` 主入口实际文案：

```text
💎 侨联地产｜您在金边的自己人

找房、约看房、入住服务，都可以从这里开始。

如果您已经在频道看到具体房源，直接点房源下方按钮进入，房源信息会自动带上。

也可以直接告诉我：区域 + 预算 + 户型。

请选择您现在需要的服务：
```

注意：

当前 `channel_welcome_text()` 实际直接返回 `welcome_text()`。

所以代码里虽然还有其他：

```text
您好，我是侨联小管家
```

文案函数，但当前 `/start` 主入口并不是那一版。

---

# 20. User Bot 首页按钮

当前：

```text
[ 🔍 帮我找房 ] [ 📅 我的预约 ]

[ 🛡 侨联保障 ] [ 🛠 入住服务 ]

[ 房源频道 ] [ 💬 联系我们 ]
```

如果没配置频道 URL：

```text
[ 💬 联系我们 ]
```

会单独一行。

---

# 21. 帮我找房

点击：

```text
🔍 帮我找房
```

当前不是立即强制进入区域步骤。

首先进入：

```text
🔍 想找什么样的房子？

直接发一句话就可以：

「BKK1 一房，预算 $600」
「富力城两房，要能做饭」
「想找高层、安静一点的」

我们会根据您的需求，优先筛选 2–3 套更值得看的房源。

还没想好？也可以按条件找 👇
```

按钮：

```text
[ 📍 按区域 ] [ 💰 按预算 ]
[ 🏠 按户型 ] [ 🏘 当前可约 ]
[ ⬅️ 返回首页 ]
```

---

# 22. 按区域

当前区域按钮：

```text
BKK1
BKK2/3

钻石岛
富力城

永旺1
TK

俄市
炳发城

水净华
森速

📍 其他区域

⬅️ 返回
```

---

# 23. 其他区域

如果用户输入一个没有按钮的区域，系统尝试识别。

随后进入预算。

---

# 24. 按预算

当前预算：

```text
$400以内
$400–600

$600–800
$800–1200

$1200–1500
$1500+

✍️ 自己输入

⬅️ 返回
```

---

# 25. 自定义预算

用户可输入：

```text
800以内
600-900
1500以上
```

识别失败提示：

```text
预算没有识别出来。请试试：
800以内、600-900 或 1500以上。
```

---

# 26. 按户型

当前：

```text
单间
一房

两房
三房

四房+
不限

⬅️ 返回
```

---

# 27. 房屋类型

部分精确找房链路还支持：

```text
🏢 公寓
🏡 别墅
🏘 排屋
🏪 商铺
💼 办公室
不限类型
```

---

# 28. 找房搜索逻辑

搜索会保存：

```text
area
property_type
budget_min
budget_max
```

到：

```text
last_search_pref
```

并创建 lead：

```text
search_pref_submit
```

管理员会收到：

```text
新找房条件

用户
联系方式
类型
区域
预算
```

---

# 29. 找到房源

当前搜索最多读取少量匹配房源。

结果由 User Bot 作为房源卡发送。

房态公开允许：

```text
active
reserved
```

已租出、下架等不会作为正常可约结果。

---

# 30. 无匹配

当前文案：

```text
🔎 暂时没有完全符合条件的房源

{条件摘要}

您可以调整一个条件继续找，
也可以让中文顾问按这个需求继续留意。
```

按钮：

```text
[ ✏️ 调整条件 ] [ 🏘 看相近房源 ]
[ 💬 联系我们 ]
[ 🏠 返回首页 ]
```

---

# 31. 频道「房源详情」

频道点击：

```text
🏠 房源详情
```

Deep Link：

```text
property_{公开编号}_details
```

User Bot 解析公开编号：

```text
QL-XX-X0X0
↓
内部 listing_id
```

然后进入：

```text
listing_cost_text()
```

---

# 32. 当前房源详情真实结构

当前生产详情不是旧文档那套：

```text
费用明细
物业
水费
电费
网络
停车
佣金
```

这些字段不会全部作为独立行在当前详情主页面直接展开。

当前更接近：

```text
🏠 房源详情｜QL-XX-X0X0

🏠 {项目｜户型}
📍 {区域}
💰 {租金}
📐 {面积}
🏢 {楼层}

📄 租约
{押付方式｜租期}

{房态}

📸 实拍编号 / 公开编号

💬 侨联说
{生成内容}
```

具体空字段按实际房源省略。

---

# 33. 「侨联说」

详情会通过 talk engine 生成简短自然语言。

目标不是地产广告，而是：

```text
一个长期在金边跑房、看房的人，
看到房子的实际特点顺手说两句。
```

它不是 canonical fact。

所以应该明确区分：

```text
事实字段
≠
侨联说
```

---

# 34. 费用字段实际在哪里

虽然详情主页面不全部展示，但系统 canonical facts 和 publication package 中仍保留：

```text
management_fee
internet_fee
water_rate
electric_rate
parking_fee
cost_notes
```

另外：

```text
侨联保障
```

中会告诉用户这些费用在签约前需要逐项确认。

所以是：

```text
数据层存在
↓
主详情没有全部铺开
↓
保障/人工顾问继续确认
```

不是字段已经被删除。

---

# 35. 更多实拍

点击频道：

```text
📸 更多实拍
```

当前正式路径：

```text
User Bot
↓
查询该 listing 的 media
↓
发送 Telegram 图片 / media group
```

不是：

```text
跳频道评论区
```

也不是：

```text
跳到其他 Telegram 帖子
```

---

# 36. 更多实拍当前设计

实际是 Telegram 原生图片浏览。

因此目前没有：

```text
第 1/9 张
上一张
下一张
每张独立房源 caption
```

这不是 bug，是当前实现方式。

---

# 37. 咨询具体房源

房源详情或其他具体房源页点：

```text
💬 联系我们
```

系统会保留：

```text
contact_listing_id
```

并通知管理员：

```text
用户联系我们

用户
联系方式
入口
咨询房源：QL-...
```

用户看到：

```text
💬 已记录您咨询的房源

🏠 {项目｜户型}
💰 {租金}
🆔 QL-...

点击下方即可联系我们。
这套房的信息已经带上，不用重新说明。
```

---

# 38. 顾问按钮

如果配置了：

```text
ADVISOR_TG
```

按钮会直接打开顾问 Telegram。

并预填类似：

```text
您好，我想咨询房源 QL-RF-K7M2

富力城｜两房
$800/月
```

---

# 39. 普通联系我们

没有具体房源时：

```text
💬 有什么需要，直接告诉我们。

找房的话，可以直接发送：
区域 + 预算 + 户型

例如：
「BKK1 两房，$900以内」
「富力城一房，要能做饭」

不方便打字，也可以直接联系我们。
```

---

# 40. 预约看房入口

频道或房源页：

```text
📅 预约看房
```

会检查房态。

允许预约：

```text
active
reserved
```

不允许：

```text
rented
inactive
offline
部分 pending
不存在
```

---

# 41. 当前预约第一步

现在已经取消旧版：

```text
先选实地 / 视频
```

正常新流程直接进入日期页。

默认：

```text
mode = offline
```

也就是实地看房。

用户看到：

```text
📅 预约看房｜QL-...

🏠 {项目｜户型}
💰 {租金}

哪天方便看房？
```

按钮：

```text
[ 今天 ] [ 明天 ]
[ 后天 ] [ 📅 其他日期 ]

[ 🎥 改为视频看房 ]

[ ⬅️ 返回房源 ] [ 🏠 返回首页 ]
```

---

# 42. 视频看房

点击：

```text
🎥 改为视频看房
```

同一日期页切换为：

```text
🎥 视频看房｜QL-...
```

按钮变为：

```text
🚶 改为实地看房
```

所以视频看房没有另起一套复杂流程。

---

# 43. 自定义日期

支持：

```text
0820
820
08-20
8-20
8月20日
下周三
周五
```

系统会进行标准化。

---

# 44. 预约时间

选择日期后进入：

```text
上午 09:00–12:00
下午 14:00–17:00
晚上 17:00–19:00
✍️ 其他时间
```

底部：

```text
⬅️ 修改日期
🏠 返回首页
```

---

# 45. 当前预约没有最终确认页

代码仍然有：

```text
_appointment_confirm_text()
_appointment_confirm_keyboard()
```

但注释已经明确：

```text
历史确认页兼容文案；
新流程选完时间直接提交。
```

因此正式路径是：

```text
房源
↓
日期
↓
时间
↓
直接提交预约
```

而不是：

```text
日期
↓
时间
↓
联系方式
↓
最终确认
↓
提交
```

---

# 46. 当前预约没有手动联系方式页

预约记录直接使用 Telegram 用户身份。

优先：

```text
@username
```

没有 username 时：

```text
Telegram user ID
```

用户不需要再填：

```text
手机号
微信
Telegram
```

---

# 47. 预约成功

提交后：

```text
✅ 预约申请已提交

顾问确认房态和时间后，
会通过 Telegram 联系你。
```

对应按钮通常：

```text
[ 📅 查看我的预约 ] [ 💬 联系我们 ]
[ 🔍 继续找房 ]
```

---

# 48. 预约幂等

系统会避免用户重复提交同一预约造成多条相同记录。

生产历史验收中已经验证：

```text
相同预约重复执行
↓
复用已有 appointment
↓
不重复新增
```

---

# 49. 预约数量自动改变房态

这是当前系统比较重要的自动逻辑。

同一房源活跃预约数：

```text
0
↓
active
↓
🟢 当前可预约
```

```text
1–4
↓
reserved
↓
🟡 已有预约 · 仍可预约
```

```text
≥5
↓
pending
↓
🔵 已有5份预约看房，房态待确认
```

---

# 50. 达到 5 份预约后频道变化

达到 5 份有效预约：

频道主帖会自动编辑。

原来：

```text
[ 房源详情 ] [ 更多实拍 ]
[ 预约看房 ]
```

变为：

```text
[ 房源详情 ] [ 更多实拍 ]
```

即自动移除：

```text
📅 预约看房
```

直到房态重新确认。

---

# 51. 管理员手工房态优先

管理员可设置：

```text
active
pending
reserved
rented
inactive
```

对于明确人工设置的终态：

```text
rented
inactive
offline
```

自动预约计数逻辑不会擅自改回 active。

---

# 52. 我的预约

首页：

```text
📅 我的预约
```

展示用户自己的预约记录。

从预约卡可继续：

```text
查看预约
联系顾问
回到房源等
```

---

# 53. 侨联保障

首页：

```text
🛡 侨联保障
```

当前保障核心方向：

```text
费用
入住交接
押金与退租
留档
```

---

# 54. 费用与押金

公开说明包含：

```text
看房和签约前
核对：
月租
押付
起租日
水电
网络
物业
停车

入住当天
记录：
房屋现状
水电表
家具家电

准备退租时
协助核对：
押金
交接事项
```

同时强调：

```text
具体金额、责任和协助范围，
以签约材料和双方确认内容为准。
```

---

# 55. 入住交接 / 押金退租材料

V2.2 已包含正式资源：

```text
handover.png
handover.pdf

deposit.png
deposit.pdf
```

用户进入相关保障页时，会发送：

```text
PNG
+
PDF
+
说明文字
```

不是仅显示一段 Telegram 文本。

---

# 56. 入住服务

首页：

```text
🛠 入住服务
```

当前文案：

```text
🛠 入住服务

住下以后，有事也可以找侨联。

不管房子是不是通过侨联租的，
住房或生活上遇到问题，都可以先问问我们。

能协助处理的，我们协助处理；
需要专业服务的，我们帮您对接。
```

按钮：

```text
[ 🔧 设备报修 ] [ 🏢 物业协调 ]
[ 📦 生活服务 ] [ 💬 其他帮助 ]
[ ⬅️ 返回首页 ]
```

---

# 57. 设备报修

设备：

```text
❄️ 空调
🚿 热水器

🧺 洗衣机
🧊 冰箱

📶 网络
🔐 门锁/门禁

🔧 其他设备
```

点击后：

```text
请发送问题照片或短视频，
并简单说明异常情况。
```

例如：

```text
空调可以启动，但一直不制冷。
```

---

# 58. 报修预约时间

报修可以选择：

```text
今天内安排
明天上午
明天下午
```

提交后写入：

```text
repair ticket
```

并通知管理员。

---

# 59. 报修管理员通知

包含：

```text
客户
联系方式
房源
问题
说明
希望时间
```

并带后台处理按钮。

---

# 60. 报修状态

支持：

```text
new
accepted
scheduled
in_progress
need_info
done
```

用户看到的是中文：

```text
已接手
已安排
处理中
需要补充
已完成
```

不会看到数据库英文状态。

---

# 61. 物业协调

用户看到：

```text
🏢 物业协调

如遇噪音、停车、门禁、
公共区域或垃圾处理等问题，
可以直接说明具体情况。

建议包括：
• 发生了什么
• 大概从什么时候开始
• 是否已经与物业沟通过
```

---

# 62. 生活服务

进入：

```text
📦 生活服务
```

按钮：

```text
🧹 保洁家政
🚚 搬家协助

🗺 周边推荐
💬 其他需求

⬅️ 返回
```

---

# 63. 周边推荐

当前：

```text
🗺 周边服务

可查看富力城已核实的周边商家；
其他区域可提交需求，
我们会在1个工作日内反馈。
```

按钮：

```text
🏙 富力城导航
📍 其他区域需求提交
⬅️ 返回生活服务
```

---

# 64. 富力城导航的状态

代码注释写着：

```text
富力城周边内容仅保留历史兼容入口。
正式“周边推荐”不再硬编码进入该页面。
```

但实际按钮：

```text
🏙 富力城导航
```

仍然存在。

所以准确结论是：

```text
它不是产品主方向，
但当前线上仍可点击进入。
```

不能写成“已经删除”。

---

# 65. 富力城导航内容

目前是硬编码商户列表，包括：

```text
餐厅小吃
烧烤夜宵
奶茶饮品
超市便利
酒店租房
休闲生活
快递物流
富力物业
```

其中直接含：

```text
Telegram 用户名
电话号码
```

每个页面底部：

```text
💡 点击用户名即可直接联系商家
✍️ 有好店想补充，可以提交给侨联

信息会持续更新，
具体价格和服务以商家实际回复为准。
```

---

# 66. 我的租约

用户租约数据不是自动根据房源发布生成。

需要管理员录入并绑定。

绑定后用户可看到：

```text
📋 我的租约

房源与账期
房源｜
交租日｜

金额与到期
月租｜
押金｜
到期日｜
还有｜X天

当前状态
租约状态稳定 / 临近到期
🔔 到期前7天提醒：已开启
```

---

# 67. 租约提醒

默认：

```text
到期前 7 天
```

用户可切换：

```text
🔔 到期前7天提醒：已开启

或

🔕 到期前7天提醒：已关闭
```

---

# 68. 新租约页没有续租/换房按钮

代码中明确：

```text
新租约页不再生成续租/换房入口；
旧回调继续兼容历史按钮。
```

当前主页面按钮：

```text
📅 我的预约
🛠 入住后服务

🔔 / 🔕 到期提醒

💬 联系中文顾问

🏠 返回首页
```

---

# 69. 旧续租/换房 callback 仍存在

旧 callback：

```text
contract:renew
contract:renew_yes
contract:change
```

仍然有完整代码。

旧链接如果还能触发，会进入：

```text
续租
换房
顾问通知
renewal_tracking
```

因此属于：

```text
兼容逻辑
```

而不是主页面入口。

---

# 70. 租客与合同管理员后台

User Bot 本身还包含一个管理员域：

```text
/admin contracts
```

用于管理租客和合同。

后台菜单：

```text
➕ 录入租约
⏰ 即将到期

🔄 续租跟进
🔧 报修工单

🏠 返回客户首页
```

---

# 71. 录入租约流程

共 7 步：

```text
1. 客户 Telegram
2. 项目名 + 房号
3. 月租
4. 押金月数
5. 每月交租日
6. 合同开始日期
7. 合同结束日期
```

最后确认。

---

# 72. 客户绑定

管理员确认后生成类似：

```text
QL260907XXXXXX
```

的 binding code。

生成 Deep Link：

```text
/start t_bind_{code}
```

如果用户已经打开过 Bot，可以直接推送：

```text
🏠 您的租客档案已准备好

房源｜
到期｜

[ 🔗 绑定租客档案 ]
```

---

# 73. 绑定成功

用户点击后：

```text
✅ 租约档案已核对
```

然后进入：

```text
我的租约
```

---

# 74. Publisher Bot 当前首页

管理员 `/start`：

```text
🏠 侨联发布助手

采集或导入房源后，
系统会自动清洗并生成封面、文案。

采集到合格房源后，我会主动提醒你。
点“房源队列”即可查看、修改并发布。

资料不完整的房源会自动留在后台，
不会打断正常发布。
```

但当前实际菜单已经收敛为：

```text
➕ 发布房源
🔵 房态管理

📢 广播中心
📡 采集源

🧪 查看发布效果
📚 发布记录
```

---

# 75. Publisher 当前 Bot Command 菜单

当前真正设置给 Telegram 的 Bot Commands 只有：

```text
/start
/cancel
```

代码中明确会覆盖管理员私聊命令菜单，清掉历史旧命令。

所以管理员手机输入 `/` 时不会看到一大堆旧命令。

---

# 76. Publisher simple_mode

当前启动：

```text
register_autopilot_features(
    application,
    include_cancel=False,
    simple_mode=True
)
```

因此当前 simple_mode 注册的是：

```text
/pending
/send
/status
/logs
/sources
/source_add
/source_on
/source_off
/daily
/daily_on
/daily_off
/daily_time
/daily_text
/new
/intake
/intake_done
```

以及：

```text
ap:* callbacks
daily:* callbacks
管理员私聊照片
管理员私聊文字
```

---

# 77. simple_mode 当前没有注册的命令

虽然函数还在，当前 Publisher 没有通过 autopilot 注册：

```text
/ops
/help
/stats
/analytics
/pause
/resume
/slots
/check
/publish
/approve
/reject
/tpl
/tpl_use
/tpl_test
/intake_cancel
/intake_pending
/post_menu
/post_index
/pin_text
```

这点非常重要。

旧文档里如果把这些写成“当前管理员可用命令”，会误导。

---

# 78. 但是部分未注册函数仍被按钮调用

Publisher 的管理面板 callback：

```text
cmd:xxx
```

会直接调用一些 autopilot 函数。

所以：

```text
命令未注册
```

不代表：

```text
功能一定完全无法运行
```

例如后台按钮可以直接调用：

```text
daily
status
sources
logs
intake
sample preview
```

因此以后做清理时应该先统一：

```text
菜单按钮
callback route
CommandHandler
```

再删除旧函数。

---

# 79. 房态管理

Publisher 提供：

```text
🔵 房态管理
```

管理员可以看到房源并设置：

```text
active
reserved
pending
rented
inactive
```

中文对应：

```text
🟢 当前可预约
🟡 已有预约 · 仍可预约
🔵 房态待确认
🔴 已租出
⚫ 已下架
```

---

# 80. 房态变化同步频道

房态更新以后：

```text
listings.status
↓
sync_channel_listing_status()
↓
编辑原频道消息 caption
↓
编辑按钮
```

不会重新发一条房源。

所以频道不会因为房态变化不断生成重复帖。

---

# 81. 每日广播

Publisher 后台：

```text
📢 广播中心
```

当前默认：

```text
7天自动轮播
```

管理员可以：

```text
👀 预览今天
📋 查看7天安排

09:30
12:30
18:30
其他时间

临时改固定文案

开启 / 暂停每日广播
```

---

# 82. 广播开启前不会自动发

保存广播模板、时间、文字：

```text
不会立刻发送
```

必须明确开启：

```text
▶️ 开启每日广播
```

---

# 83. Weather Reminder

V2.2 的天气提醒不是预约 Bot 里临时弹出来的提醒。

它属于：

```text
频道每日广播内容系统
```

现有模板共：

```text
storm
rain_heavy
rain_light
rain_possible
hot
sunny
humid
good
stable
```

---

# 84. 天气模板示例

例如：

```text
今天有雷雨，
出门前建议看一下路况，
看房时间可以和顾问确认是否需要调整。
```

```text
今天雨势较大，
看房建议预留多一点通勤时间。
```

```text
今天气温偏高，
看房尽量避开正午时段。
```

```text
今天天气不错，
适合安排看房或出门办事。
```

---

# 85. 频道置顶导航代码

仓库仍保留统一置顶页函数。

默认文案类似：

```text
侨联地产｜金边租房频道
您在金边的自己人

这里持续更新金边真实在租房源。

怎么看：
直接往下浏览……

怎么找：
用下方索引按区域、预算或户型进入 Bot 筛选。

怎么问：
看中具体房源，点房源详情……

怎么约：
点预约看房……

频道负责看房源，
Bot 负责找房、咨询和预约。
```

按钮设计：

```text
按区域找房
按预算找房

按户型找房
最新房源

联系侨联
```

但当前 `simple_mode=True` 不注册 `/post_menu` `/post_index`。

所以它属于：

```text
仍有完整实现
但不是当前管理员默认主入口
```

---

# 86. Deep Link 兼容体系

当前 User Bot 支持多种历史链接。

新标准：

```text
property_QL-RF-K7M2_details
property_QL-RF-K7M2_photos
property_QL-RF-K7M2_book
```

同时兼容：

```text
detail__
book__
consult__
photos__
discussion_entry__
旧 listing_id suffix
旧 base36 post token
旧 l_44
旧 QC/QJ
```

这是为了让以前频道已经发出去的按钮尽量不失效。

---

# 87. Deep Link 失效

无法解析时：

```text
这个链接已经失效或房源信息已更新。

您可以重新找房，
或直接联系我们。
```

而不是报：

```text
KeyError
invalid callback
listing missing
```

---

# 88. 公开 opaque token

部分历史/冻结包架构仍使用：

```text
qlxxxxxxxxxxxxxx
```

形式的不透明 public token。

解析会：

```text
public_token
↓
publication_packages
↓
listing_id
```

还保留：

```text
public_link_registry
leads post_token mapping
```

作为发布包重建后的兼容路径。

---

# 89. 当前频道正式按钮已经改为稳定公开编号

虽然 opaque token 系统仍存在兼容支持，当前正式频道 keyboard 使用：

```text
公开 QL 编号 Deep Link
```

而不是强依赖临时 publication package token。

这是目前更稳定的链接设计。

---

# 90. User Bot 用户状态

当前主 Conversation 状态主要包括：

```text
MAIN
FIND_AREA
FIND_BUDGET
APPT_MODE
APPT_FOCUS
APPT_DATE
APPT_TIME
APPT_CONFIRM
```

注意：

```text
状态存在
≠
当前主 UI 一定显示该步骤
```

例如：

```text
APPT_MODE
APPT_FOCUS
APPT_CONFIRM
```

主要用于旧 callback / 兼容。

当前正常预约 UI 已压缩。

---

# 91. 用户自由文本

User Bot 不只是按钮 Bot。

自由文本可用于：

```text
找房需求
自定义区域
自定义预算
自定义日期
自定义时间
服务咨询
报修描述
其他区域周边需求
```

---

# 92. 用户管理员通知

以下行为会给管理员产生 lead / 通知：

```text
提交找房条件
联系我们
具体房源咨询
预约
讨论区入口点击
报修
续租
换房
生活服务需求
```

管理员通知一般自动带：

```text
Telegram 用户
@username 或 tg://user?id=
房源编号
条件
来源
```

所以租客不需要重复填联系方式。

---

# 93. 当前系统中的用户身份

优先使用：

```text
Telegram user_id
Telegram username
```

作为客户识别。

这也是为什么：

```text
预约
咨询
报修
找房
```

不再反复询问手机号。

---

# 94. 当前旧文档与真实代码冲突

以下历史文件不能再直接当作生产事实：

```text
USER_JOURNEY_CURRENT.md
FINAL_UI_COPY_FLOW_20260831.md
FINAL_PRODUCTION_STATUS.md
部分历史 V2/V2.1 文档
```

它们只能用于：

```text
产品意图
历史设计
对照差异
```

---

# 95. 典型冲突一：首页

旧文档可能写：

```text
👋 你好 {名字}，我是侨联小管家
```

当前代码实际：

```text
💎 侨联地产｜您在金边的自己人
```

---

# 96. 典型冲突二：详情

旧目标稿写：

```text
💰 费用明细
物业
水费
电费
网络
停车

⚠️ 提前说清
💬 侨联判断
```

当前生产详情主页面并不是完整按这套展示。

费用数据仍在，但 UI 已经不同。

---

# 97. 典型冲突三：更多实拍

旧文档：

```text
优先打开频道评论区
```

当前：

```text
User Bot 直接发送图片 / media group
```

---

# 98. 典型冲突四：预约

旧文档可能还有：

```text
方式
关注点
日期
时间
确认
```

当前：

```text
日期
时间
直接提交
```

视频通过日期页切换。

---

# 99. 典型冲突五：公开编号

旧文档仍大量使用：

```text
QC001
QC0350
```

当前新标准：

```text
QL-RF-K7M2
```

QC 只留兼容。

---

# 100. 典型冲突六：评论区

仓库存在大量 discussion 相关代码。

但当前 patch 明确禁用新帖评论区自动内容。

所以不能看到：

```text
send_discussion_three_segments
```

就认为生产还在使用。

---

# 101. 目前架构上的主要问题

## P1：历史代码残留过多

同一功能存在：

```text
旧函数
新函数
patch
compat callback
历史命令
未注册命令
```

导致维护时非常容易误判。

---

## P1：产品文档没有真正跟上生产代码

历史文件仍写：

```text
唯一产品真源
```

但已经与代码发生冲突。

以后必须明确：

```text
PRODUCTION_CURRENT.md
```

只由生产 SHA 更新。

---

## P1：Publisher 两套管理思路混杂

当前同时存在：

```text
v2 Publisher class
autopilot_publish_bot
review_queue_patch
publication_package
meihua_publisher
```

功能上已经统一，但源码组织仍然碎。

---

## P1：Simple Mode 与旧命令函数并存

最容易让 Coding AI 做错：

```text
看到 cmd_approve()
→ 以为生产支持 /approve
```

实际上 simple_mode 没注册。

应该以后在代码里给 legacy function 加明显注释或移至 legacy 模块。

---

## P2：富力城周边导航属于历史残留但仍公开可达

代码说：

```text
历史兼容
```

按钮却仍显示：

```text
🏙 富力城导航
```

产品定义和实际 UI 不完全一致。

---

## P2：部分旧提示文案仍存在

例如 Publisher 内还有：

```text
A/B/C 三版任选一版预览
/approve
/send
```

而当前新审核 UI 已经主要变成：

```text
选封面
确认封面
生成发布键
```

说明部分后台说明文字没有一起完成收口。

---

# 102. 推荐的代码清理方向

不要直接大删。

正确顺序：

```text
第一阶段：锁生产行为
第二阶段：建立入口矩阵
第三阶段：迁移 legacy
第四阶段：删除死代码
```

---

# 103. 建议建立入口矩阵

每个功能标记：

```text
模块
Handler
入口
callback
当前可达
兼容可达
未注册
是否安全删除
```

例如：

| 功能 | 当前主入口 | 兼容 | 未注册 |
|---|---|---|---|
| 用户首页 | 是 | - | - |
| 预约关注点 | 否 | 是 | - |
| 预约确认页 | 否 | 是 | - |
| Publisher /approve | 否 | - | 是 |
| 富力城导航 | 否 | 是且可点 | - |
| discussion 自动发图 | 否 | patch 禁用 | - |

---

# 104. 推荐目录重构目标

最后可以收敛成：

```text
qiaolian/
├── collector/
│   ├── telegram.py
│   └── sources.py
│
├── parser/
│   ├── parser.py
│   ├── canonical.py
│   └── taxonomy.py
│
├── publishing/
│   ├── package.py
│   ├── cover.py
│   ├── caption.py
│   └── publisher.py
│
├── user_bot/
│   ├── home.py
│   ├── search.py
│   ├── listing.py
│   ├── appointment.py
│   ├── service.py
│   └── contract.py
│
├── admin_bot/
│   ├── intake.py
│   ├── review.py
│   ├── status.py
│   ├── broadcast.py
│   └── sources.py
│
├── legacy/
│   ├── old_callbacks.py
│   ├── old_qc_ids.py
│   └── old_deeplinks.py
│
└── tests/
```

---

# 105. 当前生产主流程最终简图

```text
┌─────────────────────────┐
│ Telegram / 微信管理员导入 │
└────────────┬────────────┘
             ↓
       source_posts
             ↓
       media_assets
             ↓
         AI Parser
             ↓
    canonical_facts.v1
             ↓
           drafts
             ↓
       质量 / 媒体门禁
             ↓
      管理员正式预览
             ↓
       选择封面模板
             ↓
       publication package
             ↓
          approved
             ↓
       MeihuaPublisher
             ↓
     Telegram 房源频道
             ↓
    ┌────────┼────────┐
    ↓        ↓        ↓
 房源详情   更多实拍   预约看房
    ↓        ↓        ↓
         User Bot
             ↓
 ┌───────────┼────────────┐
 ↓           ↓            ↓
找房         咨询          预约
 ↓           ↓            ↓
Lead        顾问通知      appointment
                          ↓
                     自动房态同步
                          ↓
                       频道更新
```

---

# 106. 当前租后主流程

```text
管理员录入租约
      ↓
tenant binding
      ↓
发送绑定链接
      ↓
用户绑定
      ↓
我的租约
      ↓
├─ 到期提醒
├─ 我的预约
├─ 入住服务
├─ 报修
├─ 物业协调
└─ 联系顾问
```

---

# 107. 当前房态闭环

```text
房源发布
↓
active
↓
0份预约
🟢 当前可预约

1-4份有效预约
↓
reserved
↓
🟡 已有预约 · 仍可预约

5份及以上
↓
pending
↓
🔵 已有5份预约看房，房态待确认
↓
频道自动移除「预约看房」

管理员确认
↓
active / reserved / rented / inactive
↓
同步编辑原频道帖
```

---

# 108. 当前生产应该认定的唯一核心数据链

以后无论 Claude、Codex、Grok 还是其他 Coding AI，涉及房源发布都应该遵守：

```text
Source
↓
Canonical Facts
↓
Draft
↓
Listing
↓
Frozen Publication Package
↓
Approved
↓
Publisher
↓
Telegram
↓
posts
↓
User Bot
```

任何尝试：

```text
绕过 canonical
直接拿 raw text 发
直接从 draft 现场重写
跳过 package
pending 直接发布
拿旧 QC 当唯一主键
```

都不应该作为新的生产实现。

---

# 109. 当前生产冻结原则

后续修改代码时至少守住：

```text
1. 不修改已有生产数据
2. 不重建数据库
3. 不重抓 Telegram
4. 不重新下载已有媒体
5. 不绕过 canonical facts
6. 不绕过 publication package
7. 不允许 pending 未审核房源直接发布
8. 不允许重复发布同一房源
9. 不让内部 l_x 暴露给用户
10. 旧 Deep Link 必须尽量兼容
11. 房态变化编辑原帖，不重复发帖
12. 管理员手工终态优先
```

---

# 110. 本次审计最终判定

生产 SHA：

```text
8e4605cf5cc21dfec3ce30729654b09e39de9abf
```

当前整体架构已经具备：

```text
采集
解析
canonical
审核
冻结包
发布
频道
User Bot
找房
实拍
咨询
预约
自动房态
保障
入住服务
租约
报修
每日广播
管理员后台
```

真正的问题已经不是“功能完全缺失”，而是：

```text
新生产流程已经形成，
但仓库仍混着很多历史实现、兼容实现和旧文档，
导致任何 AI 打开仓库后都容易认错主线。
```

下一阶段最应该做的不是继续加功能，而是：

```text
锁定 8e4605cf 当前真实行为
↓
建立生产入口矩阵
↓
把 legacy 搬出主模块
↓
删除确认不可达死代码
↓
重写唯一 CURRENT_PRODUCTION_FLOW.md
↓
以后所有 Coding AI 只看这份文档 + 当前生产测试
```

这样才能真正把现在“能跑但很乱”的仓库，整理成后续可以长期维护的生产项目。

---

# 附录 A｜最重要的生产事实速查

```text
生产 SHA
8e4605cf5cc21dfec3ce30729654b09e39de9abf

当前新公开编号
QL-RF-K7M2 格式

旧编号
QC / QJ 只兼容

频道房源帖
1张封面 + 3按钮

频道按钮
房源详情
更多实拍
预约看房

更多实拍
User Bot 发图片，不走评论区

预约
日期 → 时间 → 直接提交

默认看房方式
实地

视频看房
日期页切换

联系方式
自动使用 Telegram

预约锁房
1–4 reserved
5+ pending

当前 User Bot 首页
帮我找房
我的预约
侨联保障
入住服务
房源频道
联系我们

Publisher 首页
发布房源
房态管理
广播中心
采集源
发布效果
发布记录

Publisher 模式
simple_mode=True

新房源发布要求
canonical
有效位置
有效租金
户型
房型
至少4张原图
人工审核
approved publication package

评论区自动实拍
当前禁用

天气提醒
每日广播系统

租约提醒
到期前7天
```

---

# 附录 B｜必须避免继续传播的旧结论

以下说法目前不能再作为生产事实：

```text
“新房源编号就是 QC001”
错误

“更多实拍在频道评论区”
错误

“频道主帖是四图相册”
错误

“预约先选实地/视频”
当前主流程错误

“预约要选验房关注点”
当前主流程错误

“预约最后还要确认一次”
当前主流程错误

“预约要用户填手机号/微信”
错误

“详情一定显示物业水电网络停车”
当前主 UI 错误

“/approve 是当前 Publisher 正式命令”
simple_mode 下错误

“仓库里的 FINAL_UI_COPY_FLOW 就是当前生产真源”
错误

“有函数就代表线上在用”
错误
```

---

**生产事实以 SHA `8e4605cf5cc21dfec3ce30729654b09e39de9abf` 的真实 Handler、callback、运行配置和测试契约为准。**