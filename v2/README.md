
# 侨联地产发布 Bot v2

这是重做后的发布 Bot，专门解决旧版的两个问题：

- 交互太长、像表单机
- 最后一步才发现没有频道权限

## 这版做了什么

- `/start` 进入主菜单
- `/test_channel` 或按钮先做频道权限检测
- `/new` 开始 6 步快速发帖
- 支持封面图 / 视频
- 可跳过非必要字段
- 发布前预览
- 可修改标题 / 价格 / 区域 / 亮点 / 费用 / 顾问提醒
- 发布时自动加 4 个引流按钮，且 deep link 与生产用户 Bot 兼容，旧短链仍可识别：
  - 立即咨询
  - 预约看房
  - 收藏房源
  - 同区域更多

## 启动

```bash
pip install -r requirements.txt
cp .env.example .env
python run_publisher_bot_v2.py
```

## 关键环境变量

- `PUBLISHER_BOT_TOKEN`
- `USER_BOT_USERNAME`
- `CHANNEL_ID`
- `ADMIN_IDS`
- `SQLITE_PATH`

## 推荐工作流

1. 先点“检查频道权限”
2. 再点“新建房源”
3. 先发封面图
4. 只填核心字段
5. 预览后发布

## 参考你给我的资料，真正用上的地方

- `qiaolian-tg-generator.html`：编号体系、底部按钮、主帖/预览/按钮代码分层
- `tg-listing-generator555.html`：表单字段组织、帖子生成器思路
- `tg_rental_kit.html` 与 `rental_post_template.html`：更强的卡片结构、价格块、优缺点、顾问说
- `小彭看房水印工具.html`：适合作为独立 H5 工具，不应塞进 Bot 主流程
