# 微信笔记导入口（直接粘贴）

你这边有房源在微信笔记里时，可直接走这个口子：

- 脚本：`tools/wechat_note_bridge.py`
- 作用：把微信笔记原文直接写进 `source_posts`，并同步写 `excel_listing_rows`（用于后续封面尺寸控制和发布队列）

## 用法 1：直接粘贴（推荐）

```bash
cd /Users/a1/projects/qiaolian_dual_bots_local
python3 tools/wechat_note_bridge.py --images "/path/a.jpg|/path/b.jpg"
```

执行后把微信笔记整段贴进去，结束按 `Ctrl+D`。

## 用法 2：从文本文件导入

```bash
python3 tools/wechat_note_bridge.py \
  --text-file "/Users/a1/Desktop/wechat_note.txt" \
  --images "/path/a.jpg|/path/b.jpg|/path/c.jpg" \
  --cover-w 800 \
  --cover-h 600 \
  --cover-kind right_price
```

## 可识别字段（自动抽取）

- 标题（默认取首行）
- 位置/区域
- 户型
- 租金（月租）
- 押付方式
- 合同期
- 面积
- 配置/家具
- 联系方式（Telegram/微信/电话）

## 导入后落库位置

- `source_posts`：原始文本与图片
- `excel_intake_batches`：导入批次
- `excel_listing_rows`：标准化行数据 + 封面尺寸配置

## 建议流程

微信笔记 -> `wechat_note_bridge.py` -> `run_pipeline_autopilot.py` -> `autopilot_publish_bot`

这样就实现了“先采集（微信笔记）再整理再发布”。
