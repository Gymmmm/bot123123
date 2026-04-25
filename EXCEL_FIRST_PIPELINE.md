# Excel First 发布链路（采集→整理→封面→发布）

目标：先把房源采集成 Excel，可人工整理文案和封面尺寸，再进入发布队列。

## 1) 迁移数据库表

```bash
cd /Users/a1/projects/qiaolian_dual_bots_local
python3 scripts/migrate_excel_pipeline_v1.py
```

新增表：

- `excel_intake_batches`：一次 Excel 导入批次
- `excel_listing_rows`：每一行房源及封面规格
- `cover_render_jobs`：封面渲染任务
- `publish_queue_v2`：审核后发布队列

## 2) Excel 导入（保持旧链路兼容）

```bash
python3 tools/property_intake.py --house-csv current
```

导入后会做双写：

- 旧链路：写 `source_posts`（用于现有 parser / draft / publish）
- 新链路：写 `excel_intake_batches` + `excel_listing_rows`

## 3) Excel 字段建议

最低可用列：

- `title`
- `area`
- `type`
- `price`
- `image_cover`
- `image2`
- `image3`
- `image4`

封面规格可选列：

- `cover_w`（默认 800）
- `cover_h`（默认 600）
- `cover_kind`（默认 `right_price`）

## 4) 下一步（可继续开发）

- 将 `cover_generator` 读取 `excel_listing_rows.desired_cover_w/h/kind` 生成封面
- 审核通过后写 `publish_queue_v2`，由发布 bot 按队列发送
- 发布成功回写 `excel_listing_rows.publish_status='published'`
