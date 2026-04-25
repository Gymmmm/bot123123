# Qiaolian Pipeline

主线目标只保留三层：

1. `collect`：采集源频道、落库原始内容和媒体
2. `operate`：解析、封面、排队、频道自动运营
3. `retain`：Bot User 承接、留资、预约、召回

当前第一批重构先做 `operate` 收口：

- 用统一解析器替换占位 `ai_parser`
- 让 `drafts` 至少具备可运营的基础字段
- 后续继续把质量打分、自动筛选、用户标签沉淀并到同一条主线

