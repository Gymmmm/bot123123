-- 侨联发布自动化：扩展 drafts + Bot 用表（在现有库上执行一次）
-- 若列已存在会报错，可忽略该行。
-- review_status 约定：
--   pending   = 已结构化/可带封面，待人预览（预览池）
--   ready     = 已加入定时发布队列（ready_queue）
--   paused    = 暂停出队（仍在库内，定时器跳过）
--   rejected  = 人工丢弃
--   published = 已发频道（与现 meihua_publisher 一致）
--   approved  = 旧流水线保留兼容；新逻辑优先用 pending/ready

ALTER TABLE drafts ADD COLUMN queue_score REAL DEFAULT 0;
ALTER TABLE drafts ADD COLUMN preview_msg_chat_id TEXT;
ALTER TABLE drafts ADD COLUMN preview_msg_id TEXT;

CREATE INDEX IF NOT EXISTS idx_drafts_ready_queue
  ON drafts (review_status, queue_score, id)
  WHERE review_status = 'ready';

CREATE INDEX IF NOT EXISTS idx_drafts_pending_preview
  ON drafts (review_status, id)
  WHERE review_status = 'pending';

CREATE TABLE IF NOT EXISTS bot_settings (
  setting_key   TEXT PRIMARY KEY,
  setting_value TEXT,
  updated_at    TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS admin_actions (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  operator_id   TEXT,
  action        TEXT,
  target_type   TEXT,
  target_id     TEXT,
  payload       TEXT,
  created_at    TEXT DEFAULT CURRENT_TIMESTAMP
);
