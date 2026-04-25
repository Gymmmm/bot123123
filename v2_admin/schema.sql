-- 侨联地产 · 频道发布系统 · 数据库结构
-- 兼容现有 SQLite，追加表，不动原有表结构

CREATE TABLE IF NOT EXISTS listings (
    listing_id       TEXT PRIMARY KEY,
    type             TEXT DEFAULT '公寓',        -- 公寓 / 别墅 / 商铺 / 办公室
    area             TEXT,                        -- BKK1 / Tonle Bassac / 7makara 等
    project          TEXT,                        -- 楼盘/小区名
    title            TEXT,                        -- 标题（用于内部标注）
    price            TEXT,                        -- $650
    layout           TEXT,                        -- 1房1厅1卫
    size             TEXT,                        -- 52㎡
    deposit          TEXT,                        -- 押1付1
    contract_term    TEXT,                        -- 6个月起租
    available_date   TEXT,                        -- 即可入住 / 2025-06-01
    tags             TEXT,                        -- 逗号分隔：拎包入住,泳池,健身房
    highlights       TEXT,                        -- 亮点，逗号分隔，最多4条
    cost_notes       TEXT,                        -- 水电物业说明
    advisor_comment  TEXT,                        -- 顾问点评（内部踩盘评价）
    drawbacks        TEXT,                        -- 缺点提醒（提前说清楚）
    images           TEXT DEFAULT '[]',           -- JSON 列表：本地路径 或 TG file_id
    cover_image      TEXT,                        -- 封面图（images[0] 即可）
    status           TEXT DEFAULT 'draft',        -- draft / published / rented / offline
    created_at       TEXT DEFAULT (datetime('now','localtime')),
    updated_at       TEXT DEFAULT (datetime('now','localtime'))
);

CREATE TABLE IF NOT EXISTS channel_posts (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    listing_id          TEXT NOT NULL,
    channel_id          TEXT NOT NULL,
    media_group_id      TEXT,                     -- TG media_group_id
    media_message_ids   TEXT DEFAULT '[]',        -- JSON 列表：各图片消息 message_id（用于重发时删除）
    button_message_id   INTEGER,                  -- 按钮消息 message_id
    file_ids            TEXT DEFAULT '[]',        -- JSON 列表：TG file_id（图片上传后复用）
    status              TEXT DEFAULT 'published', -- published / offline / rented
    published_at        TEXT DEFAULT (datetime('now','localtime')),
    FOREIGN KEY(listing_id) REFERENCES listings(listing_id)
);

CREATE INDEX IF NOT EXISTS idx_channel_posts_listing ON channel_posts(listing_id);
CREATE INDEX IF NOT EXISTS idx_listings_status ON listings(status);
CREATE INDEX IF NOT EXISTS idx_listings_created ON listings(created_at DESC);
