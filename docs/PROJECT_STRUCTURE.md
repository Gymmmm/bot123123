# Project Structure (Quick View)

Server root: `/opt/qiaolian_dual_bots`

## Module Tree

```text
qiaolian_dual_bots/
├── user-bot/
│   ├── run_user_bot.py
│   └── qiaolian_dual/
│       ├── user_bot.py
│       ├── db.py
│       ├── messages.py
│       └── config.py
│
├── publisher/
│   ├── v2/run_publisher_bot_v2.py
│   ├── v2/qiaolian_publisher_v2/
│   │   ├── bot.py
│   │   ├── formatters.py
│   │   ├── keyboards.py
│   │   ├── db.py
│   │   └── config.py
│   ├── meihua_publisher.py
│   ├── autopilot_publish_bot.py
│   └── cover_generator.py
│
├── collector/
│   └── collector_bot.py
│
├── admin-web/
│   └── v2_admin/
│       ├── admin_server.py
│       ├── db.py
│       ├── publisher.py
│       ├── house_cover_v2.py
│       ├── templates/
│       └── static/
│
├── pipeline/
│   ├── run_pipeline_autopilot.py
│   ├── qiaolian_pipeline/parser.py
│   └── scripts/houses_csv_pipeline.py
│
├── tools-and-ops/
│   ├── scripts/
│   │   ├── dev/                # check/backfill/fetch/preview/login 等开发脚本
│   │   └── ops/                # migration/pin 等一次性运维脚本
│   ├── tools/
│   ├── docs/
│   └── analytics/
│
├── tests/
│   └── test_*.py
│
└── runtime-data/
    ├── data/
    ├── media/
    ├── logs/
    └── reports/
```

## What To Open First

- User flow: `qiaolian_dual/user_bot.py`
- Channel publish flow: `v2/qiaolian_publisher_v2/bot.py`
- Publish engine: `meihua_publisher.py`
- Collector flow: `collector_bot.py`
- Admin web: `v2_admin/admin_server.py`

## Runtime Entry Points (systemd)

- `qiaolian-user-bot.service` -> `run_user_bot.py`
- `qiaolian-publisher-bot.service` -> `v2/run_publisher_bot_v2.py`
- `qiaolian-collector.service` -> `collector_bot.py`
- `qiaolian-admin-web.service` -> `v2_admin/admin_server.py`
