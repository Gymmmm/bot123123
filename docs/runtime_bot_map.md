# Runtime Bot Map (Production)

Server: `/opt/qiaolian_dual_bots`

## Active services (systemd)

- `qiaolian-user-bot.service`
  - Entry: `/opt/qiaolian_dual_bots/run_user_bot.py`
  - Runtime module: `/opt/qiaolian_dual_bots/qiaolian_dual/user_bot.py`
  - Telegram: `@XxxXiaopengbot`

- `qiaolian-publisher-bot.service`
  - Entry: `/opt/qiaolian_dual_bots/v2/run_publisher_bot_v2.py`
  - Runtime module: `/opt/qiaolian_dual_bots/v2/qiaolian_publisher_v2/bot.py`
  - Telegram: `@Meihua666bot`

- `qiaolian-collector.service`
  - Entry: `/opt/qiaolian_dual_bots/collector_bot.py`

- `qiaolian-admin-web.service`
  - Entry: `waitress admin_server:app` (cwd `/opt/qiaolian_dual_bots/v2_admin`)

## Non-service helper

- `/opt/qiaolian_dual_bots/autopilot_publish_bot.py` (v2 helper, not a standalone service)
