# Final Runtime Map

## Production Flow

```text
Telegram channel
  -> publisher bot
  -> Bot A: run_user_bot.py -> qiaolian_dual/user_bot.py
  -> human advisor
  -> tenant services inside qiaolian_dual/user_bot.py

Support:
collector_bot.py -> run_pipeline_autopilot.py -> ready queue -> publisher bot
v2_admin/admin_server.py + v2_admin/publisher.py -> admin web support
```

## Service Map

| Service | Entry script | Working directory | Role |
| --- | --- | --- | --- |
| `qiaolian-user-bot.service` | `run_user_bot.py` | `/opt/qiaolian_dual_bots` | Bot A, lead capture, booking, ask-about-listing, tenant services |
| `qiaolian-publisher-bot.service` | `v2/run_publisher_bot_v2.py` | `/opt/qiaolian_dual_bots/v2` | Publisher / management bot |
| `qiaolian-collector.service` | `collector_bot.py` | `/opt/qiaolian_dual_bots` | Telegram source collector |

## Active Files

- `run_user_bot.py`
- `qiaolian_dual/user_bot.py`
- `v2/run_publisher_bot_v2.py`
- `v2/qiaolian_publisher_v2/bot.py`
- `collector_bot.py`
- `run_pipeline_autopilot.py`
- `v2_admin/admin_server.py`
- `v2_admin/publisher.py`

## Legacy / Compatibility

- `autopilot_publish_bot.py` is a legacy helper referenced by the v2 path only.
- Legacy deep-link payloads remain compatible in production.

## Deep-Link Summary

Preferred production payloads:

- `consult__<post_token>__<listing_id>`
- `appoint__<post_token>__<listing_id>`
- `more`

Legacy compatibility still accepted:

- `consult_<listing_id>`
- `appoint_<listing_id>`
- `fav_<listing_id>`
- `more_<area>`

