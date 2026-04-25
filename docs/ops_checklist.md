# Ops Checklist

## Verify Health

```bash
systemctl status qiaolian-user-bot.service
systemctl status qiaolian-publisher-bot.service
systemctl status qiaolian-collector.service
journalctl -u qiaolian-user-bot.service -n 50 --no-pager
journalctl -u qiaolian-publisher-bot.service -n 50 --no-pager
journalctl -u qiaolian-collector.service -n 50 --no-pager
ps -ef | grep -E 'run_user_bot.py|run_publisher_bot_v2.py|collector_bot.py|autopilot_publish_bot.py' | grep -v grep
```

## Verify Code

```bash
python3 -m py_compile run_user_bot.py qiaolian_dual/user_bot.py v2/run_publisher_bot_v2.py collector_bot.py run_pipeline_autopilot.py
python3 - <<'PY'
import run_user_bot
import qiaolian_dual.user_bot
print("import_ok")
PY
cd v2 && python3 - <<'PY'
from run_publisher_bot_v2 import main
print("v2_import_ok")
PY
```

## Safe Restart

```bash
sudo systemctl restart qiaolian-user-bot.service
sudo systemctl restart qiaolian-publisher-bot.service
sudo systemctl restart qiaolian-collector.service
```

## Common Checks

- Duplicate polling suspicion: check `ps` and `journalctl` for `getUpdates` conflicts.
- Deep link not writing lead: confirm `USER_BOT_USERNAME` and test `consult__<post_token>__<listing_id>`.
- Publisher not sending: check `qiaolian-publisher-bot.service`, `ready` queue, and `journalctl`.
- Collector not advancing: verify `qiaolian-collector.service`, source channel access, and `source_posts` growth.
- Cover not updated on server: compare `/opt/qiaolian_dual_bots/cover_generator.py` and `v2_admin/house_cover_v2.py` against local and rerun `py_compile`.
