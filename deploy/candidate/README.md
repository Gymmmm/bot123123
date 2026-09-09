# V3 candidate deployment

This directory is for side-by-side candidate testing only. It does not replace production systemd units.

## Layout

- repository: `/opt/qiaolian_v3_candidate/repo`
- venv: `/opt/qiaolian_v3_candidate/.venv`
- runtime root: `/opt/qiaolian_v3_candidate/runtime`
- env: `/opt/qiaolian_v3_candidate/runtime.env`
- database: `/opt/qiaolian_v3_candidate/runtime/data/qiaolian_v3_candidate.db`

All mutable paths are required to remain under `QIAOLIAN_RUNTIME_ROOT`.

## Prepare

```bash
mkdir -p /opt/qiaolian_v3_candidate
cd /opt/qiaolian_v3_candidate
git clone <repo> repo
cd repo
git checkout integration/v3-core-aftercare-runtime-hardening-20260909
python3.12 -m venv ../.venv
../.venv/bin/pip install -r requirements.txt
cp deploy/candidate/runtime.env.example ../runtime.env
```

Fill candidate Telegram credentials in `runtime.env`.

## Initialize / recover empty database

Initialization is explicit and V3-native. It creates only V3 tables, including aftercare/operations tables; it does not create legacy `drafts`, `listings`, `posts`, or legacy publication tables.

```bash
set -a
. /opt/qiaolian_v3_candidate/runtime.env
set +a
/opt/qiaolian_v3_candidate/.venv/bin/python run_candidate_preflight.py --initialize --component all
```

If the database already exists, a timestamped copy is written under the candidate backup directory before initialization.

## Start candidate services

Copy `qiaolian-v3-candidate@.service` to `/etc/systemd/system/`, run `systemctl daemon-reload`, then start only candidate units:

```bash
systemctl start qiaolian-v3-candidate@collector
systemctl start qiaolian-v3-candidate@worker
systemctl start qiaolian-v3-candidate@publisher
systemctl start qiaolian-v3-candidate@user
```

Do not stop or alter production services for this test.

## Acceptance sequence

1. preflight reports `ok=true`, no missing V3 tables, SQLite quick_check `ok`;
2. collector logs into Telegram and persists real source posts/media;
3. canonical worker produces `listings_v3` / `listing_offers` and review state;
4. publisher creates a frozen package and `publication_instances` receipt without legacy tables;
5. user bot reads only published V3 inventory;
6. aftercare case is linked to `listing_id` / `offer_id` / `publication_instances.instance_id`;
7. repair ticket can create one idempotent operations task and case/task event history is durable.

## Rollback

Candidate rollback is simply stopping the four candidate units and removing the candidate runtime directory. Production systemd units and production database are outside this layout and are not touched by these files.
