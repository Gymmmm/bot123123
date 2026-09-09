# Candidate runtime port

This directory adds only deployment/runtime isolation around the existing branch. It does not replace the branch's business data flow. Collector/Publisher/User continue to use the existing `source_posts -> drafts -> listings -> posts` database model and existing business modules.

## Isolated layout

Recommended server layout:

```text
/opt/qiaolian_candidate_runtime/
  repo/          # this branch checkout
  .venv/         # Python 3.12 venv
  runtime.env    # candidate-only credentials and paths
  state/
    data/qiaolian_dual_bot.db
    media/
    telethon_sessions/
```

Nothing here changes the existing production systemd units.

## Prepare

```bash
python3.12 -m venv /opt/qiaolian_candidate_runtime/.venv
/opt/qiaolian_candidate_runtime/.venv/bin/pip install -r /opt/qiaolian_candidate_runtime/repo/requirements.txt
cp deploy/candidate/runtime.env.example /opt/qiaolian_candidate_runtime/runtime.env
```

Fill only test/candidate credentials in `runtime.env`.

## Empty database recovery

Explicit preflight:

```bash
set -a
. /opt/qiaolian_candidate_runtime/runtime.env
set +a
/opt/qiaolian_candidate_runtime/.venv/bin/python run_candidate_preflight.py --initialize
/opt/qiaolian_candidate_runtime/.venv/bin/python run_candidate_preflight.py
```

The bootstrap is idempotent. It creates the missing collector/publisher/user tables needed by this branch, including `source_posts`, `drafts`, `media_assets`, `listings`, `posts`, `publication_packages` and delivery state. Existing rows are not deleted or replaced.

For disposable candidate testing, `CANDIDATE_AUTO_BOOTSTRAP=1` lets the service launcher recover an empty DB before starting. Leave it disabled for production cut-over until explicitly approved.

## Start all three candidate bots

Install the template under a candidate-only unit name, then:

```bash
systemctl start qiaolian-candidate@collector
systemctl start qiaolian-candidate@publisher
systemctl start qiaolian-candidate@user
```

Or run directly:

```bash
python run_candidate_runtime.py collector
python run_candidate_runtime.py publisher
python run_candidate_runtime.py user
```

## Test gate before merge

A candidate is mergeable only after:

1. branch CI passes;
2. empty-db preflight returns `ok: true`;
3. Collector can persist real `source_posts` and create `drafts`/media rows;
4. Publisher can consume the same DB and write package/post state;
5. User Bot reads the same `listings` projection;
6. the three candidate services run simultaneously without using production DB/media/session paths.

Do not merge or repoint production systemd units until these checks finish.
