# PR114 controlled release (preparation only; no deployment performed)

## Official immutable release entry

`.github/workflows/qiaolian-production-deploy.yml` accepts a full 40-hex
`deploy_sha` from a feature commit; it does not merge, push, require master
ancestry, or deploy a moving branch HEAD. `--ref` selects the reviewed workflow
branch/tag; `deploy_sha` selects the exact tested application release. Manual
dispatch additionally requires the event workflow SHA to equal deploy_sha, failing
closed before SSH if the branch moved. gh --ref accepts a branch/tag, not a raw
commit SHA. No GitHub
`environment:` is currently bound to this job. Do not invent a bypass if repository
protection changes. Concurrency remains `qiaolian-v3-production`, no cancellation.

All dispatch inputs:

- `confirm`: `CANCEL` (default) or `DEPLOY`; only DEPLOY runs the job.
- `deploy_sha`: required full immutable commit SHA.
- `hold_publisher`: boolean, default **true**. False explicitly restores normal
  scheduled publishing; it does NOT mean “send just one post”.

After the parent separately approves a release and checks that exact SHA's checks:

```sh
# Replace the placeholder with the reviewed final 40-character SHA.
gh workflow run qiaolian-production-deploy.yml -R Gymmmm/bot123123 \
  --ref feature/prod-final-gate-20261107 -f confirm=DEPLOY -f deploy_sha=<FINAL_SHA> \
  -F hold_publisher=true
```

The original `deploy-control` push trigger and exact three-field request contract
remain; that path now defaults to publisher held. No control commit was created.
The complete `python -m pytest -q tests/v3_core` gate, compilation, actual
Tesseract eng/chi_sim dependencies, host pin and SSH identity are unchanged.
Tests run before SSH. A green V3 job does **not** make the unrelated full-suite
job green or satisfy any newly configured branch protection by itself.

## Held deployment / recovery semantics

The existing publisher schedules property posting after 3 seconds and broadcast
after 5 seconds. Production's three publishing switches were all true at the last
readonly audit; restarting all four services without a hold is not a one-post test.

Held deployment creates a root-only atomic hold marker recording the release SHA
and original switch booleans; it never changes those DB settings. It stops only
publisher before switching the release symlink. Collector/canonical/user restart
against the new SHA and must pass their original health checks; publisher must be
inactive with MainPID=0. Existing queues/21 unknown deliveries are never cleared,
replayed, or reclassified. A held publisher is intentionally NOT a live accepted
publisher. Status-sync jobs hosted in that service also wait while it is stopped.

To resume after a separate explicit decision, use the same official workflow,
**the same SHA**, and `-F hold_publisher=false`. All tests/backup/preflight run again.
An inactive publisher is permitted only with a matching hold marker and the other
three services active. A failed resume rolls back with publisher still held.
Successful resume restarts four services and removes only its own hold marker.
This re-enables normal scheduling with the original settings, potentially more
than one post; do not describe it as a bounded single-post operation.

## Backup and rollback

The official workflow uses SQLite online backup into
`/opt/qiaolian_backups/qiaolian_before_<SHA>_<UTC timestamp>.db`, validates integrity
and quick checks, and sets root:qiaolianbot 0640 before activating code.

Its failure trap switches `/opt/qiaolian_v3/current` back to the recorded old
release and restarts the appropriate services; a held deployment/failed resume
keeps publisher stopped. It never restores an old database over new appointments.
Logs say rollback *attempted* and require the following readonly confirmation.
Do NOT use `deploy/v3/one_shot_remote_deploy.sh`: that historical helper restores
an old DB during rollback and can overwrite newly created appointments.

There is no independently dispatched late code-only rollback action today.
Do not pretend redeploying old `1a7c4c44...` is guaranteed to pass its own complete
V3 tests. The in-job automatic rollback handles immediate failures; a failure found
later needs the existing authorized host operator to perform the same fixed
code-only symlink rollback, or a separately reviewed bounded rollback operation.
Never improvise secret extraction or a generic remote-command workflow.

## Readonly verification / preview

```sh
gh workflow run v3-readonly-runtime-audit-20261106.yml \
  -R Gymmmm/bot123123 --ref feature/prod-final-gate-20261107
```

This existing fixed-host audit uses the original host pin and secret only on the
GitHub runner/production host. No token reaches the box. It reports current SHA,
four service ActiveState/expected ExecStart/PID presence/restart counts, DB fixed
path identity, quick check, OCR eng/chi_sim, switches as booleans, queue aggregates,
and only error-line counts from at most 500 recent journal lines (never raw logs).
It runs the *currently deployed*, statically readonly preflight with `-B`, without
`--initialize`, filters out all details, and labels it **not candidate preflight**.
A candidate preflight is additionally required by the official deployment before
activation. Any missing binary/config/dependency is a failure, not an assumed pass.

The same audit may read configured public channel identity via Telegram `getChat`
(read only, never send). It exports at most one current approved, unpublished rent
package with matching canonical revision and approved review. It globally queries
approved packages rather than treating queue=0 as no candidates. It excludes
published, sending, unknown/sent/uncommitted and exception candidates, rejects
contact-bearing captions, verifies existing frozen image hashes and fixed media
roots, and exports only public caption/IDs/revision/relative image paths. No new
cover/image generation or image download is performed. `none_eligible` and
`no_safely_exportable_candidate` are distinct. Preview is a snapshot, not a send
reservation; the real publisher must revalidate immediately before any approved send.

Abort/retain hold and inspect if SHA differs, any required service is unhealthy or
restarting, OCR/preflight/DB identity/quick check fails, new errors appear, or sending/
unknown counts increase unexpectedly. Compare unknown=21 as an observed baseline,
not a hard-coded excuse to ignore additional uncertain sends. Do not restore DB,
clear queues, post, or create fictitious bookings as part of an audit. User phone
acceptance and new-SHA real-channel acceptance are still separate, unperformed steps.
