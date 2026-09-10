# Window D HANDOFF — Issue #25 appointment date-first flow

- Window: D (`agent/c07c08c09-appointment-flow-D`)
- Baseline: `90180b834c5b13cb5e96aa51b24a081956bb18d2`
- Deployed: no
- Official channel / official bot messages: none
- Production DB: untouched
- Did not cherry-pick PR #51 / `codex/product-copy-lock-90180b83-20260910`

## Done in allowed files

Book entry is date-first (offline default). Video is an optional same-screen control (`🎥 实时视频看房`), not a required mode/focus page.

Flow owned by this window:

```
book → date → time → confirm → submit → ✅ 预约申请已提交
video date → time → confirm → submit
```

Confirm is now a real step. Time no longer persists.

Success copy is locked to `✅ 预约申请已提交` (never `预约成功`). Buttons:

- `📅 我的预约` → `v3u:home:appointments`
- `🏠 继续看房` → `v3u:t:home`
- `💬 联系中文顾问` → `v3u:home:contact`

## Tests run

```
python -m compileall <allowed production files>
PYTHONPATH=. pytest -q \
  tests/v3_core/test_user_bot_public_appointment.py \
  tests/v3_core/test_user_bot_transition_views.py \
  tests/v3_core/test_user_bot_transition_plan.py \
  tests/v3_core/test_user_bot_telegram_transition_ui.py \
  tests/v3_core/test_user_bot_transition_actions.py \
  tests/v3_core/test_appointment_submit_executor_v3.py \
  tests/v3_core/test_user_bot_telegram_appointment_submit.py
```

Result: **46 passed**.

## Out-of-boundary wiring for Window E

Do **not** merge this branch into `chief/issue25-cta-appt-10commits` from D.

`transition_callbacks.parse_transition_callback` still does not accept:

- `v3u:t:appointment_submit`
- `v3u:t:appointment_back_time`

`telegram_transition_action_handler._view_for_result` still does not render `appointment_confirm`.

Until those two files are patched, a live time click answers but does not persist (correct: confirm is next). A live confirm submit click is unclaimed by the v3 transition parser.

### Expected patch 1 — `v3_core/user_bot/transition_callbacks.py`

Add to `_FLAG_KINDS`:

```python
"appointment_submit",
"appointment_back_time",
```

No value. Encoder can stay as-is; D renderer already emits those strings via `encode_appointment_flow_choice`.

### Expected patch 2 — `v3_core/user_bot/telegram_transition_action_handler.py`

In `_view_for_result`:

```python
if result.next_step == "appointment_confirm":
    if result.appointment is None:
        raise ValueError("appointment_confirm_action_missing_draft")
    return views.appointment_confirm(result.appointment)
```

Keep `result.next_step == "appointment_submit"` as the only persist boundary.

`ViewsStub` in out-of-window handler tests must grow `appointment_confirm`.

### Expected patch 3 — text custom-time path (optional same knife)

`transition_text_actions.py` still sets custom time `next_step="appointment_submit"`.
E should change that to `appointment_confirm`, then render confirm in `telegram_transition_text_handler.py`.

Out-of-window tests that still assert time → `appointment_submit`:

- `tests/v3_core/test_user_bot_telegram_transition_action_handler.py`
- `tests/v3_core/test_user_bot_transition_text_actions.py`

## Conflicts

None on D files vs baseline `90180b83`. Integration with A/B/C CTA labels is E’s merge job. D did not adopt PR #51 `租赁详情` lock.
