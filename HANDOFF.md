# Window D handoff — Issue #25 appointment flow

Baseline: `90180b834c5b13cb5e96aa51b24a081956bb18d2`
Branch: `agent/c07c08c09-appointment-flow-D`

## Completed in D

- `appointment_success_view.py`
  - Normal created/video submissions now use the locked heading `✅ 预约申请已提交` (no `预约成功`).
  - Keeps `预约时间已修改` only for an actual update operation.
  - Adds the post-submit actions required by Issue #25: `📅 我的预约`, `🏠 继续看房`, `💬 联系中文顾问`.
  - Video submission adds the non-blocking note that the adviser sends the video-call entry before the appointment.
- `telegram_transition_ui.py`
  - Reuses the existing `v3u:home:*` callback protocol for success-view home actions (`appointments`, `search`, `contact`) without adding a new callback namespace.
- `tests/v3_core/test_user_bot_telegram_transition_ui.py`
  - Adds route-level callback assertions for the three success actions.

## Important baseline evidence

The fixed baseline is already date-first at the listing booking transition boundary:

- `build_transition_plan(... transition == "book")` creates `PublicAppointmentDraft(... mode="offline")` and returns `next_step="appointment_date"` with `render_appointment_date`.
- The existing date view already offers today/tomorrow/day-after/other-date and a video-mode choice.

So there is no mode/focus gate before the date page in the extracted transition plan itself.

## Remaining Issue #25 gap that requires E wiring

The baseline currently performs **time -> appointment_submit directly** in `transition_actions.py`. Issue #25 requires **time -> confirmation -> submit**.

A clean implementation needs a distinct submit transition (recommended `appointment_submit`) or equivalent handler-visible state. The transport codec in `transition_callbacks.py` currently supports appointment values only for `appointment_date`, `appointment_mode`, and `appointment_time`, while `telegram_transition_action_handler.py` executes persistence when the action result says `appointment_submit`.

Those two files were explicitly outside Window D's allowed modification set. D did not modify them.

### Expected E patch

1. Add/accept one explicit confirmation-submit callback, e.g. `v3u:t:appointment_submit`.
2. Change the first `appointment_time` selection to persist the chosen time in public session state but return/render an `appointment_confirm` view instead of executing persistence.
3. Confirmation view must contain:
   - property summary
   - date
   - time
   - `实地看房` / `视频看房`
   - `[✅ 提交预约]`
   - `[⬅️ 修改时间]`
4. `✅ 提交预约` is the only action that crosses into `AppointmentSubmitExecutor`.
5. `⬅️ 修改时间` returns to the time view with date/mode/property preserved.
6. Video confirmation must remain video; it must not route back through mode selection.
7. Add/adjust handler tests so a first `appointment_time:*` callback does **not** call the executor, while the explicit submit callback does.

## Verification status

The GitHub connector available in this window can read/write repository contents but cannot execute the repository's local `compileall` / `pytest` commands. Therefore D does **not** claim the mandated test command has passed. E/integration must run the exact command from the task before production acceptance.

## Safety / production

- No deployment performed.
- No production service restarted.
- No production Telegram channel/group/Bot user message sent.
- No production database modified.
- No forbidden deploy/schema/collector/ingest/canonical files modified.
- No merge/cherry-pick into `chief/issue25-cta-appt-10commits`.
