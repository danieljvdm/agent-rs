# Calendar-to-reminder integration

Enable the `google-calendar` feature for `agent_rs::google_calendar`, an adapter module
beside `sqlite` and `openai`. It is off by default. It adds two typed tools owned by the
application: `read_calendar` reads the timed meetings of one day from Google Calendar, and
`create_reminder` creates a private, transparent one-minute event with a popup at the requested
time. This is a Calendar reminder, not a Google Tasks item. Email and other task services are
later work, to be chosen from actual usage. The adapter uses only the engine's
extension points (`Tool`, `Authorizer`, `Resolution::Reconcile`); with the feature unused,
nothing in the engine, the stores or the default build changes.

## Application contract

The application supplies an OAuth access token, concrete source and destination calendar IDs,
and the local midnights that bound the requested day. It owns account consent, token renewal,
preferences, authorization policy and the approval UI. Use explicit calendar IDs rather than the
account-relative `primary` alias. Prefer a dedicated destination calendar so reminders do not
become meeting inputs.

```rust,ignore
use std::sync::Arc;
use agent_rs::{Agent, Resolution};
use agent_rs::google_calendar::{CalendarScope, GoogleCalendar};

let calendar = GoogleCalendar::new(
    access_token,
    CalendarScope::new(
        source_id,
        reminders_id,
        "2026-11-02T00:00:00-08:00",
        "2026-11-03T00:00:00-08:00",
    ),
)?;
let agent = Agent::builder("reminders", model)
    .tools(calendar.tools()) // beside the application's preference and ask_user tools
    .authorizer(Arc::new(application_policy))
    .build()?;
// At an Approval operation, show its exact title and time to the user, then:
host.resolve(run_id, &operation.call.id, Resolution::Approve(approved)).await?;
```

`read_calendar` accepts `{"date":"2026-11-02"}` for the scoped date and returns a `CalendarDay`,
`{date, meetings:[{id,start,summary,description}]}`. It expands recurring events, reads every
page, excludes cancelled and all-day events and meetings starting outside the selected day, and
sorts by start time. Google filters the lower time bound by event **end**, so events overlapping
from the previous day are filtered again. The two midnights may have different offsets on a
daylight-saving transition; the application derives them from its user's time zone, because
this adapter does not guess it. Calendar text remains untrusted data.

`create_reminder` accepts
`{"title":"Book dentist appointment","at":"2026-11-02T09:00:00-08:00"}` and returns a
`ReminderReceipt`, `{reminder_id,title,at,idempotency_key}`, only after checking the provider's
event. Titles are limited to 256 bytes; times must be RFC 3339 with an offset. It sends no
attendees, disables default reminders, sets one zero-minute popup, and requests
`sendUpdates=none`. The application approves the exact title and time, including any time that
falls before the selected day. Actual popup delivery depends on the user's Calendar
notification settings; the adapter and the eval verify the stored event, not its delivery.

The client holds the access token only in memory, in a header marked sensitive. Tokens are never
tool arguments (unknown argument fields fail validation), bindings, receipts or error text. The
endpoint, calendar IDs and day are fingerprinted in both tools' versions, so a stored run
reopened against another target fails the agent's binding check; rotating the token does not
change the binding. Do not put secrets in your own instructions, input or tool data.

Reads are bounded to eight pages, 512 events and 1 MiB per response; exceeding a bound fails
instead of returning an incomplete calendar. Requests have a 30-second timeout, stop on the
call's cancellation token, follow no redirects, are never retried by the HTTP client, and spawn
no detached work. Set the agent's `Limits` (operation timeout and active budget) for your
workload. All-day planning, incremental sync, moving or rescheduling events and OAuth refresh
flows are outside this slice.

## Outcomes

The engine records what the tool knows. A failure with a known outcome is a
`ToolOutput::Failed` result the model sees, and the run continues; a failure whose outcome is
not known is an `Error::Tool`. For `read_calendar` (`RetryPolicy::ReadOnly`) an error is also
recorded for the model, since a read has no effect to lose. For `create_reminder`
(`RetryPolicy::Never`) an error leaves the operation `Unknown`, which stops the run until the
application reconciles it.

| Path | `read_calendar` | `create_reminder` |
| --- | --- | --- |
| Arguments rejected before sending (date outside the scope; blank or long title; time without offset) | `Failed` | `Failed` |
| The service refuses with a status: any non-success for a read; a 4xx other than 408 and 409 for a write | `Failed` | `Failed`, nothing was created |
| 408, 5xx or a redirect on a write | | `Err`: it may have been created |
| 409 on a write: the event ID exists | | read it back; `Complete` only if it matches, otherwise `Err` |
| Transport failure, cancellation | `Err` | `Err` |
| Malformed, oversized or inconsistent response; partial pagination | `Err` | `Err` |
| A provider event that differs from the approved reminder, or is recurring | | `Err` |

Error messages are persisted with failed and uncertain operations, so they never contain a
request URL (which names calendars and events), a response body or a credential. Transport
errors keep the `reqwest` error as their cause with its URL removed; decoding errors keep no
cause, because a JSON error's text can quote the response. `Failed` payloads carry only the
status code.

## Approval and uncertain writes

Every write has `requires_approval`, sequential execution and `RetryPolicy::Never`. The engine
persists the approval and asks the agent's `Authorizer` immediately before dispatch. Reads use
`RetryPolicy::ReadOnly`.

The event ID is a SHA-256 digest of the scope and the call's idempotency key, and private event
properties keep the key (never a secret). A duplicate-ID response is accepted only after a GET
verifies the ID, key, title, times and reminder settings. A recurring series or instance never
satisfies the approved one-time reminder, even if its first occurrence has the same times.
Verification leaves the provider event untouched, and no existing event is overwritten. This
supports identification; it is not an external exactly-once guarantee or permission to replay
writes, which is why writes use `Never` rather than `Idempotent`.

After a transport failure, cancellation during a write, or recovery of a dispatched write, the
engine keeps the operation `Unknown`. Build the client with the same scope and current
credentials, then inspect the exact operation:

```rust,ignore
if let Some(receipt) = calendar.reconcile(&operation, &cancel).await? {
    host.resolve(run_id, &operation.call.id, Resolution::Reconcile(receipt)).await?;
}
// None means "not observed", not "safe to retry". Keep the operation Unknown.
```

Reconciliation issues only GET requests. It requires an `Unknown` `create_reminder` operation
under this client's spec (so the same scope), which the engine only dispatches after approval,
and a matching provider event. A missing event, a changed event or an unavailable service is
never turned into a fabricated success or a new write. `calendar.reminders()` reads provider
state independently of model output, returning only events marked for this scope.

## Evaluation

The [personal-workflow eval](personal-workflow-eval.md) accepts a service backend beside its
model mode. The default is still scripted inference with local files. Service mode replaces the
file-backed `read_calendar` and `create_reminder` with this adapter, registers an application
authorizer, and adds a case that loses the write receipt, closes and reopens the host, checks
the operation is still `Unknown`, reads the provider event and reconciles. All cases keep the
existing missing-input, approval and denial, steering, cumulative budget, restart and
truthful-completion checks, and an independent oracle grades the raw provider events. The
steering case runs in every service mode: the oracle expects the steered title on the provider
event, so a reminder that ignores steering fails. `--api` selects the model adapter for live
inference exactly as in file mode, so any case runs against either API with any services.

Run the deterministic HTTP workflow without credentials (it binds a loopback port):

```sh
scripts/rust.sh test --all-features --locked --test google_calendar --test personal_service_workflow
scripts/rust.sh run --locked --features google-calendar --example personal_workflow_eval -- \
  --services loopback --mode scripted --trials 1 --out eval-results/calendar-loopback-local
```

To use real model inference with the same stateful HTTP fixtures:

```sh
scripts/eval-personal.sh --services loopback --trials 1 --out eval-results/calendar-model-local
```

For live Google services, supply `GOOGLE_CALENDAR_ACCESS_TOKEN` through the existing Yielded
Infisical project (`e27bd4e8-050b-4b7b-ab72-045abb9463cc`, `dev`, `/`) or your application
environment. The wrapper builds with the feature before injecting secrets. The eval needs
permission to create and delete secondary calendars and their events;
`https://www.googleapis.com/auth/calendar.app.created` is intended for this isolated use.
Production access to existing calendars needs the appropriate event scopes and resource
permissions; see Google's [scope reference](https://developers.google.com/workspace/calendar/api/auth).

```sh
scripts/eval-personal.sh --services google-calendar --allow-service-writes \
  --trials 1 --out eval-results/calendar-live-local
```

`--allow-service-writes` authorizes only the eval's fixture setup, scripted approval decisions
and cleanup in **new, uniquely named secondary calendars**. Each attempt creates its own source
and reminder calendars, seeds synthetic meetings without default notifications, reads back the
actual reminder events, saves evidence, then deletes the calendars it owns. It accepts no
existing personal calendar as a target. Use `--mode scripted` when invoking the executable
directly to test real services without a model call. No live service mode runs in CI.

`report.json` identifies the workflow fixture (`personal-reminder-v3`), the service fixture
(`personal-calendar-service-v2`), the model mode, the API of a live run, the model, the service
backend, revision, source hashes, all attempted cases and failures. Each attempt keeps
`workflow/result.json`, raw provider events and independent checks in `service-result.json`, and
`sandbox.json` for live cleanup. The oracle
checks the event count, exact schedule, title, popup configuration and the reported receipt. A
successful result requires cleanup as well as the workflow and provider checks; failures return
a nonzero exit status. Results go under the Git-ignored `eval-results/`.

If interrupted, resume cleanup using the recorded manifest:

```sh
scripts/eval-personal.sh --cleanup-sandbox eval-results/calendar-live-local/01-dentist/sandbox.json \
  --allow-service-writes
```

Cleanup checks each calendar's exact name and ownership description before deleting it,
continues past failures to clean the other calendar, and records progress. A creation response
lost before its calendar ID was recorded stays explicitly uncertain: find the manifest's exact
unique calendar name in Google Calendar and remove that fixture by hand. The eval neither
retries an uncertain creation nor guesses which calendar to delete.

## Performance

The engine's microbenchmarks and `tests/cost_budget.rs` do not exercise Calendar HTTP, and this
feature adds no budget: its cost is network round trips, which deterministic counters cannot
bound. The eval's per-attempt `elapsed_ms` (host startup, reads, writes, waits, restarts and
completion) is the measurement to use. Take timings on an isolated hosted worker, since
overlapping local runs establish correctness only, and label loopback Calendar numbers an
initial baseline, not a comparison with the file backend or evidence about Google's latency.

Protocol references: [list events](https://developers.google.com/workspace/calendar/api/v3/reference/events/list),
[insert events](https://developers.google.com/workspace/calendar/api/v3/reference/events/insert),
[create secondary calendars](https://developers.google.com/workspace/calendar/api/v3/reference/calendars/insert).
