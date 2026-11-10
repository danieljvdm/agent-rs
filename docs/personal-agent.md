# Personal agent application

The `yielded-personal-agent` workspace package adds a local, single-owner application to the reusable runtime: authenticated HTTP operations, conversations, saved history, and an approval/input/reconciliation inbox. Its browser UI uses durable snapshots after reconnect, not progress events. The runtime crate has no dependency on the application or its HTTP/authentication/UI dependencies.

## Run it

Supply a randomly generated access token through `PERSONAL_AGENT_TOKEN` (32–512 printable ASCII characters, without spaces). It is never printed or written to either database. The UI keeps it in memory; a page reload asks for it again, then restores the selected conversation and pending decisions from the server.

```sh
# With PERSONAL_AGENT_TOKEN already in the environment:
scripts/rust.sh run -p yielded-personal-agent --locked -- --demo
```

Open `http://127.0.0.1:3000`. Create a conversation and send a message such as “Friday errands.” The explicit offline demo asks for the note body, requests approval for the exact title/body, and saves a real entry to its local notebook only after approval. Denial finishes without a write. The demo is a deterministic workflow, not a language model.

For a live model, also supply `OPENAI_API_KEY` and `OPENAI_MODEL`, and omit `--demo`. This uses the existing OpenAI Chat Completions adapter and the same notebook tools (`ask_user`, `list_notes`, `save_note`). The selected model must support the adapter's structured-output contract. The adapter's [binding](model-adapters.md) (API and endpoint) is part of the agent configuration, so pending work accepted through one route is not recovered through another. Credentials are loaded by the application and passed explicitly to the adapter; they are not part of run state. Live provider behavior has not been newly evaluated for this application slice.

`PERSONAL_AGENT_DIR` defaults to `.personal-agent`. `PERSONAL_AGENT_BIND` defaults to `127.0.0.1:3000` and must be a loopback address. New data directories use owner-only permissions on Unix. Use separate directories for demo and live configurations. A second application cannot own the same runtime database concurrently. Ctrl-C/SIGTERM drains HTTP requests, joins admission recovery, shuts down the host, and closes SQLite workers; it preserves accepted work.

## HTTP contract

All `/api` routes require `Authorization: Bearer <PERSONAL_AGENT_TOKEN>`. The public HTML/CSS/JS shell contains no user data. The service has no cookie authentication or CORS access; responses use `Cache-Control: no-store`. JSON requests are limited to 64 KiB. This token grants access to the one owner's entire application; there is no tenant selection or per-user account system.

| Method and path | Request / behavior |
| --- | --- |
| `GET /api/session` | Agent identifier and scheduler/recovery health. |
| `POST /api/conversations` | `{ "id": "<client UUID>", "title": "Friday errands" }`; returns the saved conversation, `201`. Retrying the same UUID/title is idempotent; changing its title returns `409`. |
| `GET /api/conversations` | A page of saved conversations, newest first. |
| `POST /api/conversations/{id}/runs` | `{ "key": "<client request key>", "input": "Friday errands", "expires_at_ms": null }`; returns `{ "run_id": "<UUID>" }`, `202`, after durable acceptance and catalogue association. |
| `GET /api/conversations/{id}` | Conversation metadata and a page of submissions/runs, newest first. Run snapshots contain durable history and operation evidence. |
| `GET /api/runs/{id}` | Current run snapshot, usage, history, retained operations, explicit decisions, cancellation state, and any host error. |
| `GET /api/inbox` | A page of unfinished submissions/runs, oldest first, including cancelled runs with uncertain effects. A run may still be working and have no decisions. |
| `POST /api/runs/{id}/resolve` | Resolve the exact `call_id`, `generation`, and `revision` returned in a decision; returns `204` after commit. See below. |
| `POST /api/runs/{id}/cancel` | Persist cancellation before signalling execution; returns `202`. Repeating it is harmless. Cancellation does not undo an external effect. |

Paginated reads accept `?limit=50&after=0` (`limit` 1–100). Responses contain `items` and `next_cursor`; conversation snapshots wrap these in `submissions`. Pass the returned cursor as `after` until it is null. The cursor follows each resource's documented ordering. It advances over examined records, so an inbox page can be empty with a non-null cursor when settled records are retired. New submissions may appear on an earlier page: start again at zero when refreshing. The UI initially reads 30 records per page and offers load-more controls.

Inputs are JSON values validated by the configured agent. The supplied notebook agent accepts a string. Titles and request keys are at most 200 bytes. Use a new request key for intentional new work; after a connection failure retry the identical key and payload. A busy conversation returns `409`; a rejected key remains rejected, even if the conversation later becomes available.

Example decision response (IDs are illustrative):

```json
{
  "run_id": "eb188350-5e45-48af-9446-f966c3f40ef3",
  "call_id": "save-note",
  "generation": 3,
  "revision": 9,
  "tool": "save_note",
  "arguments": { "title": "Friday errands", "body": "Book the dentist" },
  "kind": "approval",
  "prompt": "Allow save_note?",
  "actions": ["approve", "deny"]
}
```

Send the identity and version from the snapshot, without inventing or incrementing them:

```json
{
  "call_id": "save-note",
  "generation": 3,
  "revision": 9,
  "decision": { "kind": "approve" }
}
```

Other decisions are `{ "kind": "deny" }`, `{ "kind": "input", "value": "My answer" }`, or `{ "kind": "reconcile", "value": { "receipt_id": "confirmed-123" }, "is_error": false }`. Set `is_error: true` to record a verified failure. Reconciliation records an observed outcome; it never retries the external action. Check the destination first. It remains available after cancellation. Approval and input are unavailable once cancellation has been requested.

Old, duplicate, mismatched, or no-longer-waiting decisions return `409` with `error.code: "stale_decision"`. Refresh and review before issuing another decision; do not automatically replay an approval against a new revision. Active decisions use the public checkpoint compare-and-swap operation, so a concurrent worker or cancellation fences the commit. Terminal reconciliation is serialized by the application, while the runtime ensures only reconciliation is legal after settlement.

Application errors have `{ "error": { "code": "...", "message": "..." } }`. The status follows the runtime's `Error` variant, never its message:

| Status and code | Runtime error |
| --- | --- |
| `400 invalid_input` | `Input`: a bad title, key, page, input value, or reply size. Its message describes the request. |
| `404 not_found` | `NotFound`: no such conversation or run. |
| `409 idempotency_conflict` | `IdempotencyConflict`: the key or conversation ID belongs to a different request. |
| `409 conversation_busy` | `ConversationBusy`: finish or cancel the current run first. |
| `409 stale_decision` | `Stale` or `NotWaiting`: refresh before deciding again. |
| `409 incompatible_configuration` | `Incompatible`: restore the agent configuration the pending work was accepted with. |
| `409 run_not_active` | `RunNotActive`: the run has settled or is being cancelled. No route steers runs today; start a new request instead. |
| `503 unavailable` | `Storage`, `StoreBusy`, or `Interrupted` (shutting down). Retry; a submission may already be saved. |
| `500 internal` | Anything else, including variants the runtime adds later. |

Authentication failures are `401 unauthorized`. Malformed JSON/media types/body limits use their HTTP rejection status and `invalid_json`; invalid path/query syntax uses the framework's `400` response. Clients should use status codes before inspecting the optional JSON error body.

## Durability and ownership

The data directory contains three independent stores:

- `application.sqlite`: conversation metadata and the admission journal. A request is saved before calling `Host::submit`; a background recovery loop repeats pending admissions with their original key. A crash on either side of host acceptance therefore recovers one run, including when the client never received an acknowledgement.
- `runtime.sqlite`: the existing host's fenced checkpoints, cumulative budgets, history, waits, cancellation, and operation outcomes. The app only uses public runtime APIs and never writes its tables directly.
- `notebook.sqlite`: local note effects. The note and its operation key commit in one transaction. A note the notebook rejects (an empty or oversized title or body, or a replayed key with a different payload) changes nothing, so it is reported to the model as a known failure (`ToolOutput::Failed`) and the run continues; only a storage failure leaves the outcome unknown and needs reconciliation. Tool cleanup joins outstanding SQLite work after cancellation or shutdown.

The application owns one `Host<SqliteStore>` with the configured agent. Snapshots read the host's store through `DurableStore` (`status`, `operations`, `last_error`). Submission and cancellation go through the host. An active decision is applied to the exact checkpoint the user reviewed: the application reads the run, checks its generation and revision, applies `RunState::resolve`, and commits through the store's fenced checkpoint, so a concurrent worker or cancellation turns it into `409 stale_decision`. The host does not take an expected revision on `resolve`, so this commit bypasses it, and the host picks the run up on its next discovery poll (100 ms by default). Reconciling a settled run goes through `Host::resolve`. Progress events are not used: the UI is built from durable snapshots. The application sends no [steering](steering.md), but a run's history can contain a `Steering` entry accepted through the runtime's API; the UI shows it in place as the user's instruction, and the demo workflow ignores it.

Back up/restore the whole directory while the service is stopped. Restoring only one database can break the catalogue-to-run association. Admission recovery requires the original agent binding (model, tools, limits, instructions, schemas); a pending admission from another configuration remains visibly blocked until that configuration is restored. The app does not silently rebind it. Accepted run compatibility continues to be enforced by the host.

The catalogue indexes runs for discovery; the durable runtime remains the authority for their state. An inbox record is retired only after settlement with no uncertain operations. Snapshot reads are eventually consistent across runs; each decision carries a fenced revision. The UI preserves typed replies through background refreshes and reconstructs history/decisions after network recovery or reauthentication.

## Scope and evidence

This first application slice supports a local owner, notebook effects, the existing OpenAI adapter, and explicit demo mode. It does not provide shared accounts, remote hosting/TLS, production email/calendar/reminder integrations, streaming token presentation, notebook search beyond the latest 20 entries, or agent-configuration migrations. These remain separate application/adapter work; YIE-245's engine scope is unchanged.

The application tests send real HTTP requests through the served router in-process (routing, authentication, body limits, extraction, error mapping, and headers, without a socket) against file-backed SQLite stores. They cover reconnect, restart, approval/denial, rejected notes, cancellation, exact-operation/version conflicts, admission recovery, and reconciliation. A subprocess test kills the owning process after a non-replayable effect, then confirms restart exposes uncertainty and reconciliation does not repeat it. Run them with `scripts/rust.sh test -p yielded-personal-agent --locked`.

The browser regression flow is `scripts/personal_agent_browser_smoke.js`. Start `--demo` with a disposable `PERSONAL_AGENT_DIR` and the public fixture token `test-only-personal-agent-access-token`, open the local URL with a named `playwright-cli` session, then run `playwright-cli -s=<session> run-code --filename=scripts/personal_agent_browser_smoke.js`. It exercises draft/focus preservation, untrusted text rendering, reauthentication after reload, offline reconnect, denial, approved completion, and mobile overflow. Never use that public token for real personal data.

An earlier revision of this application was measured once at its loopback HTTP boundary on an isolated hosted runner (two CPUs, 400 samples per operation, deterministic in-process models). Median submit-to-completion was about 5 ms, a complete 256-entry inbox traversal about 51 ms, and approval-to-completion about 95 ms. The approval figure is almost entirely the host's 100 ms discovery interval, for the reason given under durability. That benchmark apparatus is not kept in the repository, and the figures are a characterization, not a threshold.
