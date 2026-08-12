# RFC 0013 — Durable engine adapters: deliberate thinness for `forze_temporal` (+ Inngest parity notes)

- **Status:** ✅ Executed (2026-08-12) — see §10 for the execution log
- **Scope:** Make `forze_temporal`'s thinness a *stance* instead of an accident. The package's identity is fixed as **connection + codec + interceptors + schedules + lifecycle** — workflow and activity authoring stays raw `temporalio`, forever. Four workstreams close the gap between that stance and the current code: **(W1)** a public raw-client escape hatch, **(W2)** start-options passthrough, **(W3)** a worker lifecycle step, **(W4)** activity heartbeat ergonomics. A short §6 records what parity means (and doesn't) for `forze_inngest`.
- **Related:** The durable workflow contracts document an escape-hatch policy in their own docstrings ("child workflows / continue-as-new: use the raw SDK, per the escape-hatch policy") — W1 exists because the code currently contradicts that written policy. The broker/durable failure-path review supplies the `exception_egress_policy(kind).retryable` hook `TemporalSaga` already uses. The promoted background-loop machinery (`BackgroundLoopControl`, `run_supervised`) is what W3 builds on. The observability plane's worker-process probe surface pairs with W3 operationally. Durable run control is the shipped sibling for the self-hosted tier.
- **Origin:** The eis-dag evaluation (2026-07-30): a real Temporal application uses precisely the features the wrapper doesn't expose — per-activity retry policies, `heartbeat_timeout` as the dead-worker detector, activity heartbeats with details, cancellation-type control, custom `@workflow.defn`s — and today reaches them only by building a **second, hand-configured `temporalio.Client`**, which silently drops the `EncryptingPayloadCodec` and interceptor stack the framework client carries. That is not an ergonomics gap; it is a security footgun the framework itself creates.

---

## 1. The stance, stated once

`forze_temporal` will **never** grow a workflow model, an activity registry, a step DSL, or an opinion about task-queue topology. Temporal's SDK is a complete authoring surface with its own determinism sandbox, test framework, and documentation; wrapping it produces a worse Temporal, and the self-hosted function tier already serves callers who want forze-shaped durability. What the framework legitimately owns is the *boundary*: how a process connects (config, mTLS/API key, namespace), what rides on the wire (codec — encryption; data converter — pydantic), what crosses into execution context (interceptors — identity, tenant, correlation, replay-safe time), when things start and stop (lifecycle, drain), and the schedule control plane. Everything below is that boundary, made complete.

## 2. W1 — the escape hatch, made real

`TemporalClient.__client` is name-mangled private with no accessor. The documented policy says "use the raw SDK" for anything beyond the port surface; the code makes that impossible without `Client.connect`-ing a second time by hand — and a hand-built client misses `pydantic_data_converter`-wrapped-in-`EncryptingPayloadCodec`, the interceptor stack, and the config's rpc metadata. Payloads encrypted through the framework client go **plaintext** through the workaround. The contradiction resolves in one move:

- `TemporalClient.native` — a property returning the initialized `temporalio.client.Client` (raising the standard not-initialized error before `initialize`). Named `native`, not `raw`: it is the fully-configured client, codec and all — nothing about it is raw.
- Docstring states the contract: the escape hatch is for SDK surface the port deliberately omits (child workflows, continue-as-new, workflow update polling, exotic start options); anything reachable through the port goes through the port, because the port is what the mock and DST can see.
- `RoutedTemporalClient` exposes the same per-route.

This single property deletes the entire "second client" class of bug.

## 3. W2 — start options, config-shaped

`start_workflow` accepts `queue/name/arg/workflow_id/raise_on_already_started` and nothing else — no retry policy, no execution/run/task timeouts, no id-reuse policy, no memo/search attributes. The framework answer is **not** widening the call signature five kwargs at a time; per the wiring philosophy, per-workflow-kind options are configuration:

- `TemporalStartOptions` (frozen attrs value object): `retry_policy` (max attempts, backoff — mapped to `temporalio.common.RetryPolicy`), `execution_timeout`, `run_timeout`, `task_timeout`, `id_reuse_policy`, `memo`, `search_attributes`, `start_delay`.
- Declared per workflow name on `TemporalWorkflowConfig` (where queue/name binding already lives): options are a property of the workflow *kind* — "pipeline runs never retry", "reports time out at 6 h" — not of the call site.
- `start_workflow` gains one optional `options: TemporalStartOptions | None` for the genuinely per-call case, overriding the configured default field-by-field. The contract-level `DurableWorkflowCommandPort.start` is **unchanged** — options are engine vocabulary and stay in the adapter layer; a caller needing them from a spec routes through Temporal config, exactly like every other backend-specific tuning knob in the framework.
- Anything beyond this list (versioning ramps, typed search-attribute updates, …) is what W1's `native` is for.

## 4. W3 — the worker, finally a lifecycle citizen

"Workers are separate by design" was the right call about *deployment* and the wrong call about *boilerplate*: every adopter hand-writes the same connect-with-retries → `Worker(...)` → signal handling → graceful shutdown loop (eis-dag's `worker.py` is 110 lines of exactly this). The framework already promoted supervised background loops and drain; the worker step composes them:

- `temporal_worker_lifecycle_step(client=..., task_queue=..., workflows=[...], activities=[...], *, max_concurrent_activities=..., workflow_runner=sandboxed_workflow_runner(), graceful_shutdown: timedelta = ...)` — startup starts the `temporalio.worker.Worker` under the supervised-loop machinery (crash restart with backoff, crash-loop ceiling); shutdown calls the worker's graceful shutdown bounded by the runtime's drain window, so in-flight activities get the same drain semantics as every other plane.
- The step takes the framework `TemporalClient` (via `native`) — one connection, codec included, shared with any client-side use in the same process.
- Pairs with the worker-process probe surface (the observability plane): a worker process built as `build_runtime(...)` + this step + the probe step is the canonical worker recipe, documented as such — the missing "worker entrypoint" answer stays a recipe, not a CLI.
- Explicitly not owned: how many workers, which queues exist, deployment shape. The step runs *a* worker; topology is the operator's.

## 5. W4 — activity heartbeat ergonomics

`heartbeat_timeout` is Temporal's real liveness detector for long activities, and the wrapper currently pretends heartbeats don't exist. Two small pieces, no model:

- **Auto-heartbeat companion** on the worker interceptor: opt-in (`auto_heartbeat=True` on the interceptor's config), wraps activity execution with a background task calling `activity.heartbeat()` at `heartbeat_timeout / 3` while the activity runs. Covers the common case — "my activity is alive, stop killing it" — where the activity has no incremental details to report. Off by default: an auto-heartbeat *masks* a stuck activity's failure to make progress, so turning it on is a decision.
- **Details stay manual and raw**: an activity reporting incremental state (`activity.heartbeat(details)` for resume-after-timeout) is authoring-surface territory — docs show the pattern (including pairing with the operation-progress reporter for user-visible progress), the framework adds nothing between the author and the SDK.

## 6. Inngest parity notes

`forze_inngest` is already shaped like the stance (serve endpoint, step adapter delegating to the SDK's bound step, registration, lifecycle) because the Inngest SDK forces the boundary the Temporal wrapper blurred. Parity items are small: ~~the step adapter's capability surface participates in durable run control's `supports_cancel` (mapping to platform run cancellation)~~ — **withdrawn 2026-08-05, see RFC 0036**: there is no such capability surface, and the SDK offers only declarative `cancel_on`, so participation needs either a new REST surface or a per-function capability. Neither is a parity note. The serve/registration path gets the same "canonical worker recipe" documentation treatment as W3; no escape-hatch work is needed (the SDK's own objects are already in the author's hands). No other changes — parity means *the same boundary discipline*, not the same feature list.

## 7. Proof obligations

1. `native` returns the same client instance the port methods use — payload written via `native` round-trips through a port read under an encrypting codec (the footgun test, pinned forever).
2. Configured `TemporalStartOptions` demonstrably reach the server: a started workflow's describe shows the id-reuse policy/timeouts; a per-call override wins field-by-field.
3. Worker step: activities in flight at shutdown complete within the drain window; a crashing worker restarts with backoff and trips the crash-loop ceiling; boot fails loudly on an unreachable server after the retry budget.
4. Auto-heartbeat keeps a slow activity alive past `heartbeat_timeout`; with it **off**, the same activity is timed out (the default's honesty, pinned).
5. All of the above against a real Temporal dev server (testcontainers), per the fidelity policy — there is no mock Temporal, so the integration suite *is* the battery.

## 8. Phases

- **P1** — W1 (`native`) + the footgun test. Smallest possible PR, highest safety value.
- **P2** — W2 (`TemporalStartOptions` on config + optional per-call override).
- **P3** — W3 (worker lifecycle step) + canonical worker recipe docs.
- **P4** — W4 (auto-heartbeat opt-in) + Inngest parity notes (minus cancellation → RFC 0036) + docs sweep stating the stance (§1) verbatim in the integration page.

## 9. Decision log

| # | Decision | State |
|---|---|---|
| 1 | Package identity frozen: connection/codec/interceptors/schedules/lifecycle; no workflow model, ever | locked |
| 2 | Escape hatch = `native` property on the framework client; hand-built second clients are the bug class this kills | locked |
| 3 | Start options are per-workflow-kind **config** (`TemporalWorkflowConfig`), with an optional per-call override; the engine-agnostic contract port is unchanged | locked |
| 4 | Worker step composes the promoted supervised-loop + drain machinery; topology stays operator-owned; entrypoint stays a recipe | locked |
| 5 | Auto-heartbeat is opt-in and details-less; details-bearing heartbeats stay raw SDK | locked |
| 6 | ~~Inngest parity = boundary discipline + `supports_cancel` participation, nothing more~~ → **REOPENED by RFC 0036 (2026-08-05):** unbuildable as written — the capability lives on the admin port (not the step adapter), the step adapter has no reach to a *run*, and the Inngest SDK has no imperative cancel (only declarative `cancel_on`). Parity = boundary discipline; cancellation is deferred to RFC 0036 | reopened |
| 7 | No mock Temporal; the real-server integration suite is the conformance battery | locked |

---

## 10. Execution log (2026-08-12)

All four phases landed, one commit per phase. Decision rows 1–5 and 7 held as written;
row 6 was already reopened by RFC 0036 and stayed that way (parity = boundary
discipline, cancellation deferred).

### Readings — where the spec left a choice

| # | Question | Reading taken |
|---|---|---|
| R1 | §3: `start_workflow` "gains one optional `options` … overriding the configured default field-by-field" — where does the merge happen? | The two sets meet in the **adapter**. `TemporalClientPort.start_workflow(options=…)` carries one already-merged set to the SDK; `TemporalWorkflowCommandAdapter.start(…, options=…)` merges the workflow kind's configured set with the per-call one. Decision 3's second clause stays literally true — options stay in the adapter layer, and `DurableWorkflowCommandPort.start` is untouched, because an implementation may accept *more* than its protocol promises |
| R2 | Does `TemporalStartOptions` mirror the SDK's types or wrap them? | Carries them directly (`RetryPolicy`, `WorkflowIDReusePolicy`, `TypedSearchAttributes`). The whole value object is declared engine vocabulary; a parallel `TemporalRetryPolicy` adds no meaning and would have to chase SDK evolution. `search_attributes` takes only the typed form, not the deprecated mapping |
| R3 | Do **schedules** for a workflow kind inherit its start options? | No, and it is documented on the field and the page. `ScheduleActionStartWorkflow` cannot express `id_reuse_policy` or `start_delay`, so inheriting would silently drop two of eight fields; §3 speaks only of starts, and deciding the other two belongs to whoever needs them |
| R4 | Obligation 3: "boot fails loudly on an unreachable server after the retry budget" — whose boot? | The **client** step's. The worker step takes an already-connected client, so an unreachable server fails at `Client.connect`, which raises on a refused connection without spending a retry budget. Pinned twice: connect to a dead address raises, and a worker step ordered *before* the client step raises at boot instead of crash-looping |
| R5 | §4's signature has no `interceptors=`. Deliberate? | Yes, and now provable: the SDK prepends client interceptors that are also worker interceptors (no dedup), so one `ExecutionContextInterceptor` on the client covers the worker, and a parameter here would invite running it twice. Pinned by an integration test |

### Departures — beyond what §2–§5 asked for

| # | Addition | Why |
|---|---|---|
| D1 | Two wiring refusals on the worker step: a non-positive `graceful_shutdown`, and a worker registering neither workflows nor activities | The second is the quiet one — such a worker polls a queue and answers nothing, so its tasks time out and retry forever behind a process that looks perfectly healthy |
| D2 | `DEFAULT_WORKER_GRACEFUL_SHUTDOWN = 30 s` | §4 left the default open. The SDK's own is **zero**, which turns every deploy into cancelled work |
| D3 | A stop check at the top of the supervised run | Found while testing: startup immediately followed by shutdown (a failed boot, a health-check flap) left `stop()` with no worker to shut down, and the loop then built one nobody would ask to stop — it polled out the whole grace window and was cancelled mid-task |
| D4 | `TemporalStartOptions` refuses non-positive timeouts at construction | Temporal reads a non-positive timeout as *unset*, silently widening the bound the caller meant to tighten |
| D5 | `max_consecutive_crashes` exposed, defaulting to `None` | Framework parity with `run_supervised`; exposed so the ceiling in §4 is reachable and testable |
| D6 | Corrected `skills/forze-temporal-workflows` anti-pattern 5 | It advised registering the context interceptor on both client and worker, which R5 shows runs it twice |

### Found, out of scope, not fixed

`TemporalWorkflowCommandAdapter.start` builds its handle from `WorkflowHandle.run_id`,
which `Client.start_workflow` never populates (the set field is `result_run_id`). So a
handle returned by `start` always carries `run_id=None`, and every later call on it uses
"latest run" semantics. Populating it would **pin** those calls to that run, changing
behaviour across continue-as-new and retries — a decision of its own, not a side effect
of this RFC.

### Proof obligations

| § | Obligation | Where |
|---|---|---|
| 7.1 | `native` is the client the port drives; a payload written through it round-trips under an encrypting codec | `tests/integration/…/test_native_escape_hatch.py` — and the round trip alone is **not** the assertion: the codec passes unrecognized payloads through, so a hand-built second client round-trips fine. What fails is the at-rest read through a codec-less client |
| 7.2 | Configured options reach the server; a per-call override wins field by field | `tests/integration/…/test_start_options.py` — timeouts and memo via describe, retry policy from the start event, id-reuse behaviourally (a refusal, with an allow-duplicate control) |
| 7.3 | Drain, crash restart, crash ceiling, loud boot failure | `tests/integration/…/test_worker_lifecycle.py` (drain, with a tiny-window control) + `tests/unit/…/test_worker_lifecycle.py` (restart, ceiling, boot) |
| 7.4 | Auto-heartbeat keeps a slow activity alive; with it off the same activity is timed out | `tests/integration/…/test_auto_heartbeat.py` — the off leg asserts the timeout is `HEARTBEAT` specifically, not `start_to_close` |
| 7.5 | All of it against a real Temporal dev server | Every integration leg above runs on the testcontainers dev server |
