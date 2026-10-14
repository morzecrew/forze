# RFC 0074 — Drain announcement and readiness checks

- **Status:** 📝 Draft
- **Scope:** Runtime additions for graceful deploys:
  - readiness that waits for lifecycle startup to finish;
  - `runtime.begin_draining()`, which turns readiness off without refusing operations;
  - registered drain phases that run between that announcement and teardown;
  - the dependency checks the FastAPI readiness route already has, on the stdlib probe
    listener.

  Plus a `forze_fastapi` helper that serves an embedded uvicorn with SIGTERM routed into that
  order, rather than swallowed. The state lives on the execution context, so lifecycle steps
  can read it from their hook's `ctx`.

  It changes no operation semantics: the drain gate still closes only in `shutdown()`. The
  connection drains that use the phases are RFC 0075.
- **Related:**
  - [`src/forze/application/execution/runtime.py`](../src/forze/application/execution/runtime.py):
    `ready`, `draining`, `shutdown`;
  - [`src/forze/application/execution/context/drain.py`](../src/forze/application/execution/context/drain.py):
    the drain gate;
  - [`src/forze/application/execution/background/probes.py`](../src/forze/application/execution/background/probes.py):
    `probe_listener_step`;
  - [`src/forze_fastapi/routes/health.py`](../src/forze_fastapi/routes/health.py):
    `attach_readiness_route`;
  - [`src/forze_fastapi/lifespan.py`](../src/forze_fastapi/lifespan.py);
  - RFC 0073 (fleet membership), RFC 0075 (connection drain).
- **Origin:** hh-backend-v2's fleet proposals (F5, 2026-10-14). The app builds the deploy order
  itself (`runner.py`, about 150 LOC) and its own readiness listener (`health.py`, about
  135 LOC). Its incident D-274 was every restart becoming a kill at the grace period,
  because embedded uvicorn swallowed SIGTERM.

---

## 1. Summary

- **Readiness waits for startup.** `runtime.ready` becomes true only once lifecycle startup has
  finished. Today it is true from the moment the context exists, before any step has started.
- **`runtime.begin_draining()`** makes readiness fail and `runtime.draining` true at once,
  while operations keep being admitted. A load balancer stops routing new work; work already
  routed finishes normally.
- **`runtime.drain(budget)`** announces, waits `announce_delay` for the balancer to notice,
  then runs the drain phases that steps registered, in order, within `budget`.
  `shutdown()` runs it first if nobody did and startup had finished, so the phases run on
  every orderly exit path.
- **Steps read this state from their own `ctx`.** `ctx.lifecycle.started` and
  `ctx.lifecycle.announced` mean a step never needs a handle on the runtime that runs it.
- **`serve_uvicorn(config, runtime)`** serves an embedded uvicorn whose SIGTERM and SIGINT
  start `runtime.drain()` and only then tell uvicorn to exit, with a finite graceful timeout.
- **`probe_listener_step(checks=…)`** answers readiness from the runtime and from dependency
  probes, sharing one evaluator with the FastAPI route.

## 2. Motivation

**Readiness is true too early.** `runtime.ready` is `not ctx.drain_gate.draining` once a scope
exists (`runtime.py:359-368`). The context is created before `lifecycle.startup` runs
(`runtime.py:539-543`). A probe listener started as an early step therefore reports ready
while later steps are still starting.

**Readiness flips too late.**
- `draining` is set only by `shutdown()`, through `drain_gate.drain()` (`context/drain.py:178`,
  `runtime.py:442`).
- So readiness fails at the same moment new operations start being refused. A balancer still
  routing to the replica sends it requests that are refused.
- Flipping the gate early to fix readiness would refuse operations early instead.

**A deploy wants this order:**
1. readiness fails;
2. the app's own drain phases run: couriers hand over, sockets close gently;
3. teardown.

hh-backend-v2 builds that order by hand (`runner.py:171-321`).

**Steps cannot see the runtime that runs them.**
- `ExecutionRuntime` is frozen and takes its lifecycle plan at construction (`runtime.py:87`).
- A step built with `runtime=` must be built before that runtime exists, so it cannot be in
  that runtime's own plan. `attrs.evolve` creates a fresh scope that the captured runtime
  never sees.
- `probe_listener_step(runtime)` works in its tests only because they call its startup by hand
  (`test_probe_listener.py:58-64`).
- Steps that need readiness or draining state need it from their hook's `ctx`.

**Embedded uvicorn swallows SIGTERM.**
- `uvicorn.Server.serve()` wraps itself in `capture_signals()`. That replaces the process's
  SIGTERM and SIGINT handlers with one that sets `should_exit` at once (uvicorn 0.51,
  `server.py:316-345`).
- An app that embeds uvicorn and installs its own handler loses it for as long as `serve()`
  runs. Every restart becomes an immediate exit or a kill at the grace period, with no drain
  (D-274).
- Uvicorn's shutdown then closes every WebSocket at once with 1012 and waits for open HTTP
  streams with no limit, because `timeout_graceful_shutdown` defaults to `None`. An open SSE
  stream blocks the exit until the orchestrator kills the process
  (`websockets_impl.py:151-157`, `config.py:230`).

**The stdlib probe cannot check dependencies.**
- `probe_listener_step` answers from `runtime.ready` and `runtime.draining` only
  (`probes.py:226-232`).
- Only FastAPI's `attach_readiness_route` takes probes (`routes/health.py:37-44`, `:152`).

## 3. Current state

All verified against the tree at the time of writing.

- **The runtime.**
  - `ExecutionRuntime` is a frozen, slotted attrs class (`runtime.py:87`).
  - `draining` returns `ctx.drain_gate.draining`, and `ready` returns its negation. Both are
    `False` outside a scope (`runtime.py:340-368`).
  - The profile checks scan the lifecycle plan in `__attrs_post_init__` (`runtime.py:269-296`).
- **`shutdown()` runs in this order** (`runtime.py:440-474`):
  1. `drain_gate.drain(drain_timeout)`;
  2. `ctx.drainables.stop_all`;
  3. closing the background owners;
  4. each lifecycle step's shutdown, bounded by `shutdown_step_timeout`;
  5. resetting the context.
- **Rollback.** A partial startup failure shuts the started steps down first
  (`lifecycle/run.py:153-176`).
- **The probe listener.** `probe_listener_step(runtime, *, host, port, path_ready,
  path_live, step_id)` is a stdlib HTTP listener and a drainable (`probes.py:105-155`,
  `:276-284`). Its 503 body is "draining" or "unavailable" (`:232`).
- **The FastAPI route.** `attach_readiness_route(router, runtime, *, path, probes,
  probe_timeout)` runs each probe's `health()` (`routes/health.py:37-44`, `:97`, `:152`). Each
  integration's client port declares `health() -> Awaitable[tuple[str, bool]]` on its own.
- **The lifespan.** `runtime_lifespan` checks the websocket allowlist and the bypass paths,
  then enters `runtime.scope()` (`forze_fastapi/lifespan.py:42-53`).
- **Uvicorn.** Nothing in `forze_fastapi` runs uvicorn. The only runner is the mock server's
  `uvicorn.run` (`forze_mock/server/runner.py:195-219`). The project requires
  `uvicorn>=0.42`, and 0.51 is installed.
  - `capture_signals` uses `signal.signal`, from the main thread only (`server.py:319-334`).
  - The main loop checks `should_exit`, not `force_exit` (`:259`).
  - `Server` has no `__slots__`.

## 4. Goals / Non-goals

**Goals**

- Readiness is true only between the end of startup and the start of a drain.
- Readiness can fail before any operation is refused.
- Steps can register work that must run between that moment and teardown, with one budget
  for all of it.
- Steps can read the readiness state from their hook's `ctx`.
- An embedded uvicorn exits only after that work, on SIGTERM as on SIGINT, and in bounded
  time.
- One readiness evaluator, used by both the stdlib listener and the FastAPI route.

**Non-goals**

- **Changing when operations are refused.** The gate still closes in `shutdown()`.
- **Owning the process.** forze offers the uvicorn helper; apps that run `uvicorn app:app`
  keep uvicorn's own signal handling (5.5).
- **Liveness checks.** Liveness stays "the process answers".

## 5. Design

### 5.1 Lifecycle state on the context

```python
@final
class LifecycleState:                  # ctx.lifecycle
    started: bool                      # lifecycle startup finished
    announced: bool                    # begin_draining() was called
```

- **The fields.** The runtime sets `started` after `lifecycle.startup` returns, and `announced`
  in `begin_draining()`.
- **Who reads them.** Steps read them from the `ctx` their hooks receive, so no step takes
  `runtime=`. The fleet membership step (RFC 0073) and the drain steps (RFC 0075) use them.
- **The runtime's properties:**
  - `runtime.ready`: a scope is active, `started`, not `announced`, and the gate is not
    draining.
  - `runtime.draining`: `announced`, or the gate is draining.
- **Operations versus readiness.** `ctx.drain_gate.draining` keeps its meaning: operations are
  refused.

### 5.2 Announcing a drain

`runtime.begin_draining()` sets `ctx.lifecycle.announced`. It is idempotent, and does not touch
the drain gate, so operations are still admitted until `shutdown()` closes it.

### 5.3 Drain phases

```python
ctx.drain_phases.register(
    name: str,
    phase: Callable[[timedelta], Awaitable[None]],   # receives the budget left
    *,
    order: int = 0,
)

async def drain(self, budget: timedelta, *, announce_delay: timedelta = timedelta(0)) -> None
```

`runtime.drain()` runs at most once per scope, and only after startup has finished:
1. It calls `begin_draining()`.
2. It waits `announce_delay`, so the balancer can notice that readiness failed.
3. It runs the registered phases in ascending `order`, each given the budget left.

**Budgets.**
- A phase that overruns the budget is cancelled and logged.
- A phase whose budget left is below a small floor (100 ms) is skipped, and the skip is
  logged. The phases are told the budget so they can shrink their own windows (RFC 0075).

**Where `shutdown()` comes in.**
- `shutdown()` calls `drain()` first, with `drain_budget` from the runtime's settings, if
  `drain()` has not run and `started` is set.
- After a partial startup failure, rollback has already shut steps down, so phases are not
  run against closed resources.
- A deploy that called `drain()` earlier does not run the phases twice.

**Phases are not drainables.** Drainables are loops that `stop_all` stops after the gate
closes. Phases are work that must happen while the replica still serves: a courier handing its
routes back, sockets closing gently over a window.

### 5.4 Readiness checks on the probe listener

```python
def probe_listener_step(
    runtime: ExecutionRuntime | None = None,
    *,
    host: str = "0.0.0.0",
    port: int,
    path_ready: str = "/readyz",
    path_live: str = "/livez",
    checks: Mapping[str, DepKey[Any]] | None = None,
    check_timeout: timedelta = timedelta(seconds=2),
    step_id: StrKey = PROBE_LISTENER_STEP_ID,
) -> LifecycleStep: ...
```

- **No runtime needed.** The listener reads `ctx.lifecycle`, `ctx.drain_gate` and the checks
  from the hook's `ctx`. `runtime` becomes optional and deprecated, so the step can sit in its
  own runtime's plan.
- **One evaluator.** The probe evaluation moves out of `forze_fastapi/routes/health.py` into a
  core helper next to the listener. It resolves each key, calls `health()` under the timeout,
  and reports the names that failed. `attach_readiness_route` and the listener both call it,
  so they answer the same way from the same probes.
- **The answer.** Readiness is 503 while the runtime is not ready, or while any check fails or
  times out. The body names the failing checks, in the shape the FastAPI route uses.
- **Liveness** ignores the checks.

### 5.5 Serving uvicorn

```python
async def serve_uvicorn(
    config: uvicorn.Config,
    runtime: ExecutionRuntime,
    *,
    drain_budget: timedelta,
    announce_delay: timedelta = timedelta(0),
    graceful_timeout: timedelta = timedelta(seconds=10),
) -> None: ...
```

1. **Setup.** The helper sets `config.timeout_graceful_shutdown` to `graceful_timeout` when it
   is unset, so an open SSE stream cannot block the exit forever. It builds a `uvicorn.Server`
   and replaces that instance's `capture_signals`.
2. **Signals.** The replacement installs handlers with `signal.signal`, as uvicorn does, and
   restores the previous handlers on exit. Off the main thread it installs none, as uvicorn
   does, and the app drives `runtime.drain()` itself.
3. **On the first SIGTERM or SIGINT,** the handler schedules a drain task on the loop with
   `call_soon_threadsafe`. The task awaits `runtime.drain(drain_budget, announce_delay=…)` and
   sets `server.should_exit` in a `finally`, so a drain that raises still lets the server
   exit. Uvicorn then:
   - stops accepting;
   - finishes in-flight requests within the graceful timeout;
   - runs the lifespan's shutdown.

   The runtime scope exits, and `shutdown()` closes the gate and tears down. The phases have
   already run, so they do not run again.
4. **On a second signal,** the handler cancels the drain task and sets both `should_exit` and
   `force_exit`. Setting `force_exit` alone would not stop a drain in progress.
5. **After the server returns,** the helper does not re-raise the signal, as uvicorn's own
   `capture_signals` does. The embedding process decides how it exits.

**An app that runs `uvicorn app:app` from the command line** keeps uvicorn's handling. Uvicorn
closes every WebSocket at once with 1012 before the lifespan's shutdown runs the phases, and
without `--timeout-graceful-shutdown` an open SSE stream blocks the exit. The docs say so, and
point such apps at the helper, or at least at a finite graceful timeout.

**The time budget** must fit inside the orchestrator's grace period:
`announce_delay + drain_budget + graceful_timeout + drain_timeout + the step timeouts`.

### 5.6 Alternatives considered

- **Flip the drain gate early.** Readiness would fail in time, but operations would be refused
  for the whole announcement window. Rejected.
- **Run drain phases as drainables.** `stop_all` runs after the gate has closed, and under
  uvicorn after the server stopped accepting and closed the WebSockets. Rejected.
- **Give steps the runtime.** A step built with `runtime=` cannot sit in that runtime's own
  plan (§2). Rejected for state on the context.
- **Patch uvicorn's `handle_exit`.** Overriding `capture_signals` on our own server instance is
  narrower, and survives uvicorn changing how it records signals. Chosen.

## 6. Tests

- **Readiness and startup.** With a slow later startup step, readiness is 503 until startup
  finishes, through `build_runtime` and not by calling hooks by hand.
- **Announcing.** `begin_draining()` makes readiness 503 on the listener and the FastAPI route,
  while a new operation still runs. `shutdown()` afterwards still drains the gate.
- **Phases.**
  - They run once, in order, within the budget.
  - An overrunning phase is cancelled, and the next still runs.
  - A phase with less than the floor left is skipped.
  - `shutdown()` without an earlier `drain()` runs them.
  - `shutdown()` after `drain()` does not run them again.
  - A partial startup failure runs none.
- **Steps read the context.** A step placed in its own runtime's plan sees `started` and
  `announced` change.
- **Readiness checks.**
  - A failing probe, a probe that times out, and a healthy one each give the expected answer
    and body.
  - The listener and the FastAPI route answer identically from the same probes.
- **Uvicorn**, as a subprocess test that sends SIGTERM to an embedded server:
  1. readiness fails;
  2. a phase records that it ran;
  3. the server exits within the graceful timeout, even with an SSE stream open;
  4. teardown runs;
  5. the process exits 0.
  - A second SIGINT during a slow phase cancels it and exits at once.
  - A drain that raises still exits.

## 7. Docs

- **"Graceful deploys"** on the runtime page:
  - the three stages;
  - `begin_draining`, drain phases and their budget;
  - the full budget arithmetic against the orchestrator's grace period;
  - `announce_delay` against the balancer;
  - `ctx.lifecycle` for step authors.
- **The FastAPI page:** `serve_uvicorn`, and what an app run with `uvicorn app:app` gets
  instead.
- **The probes page:** `checks=`, and readiness waiting for startup.

## 8. Out of scope

- **The connection drains themselves** (RFC 0075).
- **Admin endpoints** that call `begin_draining()` over HTTP. An app can add one in a few
  lines, and exposing it safely is the app's call.
- **Other ASGI servers** (Hypercorn, Granian). The helper is uvicorn's. Others can follow the
  same shape once someone embeds one.

## 9. Risks

- **`runtime.ready` and `runtime.draining` change meaning.**
  - `ready` now waits for startup, and `draining` turns true earlier.
  - Inside forze, the only readers are the two readiness answers (`probes.py:232`,
    `routes/health.py:97`), which want exactly the new readings.
  - The changelog names the change, and the split between `runtime.draining` and
    `ctx.drain_gate.draining`.
- **Budgets that exceed the orchestrator's grace period** get the process killed mid-phase.
  The docs give the arithmetic in 5.5.
- **Uvicorn internals.** `capture_signals` is a public-looking but undocumented method. A
  uvicorn release that drops it would break the helper. The helper checks that the method
  exists, and a test runs against the supported range.

## 10. Unresolved questions

- The floor of the uvicorn range the helper supports. `capture_signals` predates the
  project's 0.42 floor. Implementation confirms it, and raises the floor if needed.
- Whether `drain_budget` lives on the runtime's settings or is passed to `shutdown()`. The
  lean is the settings, next to `drain_timeout`.

## 11. Decisions

| # | Grade | Decision |
| --- | --- | --- |
| 1 | `LOCKED` | `begin_draining()` only announces: `runtime.draining` turns true and `ready` false, while the drain gate stays open until `shutdown()`. |
| 2 | `LOCKED` | Drain phases are registered on the context and run by `runtime.drain()` once per scope, after the announcement and `announce_delay`, in ascending order within one budget. `shutdown()` runs `drain()` first if it has not run and startup finished. |
| 3 | `ASSUMED` | A phase that overruns its budget is cancelled and logged. A phase left with less than the floor is skipped. Later phases still run. |
| 4 | `ASSUMED` | `probe_listener_step` takes `checks` and `check_timeout`, reads its state from the hook's `ctx`, and shares one probe evaluator with `attach_readiness_route`. |
| 5 | `ASSUMED` | `serve_uvicorn` overrides `capture_signals` on its own server instance, with `signal.signal` on the main thread and no handlers elsewhere. The first signal drains, then sets `should_exit` in a `finally`; a second cancels the drain and sets both exit flags. It sets a finite graceful timeout when none is set. |
| 6 | `ASSUMED` | Apps running `uvicorn app:app` keep uvicorn's signal handling. The docs say what that costs: WebSockets closed at once, and an unbounded wait without a graceful timeout. |
| 7 | `OPEN` | The supported uvicorn floor for the helper, and where `drain_budget` is configured. |
| 8 | `LOCKED` | The lifecycle state (`started`, `announced`) lives on the execution context, and steps read it from their hook's `ctx`, because a step cannot hold the runtime whose plan contains it. `runtime.ready` requires `started`. |

## 12. Phasing

1. **`ctx.lifecycle`, readiness waiting for startup, and `probe_listener_step(checks=…)`** with
   the shared evaluator. This is the cheapest piece, and fixes the early-ready bug.
2. **`begin_draining()`, drain phases and `runtime.drain()`.**
3. **`serve_uvicorn`.**
