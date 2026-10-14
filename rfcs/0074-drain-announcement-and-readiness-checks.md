# RFC 0074 — Drain announcement and readiness checks

- **Status:** 📝 Draft
- **Scope:** Three runtime additions for graceful deploys:
  - `runtime.begin_draining()` turns readiness off without refusing operations;
  - registered drain phases run between that announcement and teardown;
  - the stdlib probe listener gains the dependency checks the FastAPI readiness route
    already has.

  Plus a `forze_fastapi` helper that serves an embedded uvicorn with SIGTERM routed into that
  order, rather than swallowed.

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

- **`runtime.begin_draining()`** makes readiness fail and `runtime.draining` true at once,
  while operations keep being admitted. A load balancer stops routing new work; work already
  routed finishes normally.
- **`runtime.drain(budget)`** announces, waits `announce_delay` for the balancer to notice,
  then runs the drain phases that steps registered, in order, within `budget`.
  `shutdown()` runs it first if nobody did, so the phases run on every exit path.
- **`serve_uvicorn(config, runtime)`** serves an embedded uvicorn whose SIGTERM and SIGINT
  start `runtime.drain()` and only then tell uvicorn to exit. The usual teardown follows
  through the lifespan.
- **`probe_listener_step(runtime, checks=…)`** answers readiness from the runtime and from
  dependency probes, sharing one evaluator with the FastAPI route.

## 2. Motivation

**Readiness flips too late.**
- `runtime.ready` is `not ctx.drain_gate.draining`, and the only thing that sets `draining`
  is `shutdown()`, through `drain_gate.drain()` (`runtime.py:340-368`, `context/drain.py:178`,
  `runtime.py:442`).
- So readiness fails at the same moment new operations start being refused. A balancer still
  routing to the replica sends it requests that are refused.
- Flipping the gate early to fix readiness would refuse operations early instead.

**A deploy wants this order:**
1. readiness fails;
2. the app's own drain phases run: couriers hand over, sockets close gently;
3. teardown.

hh-backend-v2 builds that order by hand (`runner.py:171-321`).

**Embedded uvicorn swallows SIGTERM.**
- `uvicorn.Server.serve()` wraps itself in `capture_signals()`. That replaces the process's
  SIGTERM and SIGINT handlers with one that sets `should_exit` at once (uvicorn 0.51,
  `server.py:316-345`).
- An app that embeds uvicorn and installs its own handler loses it for as long as `serve()`
  runs. Every restart becomes an immediate exit or a kill at the grace period, with no
  announcement and no drain (D-274).
- Every app that embeds uvicorn hits this.

**The stdlib probe cannot check dependencies.**
- `probe_listener_step` answers from `runtime.ready` and `runtime.draining` only
  (`probes.py:226-232`).
- Only FastAPI's `attach_readiness_route` takes probes:
  `probes: Mapping[str, DepKey]`, each answering `health()`, with a timeout
  (`routes/health.py:37-44`, `:152`).
- A worker without HTTP, or one serving socket.io only, has no way to fail readiness when its
  database is gone. hh-backend-v2's listener exists for exactly that.

## 3. Current state

All verified against the tree at the time of writing.

- **The runtime.** `ExecutionRuntime.draining` returns `ctx.drain_gate.draining`, and `ready`
  returns its negation, both `False` outside a scope (`runtime.py:340-368`).
- **`shutdown()` runs in this order** (`runtime.py:440-474`):
  1. `drain_gate.drain(drain_timeout)`, which refuses new operations, waits for in-flight
     ones, and cancels them on expiry;
  2. `ctx.drainables.stop_all`;
  3. closing the background owners;
  4. each lifecycle step's shutdown, bounded by `shutdown_step_timeout`;
  5. resetting the context.
- **The probe listener.** `probe_listener_step(runtime, *, host, port, path_ready,
  path_live, step_id)` is a stdlib HTTP listener. It is itself a drainable
  (`probes.py:105-155`, `:276-284`).
- **The FastAPI route.** `attach_readiness_route(router, runtime, *, path, probes,
  probe_timeout)` runs each probe's `health()`. There is no shared health type: each
  integration's client port declares `health() -> Awaitable[tuple[str, bool]]`.
- **The lifespan.** `runtime_lifespan` checks the websocket allowlist and the bypass paths,
  then enters `runtime.scope()` (`forze_fastapi/lifespan.py:42-53`).
- **Uvicorn.** Nothing in `forze_fastapi` runs uvicorn. The only runner is the mock server's
  `uvicorn.run` (`forze_mock/server/runner.py:195-219`), and nothing touches
  `capture_signals`. The project requires `uvicorn>=0.42`, and 0.51 is installed.

## 4. Goals / Non-goals

**Goals**

- Readiness can fail before any operation is refused.
- Steps can register work that must run between that moment and teardown, with one budget
  for all of it, and it runs on every exit path.
- An embedded uvicorn exits only after that work, on SIGTERM as on SIGINT.
- One readiness evaluator, used by both the stdlib listener and the FastAPI route.

**Non-goals**

- **Changing when operations are refused.** The gate still closes in `shutdown()`.
- **Owning the process.** forze offers the uvicorn helper; apps that run `uvicorn app:app`
  keep uvicorn's own signal handling (5.4).
- **Liveness checks.** Liveness stays "the process answers".

## 5. Design

### 5.1 Announcing a drain

```python
class ExecutionRuntime:
    def begin_draining(self) -> None: ...      # idempotent; announces only
    @property
    def draining(self) -> bool: ...            # announced, or the gate is draining
    @property
    def ready(self) -> bool: ...               # a scope is active and not draining
```

- `begin_draining()` sets an announcement flag on the scope, so `draining` turns true and
  `ready` false.
- It does not touch the drain gate. Operations are still admitted until `shutdown()` closes
  the gate.
- Steps that need the difference read `ctx.drain_gate.draining` for "operations are refused",
  and `runtime.draining` for "this replica is leaving". The fleet membership step (RFC 0073)
  reads the latter.

### 5.2 Drain phases

```python
ctx.drain_phases.register(
    name: str,
    phase: Callable[[timedelta], Awaitable[None]],   # receives the budget left
    *,
    order: int = 0,
)

async def drain(self, budget: timedelta, *, announce_delay: timedelta = timedelta(0)) -> None
```

`runtime.drain()` runs at most once per scope:
1. It calls `begin_draining()`.
2. It waits `announce_delay`, so the balancer can notice that readiness failed.
3. It runs the registered phases in ascending `order`, each given the budget left.

A phase that overruns the budget is cancelled and logged, and the next phase still runs with
what is left. That is zero, so it gets one chance to return at once.

`shutdown()` calls `drain()` first if it has not run, with `drain_budget` from the runtime's
settings. Phases therefore run on every exit path, and a deploy that called `drain()` earlier
does not run them twice.

Phases are not drainables. Drainables are loops that `stop_all` stops after the gate closes.
Phases are work that must happen while the replica still serves: a courier handing its routes
back, sockets closing gently over a window.

### 5.3 Readiness checks on the probe listener

```python
def probe_listener_step(
    runtime: ExecutionRuntime,
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

- **One evaluator.** The probe evaluation moves out of `forze_fastapi/routes/health.py` into a
  core helper next to the listener. It resolves each key, calls `health()` under the timeout,
  and reports the names that failed. `attach_readiness_route` and the listener both call it,
  so they answer the same way from the same probes.
- **The answer.** Readiness is 503 while the runtime is not ready, or while any check fails or
  times out. The body names the failing checks, in the same shape the FastAPI route uses.
- **Liveness** ignores the checks.

### 5.4 Serving uvicorn

```python
async def serve_uvicorn(
    config: uvicorn.Config,
    runtime: ExecutionRuntime,
    *,
    drain_budget: timedelta,
    announce_delay: timedelta = timedelta(0),
) -> None: ...
```

1. The helper builds a `uvicorn.Server` and replaces its `capture_signals` with a context that
   installs forze's own handlers on the running loop, restoring the previous ones afterwards.
2. **On the first SIGTERM or SIGINT,** the handler starts a task that awaits
   `runtime.drain(drain_budget, announce_delay=…)`, then sets `server.should_exit`. Uvicorn
   then stops accepting, finishes in-flight requests within its own graceful timeout, and
   runs the lifespan's shutdown. The runtime scope exits, and `shutdown()` closes the gate and
   tears down. The phases have already run, so they do not run again.
3. **A second SIGINT** sets `force_exit`, as uvicorn does.
4. **After the server returns,** the helper does not re-raise the signal, as uvicorn's own
   `capture_signals` does. The embedding process decides how it exits.

**An app that runs `uvicorn app:app` from the command line** keeps uvicorn's handling. The
phases then run inside the lifespan's shutdown, after uvicorn has stopped accepting. Its
graceful timeout may already have closed long-lived connections by then. The docs say so, and
point such apps at the helper, or at a pre-stop hook that calls `runtime.drain()`.

### 5.5 Alternatives considered

- **Flip the drain gate early.** Readiness would fail in time, but operations would be refused
  for the whole announcement window. That is the failure a deploy is trying to avoid.
  Rejected.
- **Run drain phases as drainables.** `stop_all` runs after the gate has closed, and, under
  uvicorn, after the server stopped accepting and possibly closed the connections the phases
  want to close gently. Rejected.
- **Patch uvicorn's `handle_exit`.** Overriding `capture_signals` on our own server instance
  is narrower, and survives uvicorn changing how it records signals. Chosen. The
  dependency's floor is checked when the helper is built (§10).

## 6. Tests

- **Announcing.** `begin_draining()` makes readiness 503 on the listener and the FastAPI route,
  while a new operation still runs. `shutdown()` afterwards still drains the gate.
- **Phases.**
  - They run once, in order, within the budget.
  - An overrunning phase is cancelled, and the next still runs.
  - `shutdown()` without an earlier `drain()` runs them.
  - `shutdown()` after `drain()` does not run them again.
- **Readiness checks.**
  - A failing probe, a probe that times out, and a healthy one each give the expected answer
    and body.
  - The listener and the FastAPI route answer identically from the same probes.
- **Uvicorn**, as a subprocess test that sends SIGTERM to an embedded server:
  1. readiness fails;
  2. a phase records that it ran;
  3. the server exits;
  4. teardown runs;
  5. the process exits 0 without being killed.

  A second SIGINT forces the exit.

## 7. Docs

- **"Graceful deploys"** on the runtime page: the three stages, `begin_draining`, drain
  phases, the budget against the orchestrator's grace period, and `announce_delay` against
  the balancer.
- **The FastAPI page:** `serve_uvicorn`, and what changes for apps run with `uvicorn app:app`.
- **The probes page:** `checks=`.

## 8. Out of scope

- **The connection drains themselves** (RFC 0075).
- **Admin endpoints** that call `begin_draining()` over HTTP. An app can add one in a few
  lines, and exposing it safely is the app's call.
- **Other ASGI servers** (Hypercorn, Granian). The helper is uvicorn's. Others can follow the
  same shape once someone embeds one.

## 9. Risks

- **`runtime.draining` changes meaning.** Code that read it as "operations are refused" now
  sees true earlier. Inside forze, its only readers are the two readiness answers
  (`probes.py:232`, `routes/health.py:97`), which want exactly the earlier reading. The docs and the changelog name the split between `runtime.draining`
  and `ctx.drain_gate.draining`.
- **Budgets that exceed the orchestrator's grace period** get the process killed mid-phase.
  The docs give the arithmetic: `announce_delay + drain_budget + drain_timeout` plus the step
  timeouts must fit inside the grace period.
- **Uvicorn internals.** `capture_signals` is a public-looking but undocumented method. A
  uvicorn release that drops it would break the helper. The helper checks that the method
  exists, and a test pins the uvicorn versions in the support range.

## 10. Unresolved questions

- The floor of the uvicorn range the helper supports. `capture_signals` is in 0.51; the
  project's floor is 0.42. Implementation checks, and raises the floor if needed.
- Whether `drain_budget` lives on the runtime's settings or is passed to `shutdown()`. The
  lean is the settings, next to `drain_timeout`.

## 11. Decisions

| # | Grade | Decision |
| --- | --- | --- |
| 1 | `LOCKED` | `begin_draining()` only announces: `runtime.draining` turns true and `ready` false, while the drain gate stays open until `shutdown()`. |
| 2 | `LOCKED` | Drain phases are registered on the context and run by `runtime.drain()` once per scope, after the announcement and `announce_delay`, in ascending order within one budget. `shutdown()` runs `drain()` first if it has not run. |
| 3 | `ASSUMED` | A phase that overruns its budget is cancelled and logged, and later phases still run. |
| 4 | `ASSUMED` | `probe_listener_step` takes `checks` and `check_timeout`, and shares one probe evaluator with `attach_readiness_route`, so both answer identically. |
| 5 | `ASSUMED` | `serve_uvicorn` overrides `capture_signals` on its own server instance. The first SIGTERM or SIGINT runs `runtime.drain()` and then sets `should_exit`, and a second SIGINT forces the exit. |
| 6 | `ASSUMED` | Apps running `uvicorn app:app` keep uvicorn's signal handling. Their phases run at lifespan shutdown, and the docs say what that costs. |
| 7 | `OPEN` | The supported uvicorn floor for the helper, and where `drain_budget` is configured. |

## 12. Phasing

1. **`probe_listener_step(checks=…)` and the shared evaluator.** It is the cheapest piece and
   independent of the rest.
2. **`begin_draining()`, drain phases and `runtime.drain()`.**
3. **`serve_uvicorn`.**
