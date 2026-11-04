# RFC 0084 — Load and soak kit for apps built on forze

- **Status:** 📝 Draft — row 1 (does this belong in forze at all) is `OPEN` and decides the
  rest. Once it is answered, P1 is one PR. The soak's gauge half (P4) waits on RFC 0083 and on
  instruments that do not exist yet (§3.6).
- **Scope:** A small library, `forze_load`, under a new `load` extra, that drives a running app
  built on forze over HTTP and writes one JSON line per step or window. It covers:
  - several principals, each logged in once and refreshed in one place;
  - an open and a closed load model;
  - a step ramp that names the step where goodput stops rising;
  - realtime subscribers with send-to-delivery latency and a reconnect storm that resumes from
    `Last-Event-ID`;
  - a soak that flags upward drift in server RSS, CPU and forze's gauges;
  - a compare against a saved run;
  - the client's own CPU and event-loop lag, so a saturated client is never read as a slow server.

  Scenarios stay app code: the kit resolves an operation's method and path from the app's
  OpenAPI document and can generate read-only requests, but it does not invent command payloads.
  The forze-side changes are small and land in their own packages:
  - one OpenAPI vendor key, `x-forze-kind`, on governed routes (P2);
  - process RSS/CPU gauges and the missing pool, hub and mailbox gauges (prerequisites of P4,
    proposed as separate changes);
  - RFC 0083's one scrape for every worker of a pre-fork server, which the soak's gauge reads
    depend on.

  No contract, port or runtime change. It does **not** run inside the DST loop, generate load
  from several hosts, fake command payloads from schemas, gate forze's own CI, or ship a locust
  or k6 plugin (§5.1).
- **Related:**
  - [`src/forze/application/execution/operations/descriptors.py:154-204`](../src/forze/application/execution/operations/descriptors.py)
    (`OperationCatalogEntry`, `kind`, `is_read_only`) and
    [`registries.py:623`](../src/forze/application/execution/operations/registry/registries.py)
    (`FrozenOperationRegistry.catalog`);
  - [`src/forze_fastapi/routes/_attach.py`](../src/forze_fastapi/routes/_attach.py)
    (`_route_openapi_extra` `:346-398`, `attach_operation_routes` `:871`, `operation_id=op`
    `:1065`);
  - [`src/forze_fastapi/routes/authn.py:450`](../src/forze_fastapi/routes/authn.py)
    (`attach_authn_routes`) and
    [`src/forze_identity/authn/services/refresh_token.py:18-22`](../src/forze_identity/authn/services/refresh_token.py)
    (refresh lifetime, `reuse_grace`);
  - [`src/forze_fastapi/realtime/sse.py`](../src/forze_fastapi/realtime/sse.py) (resume
    `:406-437`, route `:537`) and [`ws.py:383`](../src/forze_fastapi/realtime/ws.py);
  - [`src/forze_fastapi/routes/health.py:176-247`](../src/forze_fastapi/routes/health.py)
    (`attach_metrics_route`, single process only);
  - [`src/forze/base/primitives/sketch.py:44`](../src/forze/base/primitives/sketch.py)
    (`DDSketch`);
  - [`src/forze_dst/loop.py:255-286`](../src/forze_dst/loop.py) (the simulation loop refuses
    real I/O) and [`src/forze_cli/app.py:61-63`](../src/forze_cli/app.py) (subcommand groups);
  - [`src/forze_mock/server/control.py:390-398`](../src/forze_mock/server/control.py) (the
    served mock's control plane, the kit's test target);
  - [`tests/perf/gate_compare.py`](../tests/perf/gate_compare.py) and
    [`justfile`](../justfile) `perf-save` / `perf-check` (the baseline compare this mirrors);
  - RFC 0083 (one scrape for every worker of a pre-fork server, issue
    [#838](https://github.com/morzecrew/forze/issues/838)), drafted in parallel and not yet in
    this directory: P4 depends on it;
  - [RFC 0075](0075-realtime-connection-drain.md) (connection counting and drain responses the
    storm must treat as reconnects).
- **Discussion:** [#839](https://github.com/morzecrew/forze/issues/839).
- **Origin:** Two consumer harnesses that rebuilt the same core. erp-backend's
  `app/tests/perf/stress.py` (about 670 lines, uncommitted on branch `test/stress-harness` on
  2026-11-05, and still changing) has the three modes the issue lists: ramp, SSE with a
  reconnect storm, and soak. hh-backend-v2's `scripts/load_*.py` (about 4,000 lines) measures
  its published load envelope.

---

## 1. Summary

- **The open question first (§5.1, row 1).** Two consumers already wrote live load harnesses.
  Each re-implemented the same core, and neither uses locust or k6. Most of each harness is
  app-specific and stays so: role mixes, domain bots, infrastructure sampling. The lean is a
  **thin library in forze** that owns the shared core and the forze-only knowledge. The close
  second is a test-backed recipe plus the two small forze-side changes. A locust or k6 plugin
  and a home inside `forze_dst` both lose (§5.1).
- **What only forze can supply is mostly not built yet.**
  - The catalog knows read from write (`OperationCatalogEntry.kind`), but OpenAPI does not say
    it, and list routes are `POST`.
  - Of the gauges the issue names, connection pools, SSE/WS hub subscribers, mailbox depth and
    bulkhead in-use do not exist (§3.6).
  - A pre-fork server cannot be scraped as one process until RFC 0083 lands.
  - So P1 ships the generic core, and the forze-only parts follow as each prerequisite lands.
- **P1, one PR:**
  - `forze_load` with the closed and open models, the step ramp and its knee;
  - a principal pool with forze's password login and API keys;
  - tenancy headers and the client's self-report;
  - JSONL output, `compare`, and `forze load run | compare`.
- **Later:** P2, the catalog (`x-forze-kind`, generated read scenarios, id sampling). P3,
  realtime (subscribers, send-to-delivery latency, a storm that resumes). P4, the soak (RSS,
  CPU and gauge drift), gated on RFC 0083 and the missing instruments.
- **Fail closed on data:** writes are off by default, generated scenarios are reads only, and a
  write-enabled run refuses any host outside an allowlist (rows 8–9).

## 2. Motivation

forze tests correctness under load-like conditions: DST workloads, campaigns and simulated
latency, all on a virtual clock. It also benchmarks its own code paths (`tests/perf`). Nothing
helps an app drive itself, running for real, under concurrent load. The two consumers that
needed it each wrote a harness.

**erp-backend** (`app/tests/perf/bench.py` and `stress.py`, live HTTP over httpx and uvloop):

- `POPULATION` signs clients in as `demo-<role>` users in shares, and `MIXES` gives each role
  weighted calls over `SCENARIOS`, about 30 hand-written `(method, path, body)` builders on
  `/erp/...` routes.
- `Sessions` signs in once per role and signs in again when the cookie lapses, under a lock per
  role.
- `Sample` reads ids from the seeded data once, through the app's own search routes.
- `ramp` doubles clients from 1 to 256 with 30 s steps and a warm-up. It stops when more than
  5 % of calls fail. The model is closed: "no pause between calls".
- `sse` grows event streams from 100 to 2000 on `/realtime/events` and times each write until
  each stream reads the `changed` it causes, all on one `perf_counter`. It then drops every
  stream at once and reconnects them. It does not send `Last-Event-ID`, so the storm measures
  reconnects, not resume.
- `soak` keeps a fixed load for hours. `_drift` compares the first quarter with the last and
  fits each container's memory in MB an hour (`statistics.linear_regression`).
- `Stack` samples container CPU and memory from cgroup files, Postgres commits and backends,
  and Valkey memory. It records the harness's own CPU (`resource.getrusage`), "near one core, it
  is the client that saturated, not the stack". Every row is a JSON line.

**hh-backend-v2** (`scripts/load_play.py`, `load_tournament.py`, `load_lobby.py`,
`load_mix.py`, live HTTP plus Socket.IO, and `pages/load-envelope.md`):

- Steps through tables of bots with think time and measures move-to-echo p50/p95/p99 against a
  150 ms budget. It names a knee ("between 96 and 128 tables of six",
  `pages/load-envelope.md:34`).
- Records outbox lag, per-plane CPU and memory from `docker stats`, and Postgres and Redis
  counters. It also records the driver's own CPU and event-loop lag, "past a core its queue is
  what move → echo measures".
- Reads forze's internals by restating key names: the Redis fleet hash
  `{fleet:hh:sockets}:h` and the `dlock:hh:realtime:courier:*` leases (`load_play.py:110-116`),
  "this script does not import the platform". A rename inside forze breaks it silently.
- Its load runs have already driven forze RFCs ([0069](0069-session-revocation-fast-path.md)
  and [0070](0070-outbox-consume-mode.md) both cite them as origin).

**Shared between the two:**

- a step loop with a stop rule;
- percentiles per step and per scenario;
- JSON lines;
- the client's own CPU;
- sign-in that survives expiry;
- ids sampled from live data;
- stream fan-out with a storm;
- a soak drift fit.

**Not shared, and staying app code:** the mixes and request bodies, the domain bots and
correctness checks (hh counts reorders and gaps against its own `seq`), and infrastructure
sampling through Docker, cgroups, `pg_stat_*` and Redis.

**What each one lacked from forze:**

- erp's scenario list is paths typed by hand, so a renamed route turns into 404 load.
- hh had no supported way to read connection counts, so it read forze's Redis keys.

bi-backend, eis-backend, ai-stylist, bx-backend and tb-chat-analysis have no load harness.
None of the consumer repos uses locust, k6 or pytest-benchmark. Neither harness runs in CI.

## 3. Current state

### 3.1 Operations, routes and OpenAPI

- `OperationCatalogEntry` joins a descriptor with the plan's `kind` (`QUERY` or `COMMAND`),
  `supports_idempotency_key`, `required_permissions`, `requires_authn` and `deadline`
  (`descriptors.py:154-199`). `is_read_only` is `kind is QUERY` (`:202-204`). The default kind
  is `COMMAND` (`operations/planning/plans.py:75`), so an unmarked operation counts as a
  write, which is the fail-closed default for a load kit too.
- There is no `idempotent` or `destructive` field. `commutative` (`descriptors.py:79`) is a
  DST declaration that no runtime code reads.
- `forze ops call` already treats "read-only, or carrying an idempotency key" as safe to repeat
  (`forze_cli/ops/call.py:186`).
- `attach_operation_routes` registers each route with `operation_id=op` and `name=op`, the
  operation key verbatim (`_attach.py:1058-1071`). It marks the endpoint with
  `__forze_governed_operation__` (`:1051`, `middlewares/bypass.py:24`). No function returns a
  route-to-operation map.
- `_route_openapi_extra` emits:
  - an optional `Idempotency-Key` header;
  - `x-required-permissions`, `x-requires-authn` and `x-deadline-seconds`;
  - `x-forze-query`, the filter surface (`_attach.py:346-398`).

  It does **not** emit `kind`.
- Document routes: `GET /{id}`, `POST ""`, `PATCH /{id}?rev=`, `DELETE /{id}`, and
  `POST /list | /raw_list | /list_cursor | /raw_list_cursor | /agg_list`
  (`routes/document.py:61-90`). Search routes are `POST /typed | /raw | ...`
  (`routes/search.py:38-41`). **The HTTP verb does not tell read from write.**
- Ids are typed by the DTO, `UUID` by default (`_attach.py:558-577`). Pagination is in the
  `POST` body: `page`/`size` with `size` up to 10,000, or `size`/`after`/`before`
  (`forze_kits/dto/paginated.py:44-92`).

### 3.2 Authentication

- `attach_authn_routes` (`routes/authn.py:450`) serves:
  - `POST /login` with `{login, password}`;
  - `POST /refresh` with `{refresh_token}`;
  - `POST /logout`, password and API-key routes;
  - a cookie mode.
- Responses carry `access_token`, `refresh_token`, `access_expires_in` and `refresh_expires_in`
  (`forze_kits/aggregates/authn/dto.py:86-107`). Access tokens last 15 minutes
  (`forze_identity/authn/services/access_token.py:33`), refresh tokens 7 days
  (`refresh_token.py:19`).
- **Refresh rotation has no grace by default** (`reuse_grace = timedelta(0)`,
  `refresh_token.py:22`). Presenting a rotated token counts as reuse: it revokes the whole family
  and answers 401 (`forze_identity/authn/adapters/token_lifecycle.py:318-346`). Two clients that
  share a refresh token and refresh concurrently log each other out.
- **Password hashing caps login throughput.** Argon2 uses 100 MiB per hash, and
  `hashing_concurrency=4` queues the rest (`forze_identity/authn/services/password.py:18-31`).
  A run that logs in 200 principals at once is a load test of hashing.
- Lockout is off by default (`authn/execution/deps/module.py:178`). When enabled it answers 429
  `login_locked` after 5 failures in 15 minutes
  (`forze/application/integrations/authn/lockout.py:106-125`). A wrong password in a plan must
  stop the run at the first failure.
- API keys go through `HeaderApiKeyAuthn` and need no refresh
  (`forze_fastapi/security/resolvers.py:163-242`).

### 3.3 Tenancy

- The tenant comes from the token's `tid` claim or the `X-Tenant-Id` header
  (`forze/application/contracts/tenancy/tenant_hint.py:17`). The claim wins, and a different
  header fails with `tenant_conflict` (`:39-60`).
- A raw header is trusted only with `trust_tenant_header=True`
  (`forze_fastapi/middlewares/security.py:57`). A principal with several memberships and no
  requested tenant gets no tenant bound (`forze_identity/tenancy/adapters/resolver.py:48-80`).
- `POST /tenants/{id}/activate` mints a token pair for a tenant (`routes/tenancy.py:76-79`).

### 3.4 Realtime

- **SSE** (`attach_realtime_sse_route`, `sse.py:537`): `GET` with `protocol`, `device_id` and
  `topics`, plus an ack `POST`.
  - Frames carry `id:` only for durable signals (`sse.py:240-254`).
  - `Last-Event-ID` beats the stored cursor (`:406-437`). An id no longer retained falls back
    to the cursor. Live-only streams ignore it (`:563`).
  - The mailbox keeps 1000 entries per principal (`forze_kits/integrations/realtime/mailbox.py:48`).
    Replay stops after 8 rounds, and the client must reconnect
    (`forze/application/integrations/realtime/replay.py:82`, `sse.py:748-749`).
  - The live leg is at-most-once. Each subscriber has a queue of 256 that drops when full and
    counts the drop in `hub.dropped` (`forze_fastapi/realtime/hub.py:120-126`, `:174`, `:274`).
  - While draining, a new stream gets 200 with a jittered `retry:` and
    `X-Error-Code: realtime_draining` (`sse.py:270-286`).
- **WS** (`attach_realtime_ws_route`, `ws.py:383`) resumes from a `last_event_id` query
  parameter (`:637`) and closes with 1012 while draining (`:118`, `:475-477`). Socket.IO resumes
  from the server cursor only (`forze_socketio/connection.py:408-430`).
- **No frame carries a commit time.** The event id is a `uuid7()` minted at stage time, inside
  the transaction and before commit (`forze/application/execution/outbox/enrichment.py:42`).
  The relay's `occurred_at` and HLC headers are not forwarded to SSE subscribers
  (`forze_fastapi/realtime/lifecycle.py:164-170`).

### 3.5 Metrics: how a run can read them

- Every instrument is opt-in through an `instrument_*` call on the `"forze"` meter.
  `bootstrap_telemetry` gives each process a fresh `service.instance.id`
  (`forze/base/telemetry/bootstrap.py:242`) and pushes OTLP every 15 s by default.
- `attach_metrics_route` serves Prometheus text from a `PrometheusMetricReader`, for **single
  process deployments only**. Under a pre-fork server "gauges flap between workers"
  (`health.py:190-195`; `pages/docs/running-in-prod/grafana-stack.md:255-274`). RFC 0083 (#838)
  proposes one scrape for every worker.
- In-process readers exist but are not reachable from a client: `inspect()`
  (`resilience/executor.py:383`), `pool_stats()` (`contracts/tenancy/routed_client_base.py:151`),
  mailbox `stats()`, and `hub.subscribers` / `hub.dropped`. No debug route exposes them.
- Nothing reads process RSS or CPU time: no `getrusage`, `psutil` or `/proc` in `src`.

### 3.6 The gauges the issue names, against what exists

| Issue's gauge | Exists? | Instrument (source) |
| --- | --- | --- |
| Bulkhead queue | yes | `forze.resilience.bulkhead.queue_depth` (`execution/observability.py:200`) |
| Bulkhead limit | adaptive only | `forze.resilience.bulkhead.limit` (`:216`); a fixed cap is not exported |
| Bulkhead in use | **no** | only `inspect()` (`contracts/resilience/admin.py:31-60`) |
| Connection pools | **no** | no meter in `forze_postgres`, `forze_redis`, `forze_mongo`, `forze_http`; only tenant-pool registry counts `forze.tenancy.pool.size` / `.capacity` (`execution/observability.py:287-293`) |
| Realtime hub (SSE/WS) | **no** | `hub.subscribers`, `hub.dropped` are attributes nobody exports |
| Socket.IO connections | yes | `forze.realtime.connections`, an up-down counter (`forze_socketio/observability.py:316`) |
| Mailbox | counters only | stored, replayed, trimmed, acked, overflowed (`forze_kits/integrations/realtime/observability.py:23-27`); no depth |
| Caches | yes | `forze.cache.l1.size` / `.capacity`, hits, misses, evictions (`integrations/document/observability.py:73-97`) |
| Process RSS / CPU | **no** | nothing |

The leaks a soak exists to catch are pool connections that are never returned, subscribers that
never leave, mailbox growth and RSS. Those are mostly the rows marked **no**.

### 3.7 What exists to build on, and what does not fit

- **`forze_dst` cannot drive a live app.** Its loop refuses sockets, pipes, subprocesses and
  thread executors (`forze_dst/loop.py:255-286`). Its workload runner is closed-loop and creates
  every task up front (`workload.py:73`). Its `cpu.py` charges virtual seconds and measures
  nothing. Its `stats.py` has no latency percentiles.
- import-linter makes `forze_dst` a protected package that only `forze_cli` may import
  (`pyproject.toml:1039-1072`), so a sibling package could not reuse it anyway. What carries
  over is a convention: JSONL with a meta header line (`forze_dst/campaign.py:287-301`).
- **Core has the percentile structure.** `DDSketch` is mergeable with 1 % relative accuracy by
  default (`forze/base/primitives/sketch.py:33`, `:44`). It has no serialization, so runs
  store quantiles, not sketches.
- **The perf suite's compare does not fit load runs.** `perf-save` and `perf-check` are
  pytest-benchmark's save and compare (`justfile:123-147`, `min:10%`). `gate_compare.py`
  compares the median of per-round `stats.min` (`tests/perf/gate_compare.py:45-118`). A minimum
  latency says nothing about capacity, and the format is pytest-benchmark's.
- **`forze_cli` has the pattern a command group needs.** Typer groups mounted with `add_typer`
  (`forze_cli/app.py:61-63`), `load_object("module:attr")` (`loader.py:66`), and an extra guard
  per group (`_compat.py:12`).
- **Packaging.** `httpx>=0.28.0` is already the `http` extra and is in `fastapi`, `oidc`,
  `sandbox-container` and `inference-http` (`pyproject.toml:101-112`, `:160-171`). No psutil,
  locust or k6 appears in `pyproject.toml` or `uv.lock`.
- **The served mock is a ready test target.** `forze mock serve` serves a real app on
  in-memory backends, and its control plane arms latency and faults
  (`forze_mock/server/control.py:390-398`). With a fixed bulkhead and armed latency, an app has
  a known capacity to find.

## 4. Goals / Non-goals

**Goals**

- One supported way to ramp, soak and storm a live forze app. It is small enough that erp's and
  hh's harnesses could keep their app-specific parts and drop the rest.
- Numbers that are right or flagged:
  - latency without coordinated omission in the open model;
  - a knee never named on a saturated client;
  - drift never computed from scrapes of different processes.
- Scenarios that cannot drift from the API: operation references fail at start when the
  operation is gone, renamed or changed kind.
- Safe by default against shared environments: reads only, writes behind an explicit opt-in and
  a host allowlist, and output that carries no bodies, tokens or ids.

**Non-goals**

- **A general-purpose load tool.** locust and k6 exist. The kit is for forze apps and assumes
  forze's authn, tenancy, OpenAPI and realtime shapes.
- **Distributed generation from several hosts.** One process, one clock (§8).
- **Payload faking for commands.** Valid domain payloads are app knowledge. Schema-driven
  faking produces 422 load, not realistic load.
- **Capacity planning.** The kit measures one deployment under one mix. What that predicts is
  the reader's call.
- **Gating forze's own CI.** The perf gate stays as it is.

## 5. Design

### 5.1 Does this belong in forze?

The issue asks this first, so this RFC answers it first, as row 1 (`OPEN`, for the maintainer).

| Option | What ships | For | Against |
| --- | --- | --- | --- |
| **A. Recipe + example** | `examples/recipes/load_harness/` (test-backed, against the served mock), a docs page, plus `x-forze-kind` and the missing gauges as their own changes | No API to keep stable. Cheapest. The forze-side changes help production observability anyway. | Becomes the third copy. erp and hh already have harnesses, and neither copied anything. The forze-only parts (gauge reading, catalog resolution, refresh rules) live in code each app re-copies. |
| **B. Thin library `forze_load`** (lean) | §5.2–§5.11, phased | Owns what both consumers rebuilt and what only forze knows: refresh and tenancy rules, catalog resolution, gauge names, resume semantics. erp's harness is uncommitted and could be ported now. | An API to maintain. The generic half overlaps tools that exist. |
| **C. locust plugin** | forze helpers on locust `User` classes | Mature UI, distributed mode | gevent-based, with a stdlib monkey-patch next to asyncio code; closed model per user; heavy dependencies (gevent, Flask) for a library whose users run asyncio and uvloop; no SSE client built in |
| **D. k6 extension** | an xk6 module or a JS helper library | Open-model executors, low client overhead | Go/JS, outside the Python toolchain and the app's code; cannot read the registry; SSE needs another extension; a second language in a Python framework |
| **E. Inside `forze_dst`** | the kit as a DST module | One "testing" package | The DST loop refuses real sockets (`loop.py:255-286`); it would add `httpx` to an extra built for simulation; a different audience (live runs, not seeds) |

**Lean: B, with A as the fallback.** The evidence (§2):

- The core is the same in both consumers.
- The rest is app-specific in both.
- Both chose Python, asyncio and httpx over existing tools.

A library that owns the core and the forze-only knowledge lets each harness keep only its mixes
and its infrastructure sampling. What would flip the lean to A: the maintainer will not keep a
load API stable, or neither consumer will port onto P1. Porting is each owner's call. If A wins,
`x-forze-kind` (§5.5) and the missing gauges (row 25) still land, because they stand on their
own.

### 5.2 Package, extra and entry point

```toml
# pyproject.toml
load = ["httpx>=0.28.0"]
```

- **`forze_load`** is a new top-level package. It imports `forze` core (for `DDSketch`) and
  `httpx`, and nothing else first-party. It does not import `forze_fastapi`: it is a client and
  restates the few wire names it needs (`Last-Event-ID`, `X-Tenant-Id`, `x-forze-kind`) as
  constants, with a test that pins each to the server's constant. It joins the protected list
  in import-linter with `forze_cli` as its importer, like every other optional package.
- **The Python API is the contract.** A plan is a value. Scenarios are callables or operation
  references.

```python
from forze_load import LoadPlan, Principal, Scenario, closed_ramp, password_login, run

plan = LoadPlan(
    base_url="http://localhost:8000",
    principals=[
        Principal("viewer", weight=4, auth=password_login("demo-viewer", password_env="DEMO_PW")),
        Principal("planner", weight=1, auth=password_login("demo-planner", password_env="DEMO_PW"),
                  tenant="0b1c…"),
    ],
    scenarios=[
        Scenario.op("products.list", body={"size": 50}),            # resolved via OpenAPI
        Scenario.op("products.get", path={"id": sample("products.list")}),
        Scenario.call("plans.create", make_plan_request, writes=True),  # app-written
    ],
    mixes={"viewer": {"products.list": 2, "products.get": 3},
           "planner": {"products.get": 1, "plans.create": 1}},
    allow_writes=False,      # plans.create is refused at validation while this is False
)

await run(plan, closed_ramp(steps=[1, 2, 4, 8, 16, 32, 64], step_s=30, warm_s=5),
          out="ramp.jsonl")
```

- **`forze load`** is a thin Typer group in `forze_cli`:
  `forze load run module:plan --profile ramp|open|soak ... --out FILE` and
  `forze load compare BASE HEAD [--fail-on ...]`. It loads the plan with `load_object`, as
  `forze dst` and `forze ops` do, behind a `require_load()` guard.

### 5.3 The run engine and the two load models

- **Closed model** (`closed_ramp`, `closed`): N workers, each sends, waits for the answer, then
  sends again, with optional think time. This is what erp and hh do and what the issue asks for
  ("concurrency 1 → N"). It finds capacity: throughput stops rising when the server is full.
  Its latency is **service time under a closed loop**. When the server stalls, the workers stop
  sending, and the stall is recorded once instead of once per request that should have been
  sent. This is coordinated omission. Closed-model latency rows are labelled
  `"latency": "closed"` and the docs say not to read them as an SLO.
- **Open model** (`open_rate`, `open_ramp`): arrivals are scheduled at a rate, constant or
  Poisson (`random.expovariate` on a seeded `Random`), whatever the answers do. **Latency is
  measured from the intended send time**, so a stall shows up in every request scheduled during
  it. Sends that start late because the client fell behind are counted (`behind_schedule`) and
  feed the saturation flag (§5.10). In-flight requests are capped at the rate times the request
  timeout, the most that can be outstanding while each request either answers or times out, so
  a dead server cannot grow the client without bound. A send refused at the cap is counted as
  `transport`, never dropped silently.
- **Client:**
  - one `httpx.AsyncClient` for requests, with `Limits(max_connections=<concurrency or cap>)`;
    httpx's default of 100 connections would queue inside the client above 100 workers and be
    measured as server latency;
  - a separate client for streams, because each stream holds a connection;
  - a per-request timeout from the plan, or from `x-deadline-seconds` plus a margin when the
    operation declares one.
- **Outcome classes,** counted per step and scenario:
  - `ok` (2xx);
  - `conflict` (409, expected under concurrent writes);
  - `shed` (429, load shedding);
  - `deadline` (504);
  - `forbidden` (401/403);
  - `not_found` (404, see §5.5);
  - `client_error` (other 4xx);
  - `server_error` (5xx);
  - `transport` (connect, read or timeout).

  **Goodput** counts `ok` only. A step where `forbidden` or `client_error` passes 5 % of calls
  is marked `misconfigured`. Everything answering 403 quickly looks like excellent throughput,
  and the knee search refuses it.
- **Warm-up:** each step's first `warm_s` is excluded from its stats, and so are logins and id
  sampling.

### 5.4 Several principals

- A `Principal` has:
  - a name;
  - a weight, the share of workers that run as it, by smooth weighted round robin, as erp's
    `_roles` does;
  - an `auth` that returns request headers;
  - an optional tenant;
  - its mix.
- Built-in `auth` helpers:
  - `password_login(login, password_env=...)` for forze's `POST /login` and `POST /refresh`;
  - `api_key(header="X-API-Key", key_env=...)`;
  - `bearer(token_env=...)`.

  Any async callable returning headers also works (row 12). Credentials come from environment
  variables, never from the plan's source.
- **One session per principal, shared by its workers; refresh in one place.** A per-principal
  lock serializes refresh, as erp's `Sessions` does. The session refreshes ahead of
  `access_expires_in`, and every worker of that principal uses the result. No two workers ever
  present the same refresh token, because with `reuse_grace=0` the second presentation revokes
  the family (§3.2).
- **Logins are staggered** (at most `hashing_concurrency`-many at once, default 4, settable) and
  are not measured. The first failed login stops the run before any load, which keeps a wrong
  password from tripping the lockout.
- **Tenancy:** a principal's tenant is sent as `X-Tenant-Id` only when the plan names one.
  `tenant_conflict` or `tenant_mismatch` stops the run. Load spread over several tenants
  exercises tenant pools (`forze.tenancy.pool.*`). The docs show `POST /tenants/{id}/activate`
  for principals with several memberships.

### 5.5 Scenarios from the app's routes (P2)

- **Resolution.** `Scenario.op("products.get")` names an operation key. At start the kit reads
  the OpenAPI document, from `GET /openapi.json` or from a file for deployments that disable
  docs. It finds the operation whose `operationId` is that key and takes its method, path and
  parameters. An operation that is missing, or whose `x-forze-kind` disagrees with the
  scenario's declared `writes`, **stops the run before load** (row 7).
- **`x-forze-kind`** is the one forze-side change: `_route_openapi_extra` adds
  `"x-forze-kind": "query" | "command"` from `entry.kind` (`_attach.py:346-398`). It is
  additive: every governed route gains one vendor key.
- **Generated read sweep.** `Scenario.reads(prefix=...)` produces one scenario per `query` route
  whose input is empty, paginated, or an id with optional `rev`. Ids come from §5.5.1. Routes
  with other inputs are listed as skipped, not guessed. `command` routes are never generated.
  DST's create-then-consume heuristic (`forze_dst/derive.py:118`) is not reused: `forze_dst` is
  protected, and the kit generates no writes.

#### 5.5.1 Sampling ids from live data

- `sample("products.list", field="id", size=500)` calls the list operation **as each principal
  that will use it**, once, before the first measured step, and keeps the ids that principal
  can see. An id visible to one principal may be forbidden to another under authz or tenancy.
- Samples are bounded by `size`, which the route caps at 10,000.
- Output never contains sampled ids (row 15).
- A sampled id that later answers 404, deleted by a write scenario or by someone else, counts
  as `not_found` and is dropped from the sample. It is not an error.

### 5.6 Step ramp and the knee

- `closed_ramp(steps, step_s, warm_s, stop_error_rate=0.05)` grows workers step by step. It
  never restarts the run, so sessions and connections stay warm.
- `open_ramp(rates, ...)` does the same with arrival rates.
- Each step row holds:
  - workers or rate, calls and goodput rps;
  - p50/p90/p95/p99/p99.9/max from a `DDSketch` per step and per scenario;
  - the outcome classes and the client self-report.
- **Stop rule:** errors (`server_error` + `transport`) above `stop_error_rate`, or a
  `misconfigured` step.
- **Knee (row 16):** goodput stops rising.
  - The knee is the first step *k* whose goodput is less than 5 % above step *k−1*'s, and
    *k+1* confirms it (also under 5 % above *k−1*).
  - A step marked `client_saturated` or `misconfigured` is never named. The summary says
    "no knee found below saturation" instead.
  - In the open model the knee is the first rate where p99 more than doubles over the previous
    rate, or where goodput falls more than 5 % below the offered rate.

  The summary names the knee and the steps either side of it.

### 5.7 Realtime: send-to-delivery latency and the storm (P3)

- **Subscribers.**
  - `Realtime(transport="sse" | "ws", path=..., subscribers=N, principals=..., topics=...)`
    opens N streams, spread over principals, with `device_id` per subscriber.
  - Streams are opened in batches.
  - Recorded: open time p50/p95, failed opens and open streams.
- **Writes and matching.**
  - A write scenario runs at a set rate.
  - The plan supplies `match(frame) -> key` and `key_of(response) -> key`. The frame shape is
    the app's payload; erp matches `data.ids`.
  - The first arrival per subscriber is timed. Duplicates are counted: durable signals can
    arrive twice, through the mailbox and the live race. A key unseen after `timeout_s` is
    `missing`.
- **Two latencies on one clock (row 19).** The writer and every subscriber are in one process,
  on one monotonic clock, so no clock alignment is needed:
  - **send → delivery**, from the write's send to the frame. It is an upper bound that includes
    the write's own latency, and is what erp measures;
  - **answer → delivery**, from the write's response to the frame. It can be negative when the
    live leg beats the HTTP answer; that is reported, not clipped.

  True commit time is not observable. No frame carries it, and the uuid7 event id is minted
  before commit on the server's wall clock (§3.4). The docs say so.
- **Client side drops.**
  - A subscriber that reads slower than frames arrive fills its 256-slot server queue, and the
    live leg drops (`hub.py:120-126`).
  - The kit reads frames in a dedicated task per stream and parses lazily.
  - Delivered fraction below 1 on an unsaturated client is a server finding. On a saturated
    client it is flagged as the client's.
- **Storm.**
  - Every stream drops at once and comes back in one batch.
  - Each SSE subscriber sends `Last-Event-ID` with the last durable id it saw. A WS subscriber
    sends `last_event_id` as a query parameter.
  - Writes keep running throughout, so gaps are measurable.
  - Recorded:
    - time until all streams are back (`back_in_s`);
    - frames replayed;
    - **gaps**, keys written while the stream was down that never arrived;
    - duplicates;
    - streams that resumed from the cursor because their id aged out of the mailbox's 1000.
  - A live-only route ignores `Last-Event-ID` (`sse.py:563`). There the storm records
    `resume: "none"` and measures reconnects only.
  - Draining answers are reconnects with the server's `retry:`, never errors:
    - SSE: 200 with `retry:` and `X-Error-Code: realtime_draining`;
    - WS: close 1012.
- Socket.IO is not in P3 (§8). hh would need it, and it resumes from the server cursor only.

### 5.8 Soak (P4)

- **Steady load.** A fixed closed or open load and optional subscribers run for a duration.
  A row is written every `window_s` (default 60 s).
- **Server samples.** In each window the kit scrapes `/metrics` and keeps `forze_*` families
  plus the process gauges of row 24. A small stdlib parser covers the text format; P4 adds no
  dependency.
- **Drift (row 26).**
  - After a warm-up of at least two windows, each series gets an OLS slope over time
    (`statistics.linear_regression`, as erp's `_drift`) and a first-quarter against
    last-quarter mean.
  - A series is **flagged** when its slope over the run exceeds 10 % of its first-quarter mean
    and the last quarter is above the first.
  - A gauge with a matching capacity gauge (`forze.cache.l1.size` against `.capacity`) is not
    flagged once it reaches capacity. A cache filling up is not a leak.
  - Flags never fail the run; the reader decides.
- **One process per series (row 22).**
  - The kit records the scraped `service.instance.id`, as the exporter renders the resource.
    If consecutive scrapes name different instances, the scrape is landing on different
    workers: the kit marks every gauge series `interleaved` and computes no drift from it.
  - RFC 0083 removes that limit for pre-fork servers. Until it lands, the soak's gauge half
    covers single-process apps only. Client-side numbers and drift work everywhere.
- Writes during a soak grow the data, so the workload changes as it runs. Rows record goodput
  next to latency, so a slowdown can be told apart from more data. erp already records
  `db_mb_growth` for that reason.

### 5.9 Output and baseline compare

One JSON object per line. The first line is meta, then one row per step, window or phase, then
a summary, as `forze_dst.campaign.write_records` does:

```json
{"kind": "meta", "schema": 1, "forze_load": "0.23.0", "target": "http://localhost:8000",
 "openapi_sha256": "9f2c…", "model": "closed", "client": {"cpus": 8, "python": "3.13.1",
 "loop": "uvloop"}, "plan": {"principals": {"viewer": 4, "planner": 1}}, "started_at": "…"}
{"kind": "step", "workers": 32, "duration_s": 30.0, "calls": 9120, "goodput_rps": 301.2,
 "latency": "closed", "ms": {"p50": 41.0, "p95": 120.3, "p99": 210.9, "max": 803.0},
 "outcomes": {"ok": 9050, "conflict": 12, "shed": 0, "not_found": 3, "server_error": 0,
 "transport": 0}, "client": {"cpu_cores": 0.61, "loop_lag_p99_ms": 4.1, "saturated": false},
 "scenarios": {"products.get": {"calls": 5470, "ms": {"p95": 98.2}}}}
{"kind": "summary", "knee": {"workers": 64, "goodput_rps": 512.0}, "stopped": null}
```

- **What output never holds:** bodies, headers, tokens, credentials or sampled ids. Only counts,
  latencies and outcome classes (row 15). `target` is written without userinfo, query or
  fragment, parsed with `urlsplit`.
- **`compare BASE HEAD`** matches rows by `kind` plus `workers` or `rate` or `window`, and
  scenarios by name. It prints per-row deltas for goodput, p95 and p99. Rows whose meta differs
  (`openapi_sha256`, `model`, client cores) are compared with a warning.
- **Report only by default.** `--fail-on p95:15%,goodput:10%` exits 1 when a threshold is
  crossed (row 28). Load runs vary far more than micro-benchmarks, so a default failure would be
  a flaky gate.
- The format is not pytest-benchmark's, and `gate_compare.py` is not reused (§3.7).

### 5.10 The client's self-report

- **CPU:** `time.process_time()` delta over the wall-clock delta for each step, in cores. This
  is portable; `resource` is Unix only.
- **Event-loop lag:** a probe sleeps 50 ms in a loop and records the overshoot, as hh does. The
  row reports its p99.
- **Saturation:** a step is `client_saturated` at 0.85 cores or more, a loop lag p99 above
  25 ms, or `behind_schedule` above 1 % of scheduled sends. One event loop runs Python on one
  core, so past that point the client's queue is what the latency measures.
- Several client processes (row 32) are not in P1–P4. Merging their results needs `DDSketch`
  serialization, which does not exist.

### 5.11 Guards against destructive writes

- **Off by default (row 8).** `allow_writes=False`. Under it, plan validation refuses:
  - any `Scenario.call` declared `writes=True`;
  - any `Scenario.op` whose `x-forze-kind` is `command`.

  `writes` has no default on `Scenario.call`, so every free callable states its effect.
  Validation runs before any login.
- **Host allowlist (row 9).** With `allow_writes=True`, the target host, parsed with
  `urlsplit` and compared exactly, must be a loopback address or `localhost`, or be listed in
  the plan's `write_hosts` or in `FORZE_LOAD_WRITE_HOSTS`. A substring test would accept
  `localhost.example.com`.
- **Never generated:** delete, restore and every other `command` route. An app that wants
  deletes under load writes them itself and opts in.
- **No cleanup.** The kit does not undo writes. The docs say to run write-enabled plans against
  a seeded, disposable stack, as erp's `compose.stress.yml` and hh's `compose.load.yml` do.

### 5.12 Capabilities at a glance

| Capability (issue) | Needs from forze | Cost | Main pitfalls | Phase |
| --- | --- | --- | --- | --- |
| Scenarios from routes | `x-forze-kind` (one line, `_attach.py:346-398`); `operation_id=op` already | S | verb ≠ kind; command payloads are app code; ids are per principal; OpenAPI may be off in prod | P2 |
| Several principals | nothing new | S | refresh reuse revokes the family; Argon2 queue of 4; lockout; tenant selection | P1 |
| Step ramp + knee | nothing | S–M | closed vs open; client saturation; httpx's 100-connection default; misconfigured 403 steps | P1 |
| Realtime latency + storm | nothing to start; a frame timestamp would be optional | M | one clock only; live leg at-most-once and 256-slot queue; duplicates; mailbox cap 1000; replay rounds; drain answers | P3 |
| Soak drift (RSS, CPU, gauges) | RFC 0083; process gauges; the missing gauges (§3.6) | M + prerequisites | interleaved scrapes; caches filling up; GC sawtooth; data growth | P4 |
| JSON output + baseline | nothing | S | noise; matching rows; environment fingerprint | P1 |
| Client CPU self-report | nothing | XS | one loop = one core; loop lag inflates latency | P1 |

## 6. Tests

All of the kit's own tests run without Docker. Timing assertions run on an injected clock, or
under `-m perf`, which `just test` excludes.

- **Engine (unit, `tests/unit/test_forze_load/`):** the clock and sleep are injected, and
  `httpx.MockTransport` answers with synthetic latency on that clock.
  - **Coordinated omission:** the fake server stalls for 1 s. The open model reports the stall
    in every request scheduled during it (p99 ≈ 1 s). The closed model records it once per
    worker, and its rows are labelled `closed`.
  - The in-flight cap refuses past the bound and counts the refusals.
  - Outcome classes for each status, and the 5 % `misconfigured` rule.
  - The httpx limit equals the worker count.
- **Knee (unit):** synthetic goodput curves.
  - Linear then flat: the knee is at the bend.
  - Noisy plateau: one dip is not a knee without confirmation.
  - Saturated or misconfigured steps are never named.
  - The stop rule fires at 5 % errors.
- **Principals (unit):**
  - A fake authn that revokes the family on reuse. Concurrent workers of one principal trigger
    exactly one refresh, and a sabotaged lock reproduces the 401 storm.
  - Logins are staggered to the configured concurrency.
  - The first failed login stops the run.
  - `X-Tenant-Id` is sent only when named, and `tenant_conflict` stops the run.
- **Guards (unit):**
  - A `command` op and a `writes=True` call are refused while writes are off.
  - A `Scenario.call` without `writes` is a type error.
  - A non-loopback host is refused, including `localhost.example.com` and `127.0.0.1.nip.io`.
    `FORZE_LOAD_WRITE_HOSTS` is matched exactly.
- **Output (unit):**
  - The JSONL schema round-trips.
  - `compare` deltas and `--fail-on`.
  - Redaction: a run whose server echoes the `Authorization` header and bodies leaves neither
    in the file.
  - `target` is stripped of userinfo and query.
- **Parsers (unit):**
  - SSE frames: `id`, `event`, `data`, `retry`, keepalive comments, and multi-line data.
  - The Prometheus text subset.
  - Wire-name constants pinned to `forze_fastapi`'s, from a test that may import both.
- **Realtime (unit, P3):**
  - The storm sends `Last-Event-ID` (SSE) and `last_event_id` (WS) with each subscriber's last
    durable id.
  - Draining answers count as reconnects.
  - Duplicates are counted once, and gaps are computed from keys written while down.
- **Drift (unit, P4):** synthetic series. Flat is not flagged. A linear leak is flagged. A
  sawtooth is not flagged. A series that fills to its capacity gauge is not flagged.
  Interleaved instance ids give no drift.
- **`x-forze-kind` (unit, `tests/unit/test_forze_fastapi/`, P2):** query and command routes
  carry the key with the right value. Unflagged routes are otherwise unchanged.
- **Example (test-backed, `examples/recipes/load_ramp/`):** the served mock with a fixed
  bulkhead and armed latency, under uvicorn in a subprocess. A short ramp (1, 2, 4, 8 workers at
  1 s) runs against it.
  - `tests/unit/test_examples/test_load_ramp.py` asserts the file's shape and that no step
    errored.
  - A `-m perf` twin asserts that goodput plateaus within a wide band of limit ÷ latency.
- **Not tested:** real backends' capacity, several hosts, cross-clock latency.

## 7. Docs

- A how-to, `pages/docs/testing/load-and-soak.md`, under Testing in `pages/zensical.toml`:
  - a plan, a ramp, reading the knee, a compare;
  - principals and refresh;
  - the write guard;
  - realtime (P3) and soak (P4) as they land.
- An explanation section on that page:
  - open against closed;
  - coordinated omission;
  - why closed-model latency is not an SLO;
  - client saturation;
  - why send-to-delivery is an upper bound.
- A reference for the JSONL schema, versioned by `schema`.
- **Warnings worded plainly:**
  - write-enabled plans write real data and the kit never cleans it up;
  - do not point it at production;
  - gauge drift needs one scrape per process, so on a pre-fork server it needs RFC 0083.
- A `CHANGELOG.md` `Added` entry per phase. P2's `x-forze-kind` gets its own line, because it
  changes every app's OpenAPI document additively.

## 8. Out of scope

- **Several generator hosts.** They need clock alignment for cross-host latency and result
  merging. Named as the escape hatch, not built. Revisit when one process measurably saturates
  before a consumer's server does.
- **Several generator processes on one host.** Demand-gated (row 32). It needs `DDSketch`
  serialization in core.
- **A Socket.IO client.** hh uses Socket.IO, but its bots are domain logic that the kit would
  not replace. Revisit if hh ports its non-domain parts onto the kit.
- **A Prometheus query source** for OTLP-push deployments. Demand-gated (row 23). Reading the
  run window from the deployment's Prometheus-compatible API is the alternative to RFC 0083 for
  apps that already push.
- **Fault injection against live environments.** The served mock's control plane and DST cover
  faults. A load kit that injects faults into a shared environment fails open.
- **Schema faking for command payloads** (§4).
- **A UI or dashboards.** JSONL is the interface; plotting is the user's.
- **Infrastructure sampling** (Postgres, Redis, cgroups, Docker). It is deployment-specific,
  and both consumers already do it their own way.

## 9. Risks

- **The third copy.** If neither consumer ports onto P1, forze keeps an API nobody uses.
  Mitigation: row 1 is `OPEN`. P1 is small, and the docs page shows erp's ramp rewritten on it.
  Whether to port is each owner's call.
- **Numbers read as more than they are.**
  - A knee on one machine with one mix is not capacity.
  - Closed-model latency is not an SLO.
  - Send-to-delivery includes the write.

  Mitigation: rows carry `latency: closed`, `client_saturated` and `interleaved` labels, and
  the docs explain each.
- **The client is the bottleneck.** httpx's per-request CPU cost bounds one process, and that
  bound is unmeasured. Mitigation: the self-report flags it. P1 measures the single-process
  ceiling against the served mock and records it in the docs. Multi-process waits for demand.
- **A write-enabled run against a shared environment.** Mitigation: off by default, an exact
  host allowlist, and no generated writes. Residual: an operator who lists a shared host on
  purpose. The docs warning is the only defense there.
- **The soak half reads as unfinished** until RFC 0083 and the missing gauges land. That is
  stated in the status, §3.6 and §5.8, rather than shipped as a soak that silently reads one
  worker in four.
- **Coupling to forze's wire names.** The kit restates `Last-Event-ID`, `X-Tenant-Id` and
  `x-forze-kind`, with a test that pins them to the server's constants. This is the supported
  version of what hh does with Redis keys.

## 10. Unresolved questions

- **Row 1 (does it belong) is the maintainer's** and gates everything else. Rows 2–3 follow
  from it.
- **RFC 0083's shape** decides how the soak tells processes apart. Shared snapshots keep
  `service.instance.id` as a label per series. Fan-out may render it differently. Settle row 22's
  detection rule when 0083 is decided.
- **Process gauges on macOS.** The standard library has no current RSS there (`ru_maxrss` is the
  peak). Settle in the process-gauge change (row 24): Linux `/proc/self/statm` only, or peak on
  other platforms, labelled as such.
- **Whether erp or hh would port.** It is evidence for row 1, asked of the owners, not decided
  here.

## 11. Decisions

| # | Grade | Decision |
| --- | --- | --- |
| 1 | `OPEN` | **Does the kit belong in forze?** Options: (A) a test-backed recipe and docs page only; (B) a thin `forze_load` library; (C) a locust plugin; (D) a k6 extension; (E) a module inside `forze_dst`. **Lean B**, with A as the fallback (§5.1). Two consumers rebuilt the same core and kept the rest app-specific; both chose Python, asyncio and httpx over existing tools. C and D fight the app's toolchain and cannot read forze's registry. E's loop refuses sockets. Flips to A if the maintainer will not keep a load API stable, or no consumer will port. The maintainer decides. |
| 2 | `OPEN` | **Packaging.** Options: a new `forze_load` package under a `load` extra (`httpx` only); inside `forze_dst`; inside `forze_cli`. **Lean: a new package.** httpx is already an extra elsewhere, so the cost is one extra line. `forze_dst` is simulation-only and protected. `forze_cli` would pull Typer into a library import. |
| 3 | `OPEN` | **Entry point.** Options: Python API only; a `forze load` Typer group only; both. **Lean: both.** The Python API is the contract, because plans hold callables. `forze load run module:plan` and `forze load compare` are thin wrappers using `load_object`, as `forze dst` and `forze ops` do. |
| 4 | `OPEN` | **Load model.** Options: closed only (what erp and hh do); open only; both. **Lean: both.** Closed for the concurrency ramp the issue asks for, which finds capacity. Open for latency and for soak, where coordinated omission matters. Closed-model rows are labelled `latency: closed`. |
| 5 | `ASSUMED` | Open-model latency runs from the intended send time. Late sends are counted as `behind_schedule`. In-flight requests are capped at rate × request timeout, and a send refused at the cap counts as `transport`. |
| 6 | `OPEN` | **Catalog source.** Options: (a) the OpenAPI document (URL or file) plus a new `x-forze-kind` vendor key; (b) the registry and FastAPI routes in-process through `forze_cli`, which may import `forze_fastapi`; (c) both. **Lean (a).** It is what a client sees, works against any deployment, needs no app import, and the forze change is one line in `_route_openapi_extra`. (b) would tie the kit to running beside the app's code. |
| 7 | `ASSUMED` | An operation reference that is missing from OpenAPI, or whose `x-forze-kind` contradicts the scenario's declared `writes`, stops the run before any load. Generated scenarios are `query` routes whose input is empty, paginated or id/rev. Other inputs are listed as skipped. There is no schema faking. |
| 8 | `LOCKED` | **Writes are off by default and fail closed.** Generated scenarios are never `command`s. A `Scenario.call` must state `writes` (no default). A write scenario is refused at validation unless `allow_writes=True`. The kit points at shared environments. Relaxing this later cannot undo data already written, and every plan written against the relaxed default would change meaning. |
| 9 | `OPEN` | **How a write-enabled run is guarded.** Options: the flag alone; the flag plus an exact host allowlist (loopback, `localhost`, plan `write_hosts`, `FORZE_LOAD_WRITE_HOSTS`); the flag plus a target-side opt-in (the app declares itself a load target). **Lean: flag plus allowlist**, compared exactly after `urlsplit`. The target opt-in is stronger but needs a server-side surface. It is demand-gated. |
| 10 | `ASSUMED` | One session per principal, shared by its workers. Refresh runs under a per-principal lock, ahead of `access_expires_in`. No refresh token is ever presented twice: with `reuse_grace=0`, a second presentation revokes the family (`token_lifecycle.py:318-346`). |
| 11 | `ASSUMED` | Logins are staggered to at most 4 at once by default (forze's `hashing_concurrency`) and are not measured. The first failed login stops the run before load, which keeps a wrong password from tripping the lockout. |
| 12 | `OPEN` | **Credential source.** Options: built-in `password_login` (forze `POST /login`/`/refresh`), `api_key`, `bearer`; an app callable returning headers; both. **Lean: both.** Credentials come from environment variables only. The helpers cover forze's own routes, and the callable covers cookies, OIDC and anything app-specific. |
| 13 | `ASSUMED` | `X-Tenant-Id` is sent only when a principal names a tenant. `tenant_conflict` or `tenant_mismatch` stops the run. |
| 14 | `ASSUMED` | Ids are sampled per principal through list operations, once, before the first measured step, bounded by `size`. A sampled id answering 404 is `not_found` and leaves the sample. |
| 15 | `ASSUMED` | Output holds counts, latencies and outcome classes only: no bodies, headers, tokens, credentials or ids. `target` is written without userinfo, query or fragment. |
| 16 | `ASSUMED` | Knee rule: the first closed-model step whose goodput is under 5 % above the previous step's, confirmed by the next step. Never a `client_saturated` or `misconfigured` step. Open model: the first rate where p99 more than doubles, or where goodput falls more than 5 % below the offered rate. Thresholds may be tuned during P1 against the served mock; log the values chosen. |
| 17 | `ASSUMED` | A step where `forbidden` plus `client_error` exceed 5 % is `misconfigured`, stops a ramp, and is never a knee. Goodput counts 2xx only. |
| 18 | `ASSUMED` | Percentiles come from core's `DDSketch` (1 % relative accuracy), per step and per scenario. Rows store quantiles and counts, not sketches, because no serialization exists. |
| 19 | `OPEN` | **Realtime latency measure.** Options: (a) send → delivery and answer → delivery on the one process's monotonic clock; (b) the uuid7 event id's embedded time against the client clock, with an estimated offset; (c) a new server timestamp in frames. **Lean (a).** No clock alignment is needed, and it is what erp measures. The docs call send → delivery an upper bound. (b) is pre-commit and cross-clock. (c) changes the realtime protocol for a test tool. |
| 20 | `ASSUMED` | Frame matching is app-supplied (`match(frame)`, `key_of(response)`). The first arrival per subscriber is timed. Duplicates and `missing` keys after a timeout are counted. |
| 21 | `ASSUMED` | The storm resumes each subscriber from its last durable id: `Last-Event-ID` on SSE, the `last_event_id` query on WS. Writes continue during it. Gaps, duplicates and cursor fallbacks are counted. Draining answers (SSE 200 + `retry:` + `X-Error-Code: realtime_draining`, WS 1012) are reconnects, not errors. A live-only route records `resume: "none"`. |
| 22 | `LOCKED` | **Gauge drift is computed only from scrapes of one process.** If consecutive `/metrics` scrapes name different `service.instance.id`s, every gauge series is marked `interleaved` and no drift is computed. Under a pre-fork server this depends on RFC 0083 (#838). Computing drift from interleaved scrapes produces confident false findings that later work would build on. |
| 23 | `OPEN` | **Gauge source.** Options: the app's `/metrics` (single process now, pre-fork after RFC 0083); the deployment's Prometheus-compatible query API over the run window; both. **Lean: `/metrics` first.** It matches RFC 0083's audience of compose stacks with no collector. The query API is demand-gated, for apps that push OTLP. |
| 24 | `OPEN` | **Server RSS/CPU source.** Options: (a) process gauges exported through forze's meter (stdlib `/proc/self/statm` and `time.process_time`, a new `instrument_process`) and read with the other gauges; (b) the kit reads `/proc/<pid>` for PIDs it is given (same host only); (c) cgroup files (containers, same host); (d) none, and leave it to the user's monitoring. **Lean (a)**, as its own small change that P4 depends on. It works through the same scrape and RFC 0083 covers pre-fork. (b)/(c) are what erp and hh already do for their stacks. |
| 25 | `OPEN` | **The missing gauges** (§3.6): connection pool checked-out and idle per backend, SSE/WS hub subscribers and dropped, mailbox depth, bulkhead in-use. Options: add them under this RFC as part of P4; propose them as separate issues. **Lean: separate issues.** Each is production observability first, and the kit reads whatever `/metrics` serves. |
| 26 | `ASSUMED` | Drift: after two warm-up windows, an OLS slope and a first- against last-quarter mean per series. Flagged when the slope over the run exceeds 10 % of the first-quarter mean and the last quarter is higher. Not flagged once a gauge reaches its capacity gauge. Flags never fail the run. |
| 27 | `ASSUMED` | Output is JSONL: a meta line, rows, a summary, with `schema: 1`, following `forze_dst.campaign`'s convention. It is not pytest-benchmark's format, and `gate_compare.py` is not reused. |
| 28 | `OPEN` | **Compare semantics.** Options: report only; exit 1 beyond default thresholds; report by default with an opt-in `--fail-on p95:15%,goodput:10%`. **Lean: report plus opt-in.** Load runs are far noisier than the micro-benchmarks `perf-check` gates at 10 %. A default failure would make every consumer's CI flaky. |
| 29 | `ASSUMED` | One `httpx.AsyncClient` for requests with `Limits(max_connections=...)` set to the worker count or in-flight cap, and a separate client for streams. httpx's default of 100 connections would queue in the client above 100 workers. |
| 30 | `ASSUMED` | Client self-report: `time.process_time()` over wall time per step, and a 50 ms loop-lag probe. `client_saturated` at ≥ 0.85 cores, loop lag p99 > 25 ms, or `behind_schedule` > 1 %. |
| 31 | `ASSUMED` | `forze_load` imports `forze` core and `httpx` only. It restates the wire names it uses as constants pinned by a test to `forze_fastapi`'s. It joins import-linter's protected list with `forze_cli` as an allowed importer. |
| 32 | `OPEN` | **Several client processes.** Options: none (single process, flagged when saturated); a process pool in P1; demand-gated later. **Lean: demand-gated.** Merging needs `DDSketch` serialization in core, and P1 first measures the single-process ceiling against the served mock. |
| 33 | `ASSUMED` | The kit's own tests use injected clocks, `httpx.MockTransport` and the served mock (`forze_mock.server`). No real backends. Timing-band assertions run only under `-m perf`. |

## 12. Phasing

- **P0** — the maintainer answers row 1, and with it rows 2 and 3. Nothing below starts before.
- **P1** — one PR (§5.2–§5.4, §5.6, §5.9–§5.11):
  - `forze_load` and the `load` extra;
  - plans, principals with `password_login`, `api_key` and `bearer`, and tenancy headers;
  - the closed and open engines with outcome classes and the in-flight cap;
  - `closed_ramp`, `open_ramp` and the knee;
  - the client self-report;
  - JSONL output and `compare`;
  - the write guard;
  - `forze load run | compare`;
  - the unit tests in §6, the `load_ramp` example and its test;
  - the docs page and the changelog.

  Operation references are not in P1. Scenarios are `Scenario.call` with `writes` stated.
- **P2** — one PR: `x-forze-kind` in `forze_fastapi` with its test; `Scenario.op` resolution
  with the stop-on-mismatch rule; `Scenario.reads` and `sample`.
- **P3** — one PR: SSE and WS subscribers, send → delivery and answer → delivery, the storm with
  resume.
- **P4** — gated on RFC 0083 for pre-fork servers, on the process-gauge change (row 24), and on
  whichever missing gauges (row 25) have landed: the soak, the `/metrics` parser, the
  one-process rule (row 22) and drift.
- **Demand-gated:** several client processes (row 32), the Prometheus query source (row 23), a
  target-side write opt-in (row 9), and a Socket.IO client (§8).
