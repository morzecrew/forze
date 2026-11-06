---
title: Load testing
icon: lucide/gauge
summary: Ramp a running app as several principals, find where its throughput stops rising, soak it for drift, time realtime delivery, and compare two runs
---

A generic load tool knows nothing about your app. It signs in as one admin, sends the same
request in a loop, and reports latency that silently includes its own queue once it runs out
of CPU. `forze_load` drives your running app over HTTP as the principals that really use it,
step by step, and writes one JSON line per step: goodput, latency percentiles, outcome
classes, and whether the client itself was the bottleneck. Install the `load` extra, and
`cli` for the `forze load` command.

## Write a plan

A plan names the target, who signs in, and what each of them sends. A scenario is an async
function that sends one request through the `Call` it is given; `Call.request` adds the
principal's credential and tenant headers. It takes a path relative to the plan's base URL:
a URL with a scheme or a host, such as a pagination `next` link, is refused before anything
is sent, so the credential never leaves the target the guard checked. The run's clients
refuse any request to another origin, from a scenario or a credential, the same way.

```python
from forze_load import Call, LoadPlan, Principal, Scenario, api_key, password_login


async def list_products(call: Call):
    return await call.request("POST", "/products/list", json={"size": 20})


async def create_product(call: Call):
    name = f"load-{call.rng.randrange(10**6)}"
    return await call.request("POST", "/products", json={"name": name, "price": 1})


plan = LoadPlan(
    base_url="http://localhost:8000",
    principals=[
        Principal(name="viewer", weight=4, mix={"list": 1},
                  auth=api_key("VIEWER_API_KEY")),
        Principal(name="buyer", weight=1, mix={"list": 3, "create": 1},
                  auth=password_login("buyer@example.com", password_env="BUYER_PASSWORD"),
                  tenant="6ba7b810-9dad-11d1-80b4-00c04fd430c8"),
    ],
    scenarios=[
        Scenario("list", list_products, writes=False),
        Scenario("create", create_product, writes=True),
    ],
    allow_writes=True,
)
```

- **`writes` has no default.** Every scenario says whether it changes data, and the run
  refuses a writing scenario unless the plan sets `allow_writes=True` (see
  [Writes and shared environments](#writes-and-shared-environments)). It is a declaration the
  kit trusts, not something it can check: forze's list and search routes are `POST`, so a
  request does not show whether it writes.
- **`weight`** is the share of workers that run as a principal; **`mix`** is how often its
  workers pick each scenario.
- **Credentials come from environment variables only.** `api_key` sends a key in
  `X-API-Key` (or the header you name), `bearer` sends a pre-issued token, and
  `password_login` signs in through the `/login` and `/refresh` routes that
  `attach_authn_routes` serves. Any async callable that takes the client and returns headers
  works too, for an external IdP or a cookie-mode app. The run's clients keep no cookies,
  because every principal shares them: a cookie-mode credential returns its session as an
  explicit `Cookie` header. `password_login`'s routes are paths on the target, `/login` and
  `/refresh` by default; for authn routes mounted under a prefix, pass `login_path=` and
  `refresh_path=`. A URL with a host is refused. Each run opens a fresh session, so a plan
  can be run again.
- **`tenant`** is sent as `X-Tenant-Id`. A token's tenant claim wins over it, and the app
  reads the header only with `trust_tenant_header=True`.

## Call operations by key

A hand-typed path drifts from the API: a renamed route turns into a run of 404s. A scenario
can name the operation instead, by the key forze gives the route as its `operationId`, and
the run reads the method and path from the app's OpenAPI document when it starts:

```python
from forze_load import LoadPlan, Principal, Scenario, api_key, sample

plan = LoadPlan(
    base_url="http://localhost:8000",
    principals=[
        Principal(name="viewer", mix={"products.get": 3, "list": 1, "reads": 1},
                  auth=api_key("VIEWER_API_KEY")),
    ],
    scenarios=[
        Scenario.op("products.get", path={"id": sample("products.list")}),
        Scenario.op("products.list", name="list", body={"size": 50}),
        Scenario.reads("orders."),
    ],
)
```

- **The kind is checked.** forze marks every generated route with `x-forze-kind`, `query`
  or `command`. `Scenario.op` declares `writes=False` unless you pass `writes=True`, and the
  run stops before signing anyone in when the operation is missing from the document or its
  kind disagrees with that declaration. A route with no `x-forze-kind`, or an unknown value,
  counts as `command`: a hand-written route, or one served by a forze older than the key.
- **The inputs are checked too.** Every placeholder in the path must be filled, query names
  must be ones the route declares, and a route that requires a body must get one. `body` is
  sent as JSON, so the route must take `application/json` or another `application/*+json`
  type. It goes out as `application/json` when the route offers that, else as the route's
  own type, such as `application/merge-patch+json`. A body must carry the fields its schema
  requires at the top level. An
  operation's parameter replaces the path's of the same name and location, as OpenAPI
  says. Path values are URL-encoded. Header and cookie parameters are not checked: a
  principal's credential may be what supplies them.
- **Where the document comes from.** `openapi` defaults to `/openapi.json` on the target,
  fetched without credentials and only when the plan names an operation. For a deployment
  that serves no docs, or guards them, save the document and pass
  `openapi=Path("openapi.json")`, or the parsed document itself. When the document's first
  server is a path, as FastAPI writes a `root_path` (`/api/v1`), `base_url` must end with
  it. Every request goes under `base_url`, so an operation or path that names a server of
  its own, neither the document's nor `base_url`, is refused when referenced and skipped by
  a sweep. Server variables are read at their defaults. The meta line records the document's digest as `openapi_sha256`, so `compare` warns
  when two runs read different documents.

### Sample ids from live data

`sample("products.list", field="id", size=500)` calls the list operation once as each
principal that uses it, after sign-in and before the first step, and keeps the values of
`field` from the answer's `hits`, at most `size` of them. `size` is sent as the body's
`size`, or as the query's when the body has none, and is at most 10,000. Each call picks
one value at random. Each principal samples for itself, because an id one principal can
read may be forbidden to another.

- A sample must read a `query` operation, and an empty one stops the run before load,
  naming the principal and the operation. The body it sends, `{"size": n}` or `{}`, must
  fit the route's schema by the same rule as a sweep's (below), or the run is refused.
- An id that answers 404 later, deleted by a write scenario or by someone else, counts as
  `not_found` and leaves the sample. A request that carries two sampled values drops
  neither: the 404 does not say which one is gone. A sample emptied by 404s retires its
  scenarios for that principal, and the step row lists them under `sample_exhausted`. The
  principal's workers carry on with the rest of its mix, but the knee search stops at that
  step, because the steps after it run another mix. When nothing is left, the run ends the
  step there and stops: the summary's `stopped` says `sample_exhausted` and names the
  principal, and the step it cut short says `cut_short`. Such a step is never named the
  knee, and `compare` reports it without gating on it.
- Sampled ids never reach the output.

### Sweep the read routes

`Scenario.reads("orders.")` generates one scenario per `query` operation whose key starts
with the prefix and whose input the kit can fill without guessing:

- no input, or a JSON body whose schema accepts `{}`, such as a paginated list, sent as `{}`;
- an `id`, in the path or the query, with an optional `rev`. Ids are sampled from the `list`
  operation beside the route: `orders.list` for `orders.get`.

The body check is conservative by construction. It reads only `type`, `properties`,
`required`, `minProperties`, `maxProperties`, `enum`, `const`, `additionalProperties` and
`nullable`, plus annotations such as `title`, `description` and `default`, and judges the
exact body it would send against them. A schema with any other keyword at its top level,
such as `not`, `allOf` or `if`, is skipped rather than half read.

Every other `query` route is recorded with its reason under `plan.skipped` in the meta line,
and `forze load run` prints them: one that needs a header or a cookie, or whose body schema
cannot be resolved, uses a keyword the kit does not evaluate, or refuses `{}`, among them.
`command` routes are never generated. A mix weights the group by its name, `reads` by
default, and the weight is split evenly between the generated scenarios; the rows name each
by its operation key. An operation another scenario of the plan already calls, by its name
or as a `Scenario.op` under another name, is left to that scenario, and a sweep that
generates nothing stops the run. An explicit `Scenario.op` body is checked for its required
fields only.

## Run a ramp

Point the CLI at the plan by import string, or at a function that takes no arguments and
returns one:

```bash
forze load run myapp.load:plan --steps 1,2,4,8,16,32,64 --step-s 30 --warm-s 5 --out ramp.jsonl
```

Or call `run` from Python, which returns the same rows:

```python
import asyncio

from forze_load import ClosedRamp, OpenRamp, run

result = asyncio.run(
    run(plan, ClosedRamp(steps=[1, 2, 4, 8, 16, 32, 64], step_s=30, warm_s=5), out="ramp.jsonl")
)
latency = asyncio.run(run(plan, OpenRamp(rates=[50, 100, 200], step_s=60, warm_s=5)))
```

Pick the model by the question you are asking:

- **`ClosedRamp` finds capacity.** Each step runs that many workers; each sends, waits for the
  answer, and sends again (`think_s` adds a pause). Throughput stops rising when the server is
  full. Its latency is service time: when the server stalls, the workers stop sending, so the
  stall is counted once per worker instead of once per request that should have gone out.
  Its rows say `"latency": "closed"`; do not read them as a latency objective.
- **`OpenRamp` measures latency.** Requests arrive at each step's rate, evenly or as a Poisson
  process (`arrivals="poisson"`), whatever the answers do, and latency runs from each
  request's *intended* send time. A stall shows up in every request scheduled during it.
  In-flight requests are capped at the 1 − 10⁻⁶ quantile of a Poisson count with mean
  rate × timeout (9 at a mean of 1, 151 at 100), so a healthy server is never refused and a
  refusal means a real backlog. A refused send counts as a call and as `transport`.

Before any load, the plan is validated and every principal signs in, at most four at a time:
forze hashes passwords with Argon2 four at a time by default, and a mass sign-in would
measure that queue. The first failed sign-in stops the run, and the command exits 2. A ramp
that stops on its stop rule exits 0: the stop is a result, recorded in the summary. To gate
on a run, compare it with `--fail-on` (see [Compare two runs](#compare-two-runs) and
[Exit codes](#exit-codes)).

## Read the result

Each step's warm-up (`warm_s`) is left out of its numbers. A call counts in the step that
sent it, even when it answers after the step ended, and rates divide by the measured
window's real length. A step row carries:

| Field | Meaning |
| --- | --- |
| `goodput_rps` | 2xx answers per second |
| `sampled` | Answered calls; the latency percentiles cover these only |
| `ms` | `p50`, `p90`, `p95`, `p99`, `p999` and `max`, in milliseconds, credential time excluded |
| `outcomes` | Counts of `ok`, `conflict` (409), `shed` (429), `deadline` (504), `forbidden` (401/403), `not_found`, `client_error`, `server_error`, `transport` |
| `misconfigured` | More than 5 % of calls answered 401/403 or another unexpected 4xx |
| `unfinished` | Calls the step sent that had not finished when its row was written |
| `auth` | Time spent signing in or refreshing: `total_s` and `max_ms` |
| `client` | `loop_cpu`, `loop_cpu_judged`, `process_cpu`, `loop_lag_p99_ms`, `behind_schedule`, `saturated` |
| `scenarios` | Calls, 2xx and latency per scenario |
| `sample_exhausted` | Scenarios retired during the step, as `principal` and `scenario`, because 404s emptied their sample; present only when one was |
| `cut_short` | `true` when a stop ended the step before its window did; such a step is never the knee, and `compare` never gates on it. Present only then |

A transport failure (a refused connection, a timeout) is counted but has no latency.

The ramp stops at the first step where 5xx answers (504 included) and transport failures pass
`stop_error_rate` (default 5 %), or that is misconfigured: a mix that answers 403 quickly looks
like excellent throughput and measures nothing.

The summary names the **knee**: in a closed ramp, the first step whose goodput is less than
5 % above the previous step's, confirmed by the step after it. In an open ramp, it is the
first rate whose p99 more than doubles, with at least 100 answered calls in each step, or
whose goodput falls more than 5 % below the offered rate less its `conflict` and `not_found`
answers; either is confirmed only when the next rate repeats it. A step whose client was
saturated, that was misconfigured, or that retired scenarios (`sample_exhausted`) is never
named and never confirms a knee; the summary's `note` says why no knee was named.

## Compare two runs

```bash
forze load compare base.jsonl head.jsonl
forze load compare base.jsonl head.jsonl --fail-on p95:15%,goodput:10%
```

Steps are matched by worker count or rate, a soak's windows by their number, and goodput,
p50, p95 and p99 are printed side by side. A file that cannot be read as a run exits 2,
with or without `--fail-on`. Without `--fail-on` the command only reports: load runs vary
far more than micro-benchmarks, and a default threshold would make a CI gate flaky.
`--fail-on` names `goodput`, `p50`, `p95` or `p99`, each with a positive percentage. With
it, the command exits 1 when a named metric is worse by more than its share, or has a value
in the base run and none in the head (no call answered), and 2 when no step appears in both
runs: a gate that compared nothing does not pass. A step either run cut short (`cut_short`)
is printed with a warning and never gated, since its window was shorter; a gate left with
only such steps compared nothing, and exits 2 too.
When the two meta lines differ in any field but the start time (the target,
the client's cores, Python or event loop, the `forze_load` version, the profile, the
principal weights or mixes), the command compares anyway and names each field in a warning:
another host, runtime or mix is another measurement, not a regression.

## Soak a steady load

A ramp finds where the server fills up. A soak asks whether it stays healthy at a load it can
carry, for hours: the leaks only time shows are pool connections never returned,
subscribers that never leave, and memory that only grows. It holds one load level, writes a
row per window, and flags what drifted upward:

```bash
forze load run myapp.load:plan --steps 32 --soak-s 14400 --window-s 60 --out soak.jsonl
```

```python
import asyncio

from forze_load import Soak, run

result = asyncio.run(run(plan, Soak(duration_s=4 * 3600, workers=32), out="soak.jsonl"))
```

`Soak` takes `workers` for a closed load or `rate` for an open one (`--model open` on the
CLI), never both. On the CLI, `--steps` names the one load level, and `--step-s` and
`--warm-s`, which shape a ramp, are refused beside `--soak-s`, as `--window-s` is without
it. The windows run back to back on a fixed schedule from the load's start, whatever the
scrapes take, so every call counts in one. Nothing is sent after the last window closes: the
calls in flight drain into it, and a soak lasts its windows plus that drain and the last
scrape. Each
`window_s` (default 60 s) writes a `window` row with a step row's fields, plus:

| Field | Meaning |
| --- | --- |
| `window` | The window's number, from 1 |
| `warm` | `true` for the first `warm_windows` (default 2, at least 2), which the fit leaves out |
| `t_s` | Seconds since the load started, at the window's end, rounded to the millisecond (the fit reads the full value) |
| `server` | The scrape the window's end started: `instances` (each `service_instance_id` it named) and `series`, or `error` when it failed |

Writes during a soak grow the data, so the workload changes as it runs. Each row records
goodput beside latency, so a slowdown can be told apart from more data. The warm-up windows
are cold start, so they never stop a soak: the stop rule applies from the first window after
them. A window's row is written once its calls have drained and its scrape has answered,
while the windows after it run; rows are written in window order. A stop a row calls for
ends the window then running, written with `cut_short`, and nothing is sent after it.

### What the soak reads from the server

`LoadPlan.metrics`, `/metrics` by default, is the path on the target the soak scrapes,
without a credential: once before anyone signs in, then as every window ends, beside the
windows after it and within `timeout_s`; a scrape slower than a window overlaps the next
one. A route
that cannot be read before load stops the run, and the command exits 2; set
`metrics=None` to soak on client numbers only. A scrape that fails later is recorded as the
row's `server.error`, and the soak goes on. A point is stamped when its scrape was sent.

The soak keeps the `forze_*` series and the process gauges from `instrument_process`
(`process_cpu_time_seconds_total`, `process_memory_usage_bytes`). A counter is turned into a
per-second rate between scrapes, so `series` shows rates for counters and values for
gauges. Histograms are not fitted. The app exports only what it instruments, so serve the
route with [`attach_metrics_route`](../reference/fastapi-routes.md) and call the
`instrument_*` functions for what you want watched: `instrument_process` for memory and CPU,
`instrument_connection_pools` and `instrument_http_clients` for connections,
`instrument_resilience` for bulkheads, `instrument_realtime_hub` for SSE and WebSocket
subscribers, `instrument_document_l1` for caches, `instrument_tenant_pools` for the tenant
pool cache.

### Read the drift

After the warm-up, every series gets a least-squares slope over time and the mean of its
first and last quarter of windows. A series is **flagged** when the slope times the run's
duration passes 10 % of its first-quarter mean and the last quarter is above the first. The
threshold does not depend on `window_s`. From a first quarter of zero any rise passes 10 %,
so such a series is flagged only when every window of its last quarter is above zero, and is
`sparse` otherwise: one late event is not a leak. A gauge that reaches its capacity is not flagged,
because a cache or a pool filling up is not a leak: a cache's size against its capacity, a
connection pool's `in_use` and `idle` against its `max`, a bulkhead's `in_use` against its
`limit`, the tenant pool cache's size against its capacity. The client's p50, p95 and p99
are fitted too.

The summary's `drift` lists every `flagged` series and gives each one a `status`:
`flagged`, `steady`, `sparse`, `at_capacity`, `too_few` (fewer than four windows after the
warm-up), `inconclusive` (every point at one time, so no slope), or one of the two below. A
window a stop cut short stays out of the client's fit, since one slow answer can dominate
its few calls; its scrape still counts. When nothing could be fitted, `forze load run` says
so beside `drift: none flagged`, with the count of series per reason. Flags never fail the run: it exits 0, and reading them is yours.

**Drift is computed only from scrapes of one process.** A scrape names its process through
the `service_instance_id` label that `bootstrap_telemetry`'s resource puts on
`target_info`:

- A single-process app names the same instance in every scrape, and its series are fitted.
- A pre-fork server (`uvicorn --workers 4`) answers each scrape from whichever worker took
  it. Its series are marked `interleaved` and not fitted: a fit across processes is a
  confident false finding. A process that restarted mid-run is marked the same way.
- With [one scrape for every worker](../running-in-prod/grafana-stack.md#several-workers-one-scrape)
  (`attach_metrics_route(..., snapshots=...)`), every series carries its own
  `service_instance_id`, and each worker's series is fitted on its own. A worker's snapshot
  can be up to its interval old, so its points are stamped when the worker wrote it (the
  scrape's time less `forze_metrics_snapshot_age_seconds`), and a snapshot the previous
  scrape already read adds no point.
- Scrapes that name no instance are marked `unattributed`.

The meta line records how the first scrape ties series to a process, as
`server.attribution`: `instance`, `per_series`, `interleaved` or `none`. With the last two
no server series will be fitted, so `forze load run` warns before the load starts; the soak
still runs, and its client numbers still count. A pre-fork server without one scrape for
every worker reads as `instance` at first: only a later scrape that lands on another worker
shows it.

`compare` matches two soaks window by window. It prints the warm-up windows and never gates
on them.

## Measure realtime delivery

A ramp says how fast the app answers. It does not say how long a change takes to reach the
people watching it, or what they miss when every connection drops at once. Pass a `Realtime`
profile instead of a ramp: the run holds SSE or WebSocket streams open against
`attach_realtime_sse_route` or `attach_realtime_ws_route`, sends writes at a set rate, and
times each write's frame at every stream it should reach. Then it drops every stream at once
and measures the resume.

```python
import asyncio

from forze_load import Frame, Realtime, run


def shipped(frame: Frame):
    return frame.data.get("order_id") if frame.event == "order.shipped" else None


realtime = Realtime(
    transport="sse",  # or "ws"
    path="/realtime/sse",
    subscribers=200,
    rate=20,
    writers=["buyer"],
    match=shipped,
    key_of=lambda response: response.json()["id"] if response.is_success else None,
    audience=lambda response: ["buyer"],
    duration_s=60,
)
result = asyncio.run(run(plan, realtime, out="realtime.jsonl"))
```

- **Subscribers** are spread over `principals` (every principal by default) by weight, each
  stream signed in as its principal, with its own `device_id` and the `topics` you name. The
  app must grant the topics. A WebSocket carries the credential on its upgrade request, and
  the route's resolver reads it there. The `device_id` keys the stream's stored cursor: the
  SSE route reads it itself, while the WebSocket route takes the key from the
  `WsConnection.client` the resolver returns. `build_ws_connection_resolver` fills it from
  `device_id`; a hand-written resolver must do the same, or a principal's subscribers share
  one cursor, and a stream that reopens with no id replays from another's. Streams open in
  batches of `batch`. The kit never acks, so no cursor moves and nothing is trimmed, and every
  first open replays the principal's retained mailbox, up to 1,000 entries. On a shared
  stack that skews the mailbox metrics: every open adds to `forze.realtime.mailbox.replayed`
  and `forze.realtime.mailbox.replay_size`, and the run never adds to `acked`. Run it against
  a stack of its own. The first write
  waits until no frame has come for a quarter of a second, at most `timeout_s`, so that
  replay is over. It also waits, `timeout_s` at most, until a stream is open.
- **Writes** run for `duration_s` as open-model arrivals at `rate`: each picks one of
  `writers` (every principal by default) by weight, then a scenario from its mix. The write
  guard applies as it does to a ramp.
- **Matching is yours.** `key_of(response)` names the key a write's event will carry, or
  `None` when the write emits none. `match(frame)` reads the key back from a frame (`event`,
  `id` and `data`, the payload of forze's envelope), or `None` for a frame no write caused.
  `audience(response)` names the principals the event reaches, so a principal-addressed
  write is expected only at that principal's streams; `None`, the default, expects it at
  every stream. Use one key per write. An exception in any of the three stops the run.
- **Each delivery is timed twice, on one clock.** The writer and every subscriber run in one
  process, so no clock alignment is needed. `send_ms` runs from the write's send to the
  frame: an upper bound, because it includes the write's own latency. `answer_ms` runs from
  the write's answer: a frame can beat the answer when the live leg is faster than the HTTP
  response. Those negative times are not clipped; they are counted beside the percentiles in
  `answer_negative`, with their share and minimum. When every frame beats its answer, the
  percentiles are `null`. The true commit time is not observable: no frame carries it.
- **Missing and gaps.** The first frame per stream and key is timed; later ones count as
  `duplicates`, which a durable signal can produce through the mailbox and the live leg. A key
  a stream has not received `timeout_s` after the write's answer is a `gap` when the stream
  was down at the write's send, and `missing` otherwise. If that stream receives the key
  later still, within ten times `timeout_s` of that, it counts once as `late`; any other
  frame for a closed key counts nowhere. Every count of a write, `late` and `replayed`
  included, goes in the row of the phase that sent it.
- **The storm.** With `storm=True` (the default), every stream drops at once after the steady
  phase and reopens in one batch, each sending the last durable id it saw: `Last-Event-ID` on
  SSE, the `last_event_id` query parameter on a WebSocket. Writes run on for another
  `duration_s`. A resume whose id the mailbox no longer holds falls back to the stored cursor
  without telling the client, so a fallback shows only as gaps and duplicates. A live-only
  SSE route ignores `Last-Event-ID`: set `resume=False` there, and the row records
  `resume: "none"`.
- **Draining is a reconnect, never an error.** A draining replica answers a new SSE stream
  200 with a `retry:` and `X-Error-Code: realtime_draining`, and closes a new WebSocket with
  1012. The stream retries and counts it under `draining`. An open stream that ends, closed by
  the server or a drain step, cut by the network, or silent for `idle_s`, reopens with its
  resume and counts under `ended`.
- **Errors are retried; a 4xx refuses.** A transport failure, a 5xx (a balancer's 502 or 503
  passes), a 101 that does not complete the WebSocket handshake, or a socket closed with
  another code than 1008 or 1012 before it answered the kit's first ping counts under
  `errors`, and the stream retries. Any other
  status than 200, an answer that is not `text/event-stream`, or a WebSocket closed with 1008
  before that ping is `refused`, and that stream stays closed. When no stream is open
  `timeout_s` after the last batch or after the replay's quiet, every one refused, failing or
  dropped, the run stops before its first write with `stopped.reason: "no_stream"`.
- **Retries spread out.** A stream waits the server's `retry:`, or one second, doubled for
  each failure in a row up to 30 seconds, then jittered by half either way, so the streams do
  not return in lockstep; never less than 50 ms.
- **A silent stream is cut.** `idle_s` (60 seconds by default) bounds the wait for a byte.
  Keep it above the server's heartbeat: forze's SSE route sends a keepalive comment every 15
  seconds by default, and uvicorn pings a WebSocket every 20. Set `idle_s=None` for a server
  that sends neither.
- **The WebSocket client is strict.** It checks the handshake's `Sec-WebSocket-Accept`,
  echoes the server's close, and closes on a protocol violation: 1002 for a masked frame,
  reserved bits or a malformed control frame, 1007 for text that is not UTF-8, 1009 for a
  message over 16 MiB.

Each phase, `steady` and then `storm`, writes one row with `"kind": "phase"`. It carries the
writes' numbers as a step row does (`calls`, `goodput_rps`, `ms`, `outcomes`, `client`, …)
plus:

| Field | Meaning |
| --- | --- |
| `streams` | `subscribers`; `open` at the phase's end; `opened`, with `open_ms`; `refused`, `errors` (retried), `draining`, `ended` |
| `streams` (storm) | `back` and `back_in_s`, the time until every stream was back (`null` while one is away); `resume`, `last_event_id` or `none`; `resumed`, the streams that sent an id |
| `delivery` | `expected`, `delivered` and their `share`; `duplicates`, `missing`, `gaps`, `late`; `replayed`, frames a reopened stream received for writes sent before it reopened |
| `delivery` latency | `send_ms`, `answer_ms`, and `answer_negative` (`count`, `share`, `min_ms`) |

Each stream is read by its own task. On SSE a frame's JSON is decoded only when `match`
reads `data`; a WebSocket frame is decoded on arrival, since its event name is inside. A
stream that reads slower than its frames arrive fills its 256-slot queue on the server, and
the live leg drops what does not fit. So a `share` below 1 is a server finding
when `client.saturated` is false, and the client's own limit when it is true. The server
counts each such drop as `forze.realtime.hub.dropped` when the app calls
[`instrument_realtime_hub`](../reference/metrics.md#realtime-hub). A realtime run scrapes
nothing, so read that counter from your own metrics.

Realtime runs from Python only: `forze load run` has no realtime model, since `match` and
`key_of` are code. `forze load compare` reads step and window rows, and skips phase rows, so
two realtime runs compare nothing: without `--fail-on` the command warns, and with it, it
exits 2. The meta line records the profile's numbers, not its path, topics or callables.

## Writes and shared environments

Writes are off by default, and turning them on is not enough on its own. A run with a writing
scenario needs `allow_writes=True` **and** a target host that is a loopback address,
`localhost`, listed in the plan's `write_hosts`, or listed in the comma-separated
`FORZE_LOAD_WRITE_HOSTS` environment variable. Hosts are compared exactly, never by suffix:
`localhost.example.com` is refused. The check runs before anything is sent. An operation
reference is a writing scenario when it declares `writes=True`, which its route's
`x-forze-kind` must confirm as `command`; a sweep never generates one.

The run never cleans up what it wrote. Run write-enabled plans against a seeded, disposable
stack, never against production.

## Exit codes

`forze load` keeps 1 for a crossed threshold, so a CI job can tell a regression from a run
that never measured anything:

| Exit | `forze load run` | `forze load compare` |
| --- | --- | --- |
| 0 | The run finished, a stop included: the stop rule or an emptied sample. A soak's drift flags never fail it | The runs were compared, and no `--fail-on` threshold was crossed |
| 1 | Never: `run` has no threshold to cross | A `--fail-on` metric is worse by more than its share, or has a value in the base run and none in the head |
| 2 | The plan cannot be loaded or is refused; an operation does not resolve; the OpenAPI document or a soak's metrics route cannot be read; a credential is missing; a sign-in fails; a sample fails or comes back empty; a refresh fails after the token expired; an option is invalid; the run file cannot be written; your plan's code (its module, factory or a scenario) raises or exits, which prints its traceback | A file cannot be read as a run, with or without `--fail-on`; the `--fail-on` spec is invalid; `--fail-on` finds no whole step in both runs, as with two realtime runs; an option is invalid |

Every `forze load` command exits 2 when the `load` extra is not installed.

## Notes

- **One process drives the load, on one core.** A step's `client.saturated` is set when the
  event loop's thread uses 0.85 of a core, its lag p99 passes 25 ms, or more than 1 % of
  open-model sends start late. Past that point the client's queue is what the latency
  measures: read those rows as the client's limit, not the server's. An app served through
  `run(..., transport=...)` on the same event loop runs on that thread too, so then the loop
  CPU is reported but not judged (`loop_cpu_judged: false`).
- **One session per principal.** All of a principal's workers share its sign-in, and one of
  them refreshes it, under a lock, shortly before the access token expires. forze revokes a
  whole session family when a rotated refresh token is presented again, so two workers
  refreshing one token would sign each other out. Sign-in and refresh use their own
  connections, never the request pool. A refresh that fails in transport or with a 5xx keeps
  the current token while it is valid and retries with backoff; once the token has expired,
  the failure stops the run.
- **The file holds numbers only:** counts, latencies and outcome classes. No bodies, headers,
  tokens or ids, and the target is written as scheme, host and port only. The meta line
  records each principal's weight and mix.
- **One timeout for every request.** Operation references use the plan's `timeout_s` too;
  a route's `x-deadline-seconds` is not read.
