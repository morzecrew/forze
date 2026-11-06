# Load-testing a running app

`forze_load` drives your running app over HTTP as the principals that really use it, step by
step, and writes one JSON line per step: goodput, latency percentiles, outcome classes, and
whether the client itself was the bottleneck. What a generic tool cannot do: name a route by
its operation key, check that key against the route's kind, and sample ids from the app's own
list routes. Install `forze[load,cli]`; the `cli` extra adds the `forze load` command. The
[load testing docs](https://morzecrew.github.io/forze/latest/testing/load-testing/) hold the
full surface; this file is the working path.

## Write a plan

A plan module names the target, the principals, and what each of them sends. Call operations
by key, the route's `operationId`, instead of typing paths:

```python
from forze_load import LoadPlan, Principal, Scenario, api_key, password_login, sample

plan = LoadPlan(
    base_url="http://localhost:8000",
    principals=[
        Principal(name="viewer", weight=4, mix={"products.get": 3, "list": 1, "reads": 1},
                  auth=api_key("VIEWER_API_KEY")),
        Principal(name="buyer", weight=1, mix={"list": 3, "create": 1},
                  auth=password_login("buyer@example.com", password_env="BUYER_PASSWORD"),
                  tenant="6ba7b810-9dad-11d1-80b4-00c04fd430c8"),
    ],
    scenarios=[
        Scenario.op("products.get", path={"id": sample("products.list")}),
        Scenario.op("products.list", name="list", body={"size": 20}),
        Scenario.op("products.create", name="create", writes=True,
                    body={"name": "load", "price": 1}),
        Scenario.reads("orders."),
    ],
    allow_writes=True,
)
```

- **Principals.** `weight` is a principal's share of the workers; `mix` is how often its
  workers pick each scenario, by scenario name. Credentials come from environment variables
  only: `api_key` (`X-API-Key` by default), `bearer`, and `password_login`, which signs in
  through the `/login` and `/refresh` routes of `attach_authn_routes`. Pass `login_path=` and
  `refresh_path=` when those routes sit under a prefix. `tenant` is sent as `X-Tenant-Id`,
  which the app reads only with `trust_tenant_header=True`.
- **The kind is checked.** Every generated route carries `x-forze-kind`, `query` or
  `command`. `Scenario.op` declares `writes=False` unless you pass `writes=True`. The run
  stops before signing anyone in when an operation is missing from the document or its kind
  disagrees with `writes`. A hand-written route has no kind, so it counts as `command`: call
  a hand-written read with a plain `Scenario(name, send, writes=False)` instead.
- **Samples.** `sample("products.list", field="id", size=500)` calls the list operation once
  per principal, after sign-in, and keeps `field` from the answer's `hits`. `size` is at most
  10,000. Each principal samples for itself, since another may not see the same ids. An empty
  sample stops the run before load. An id that later answers 404 leaves the sample.
- **The read sweep.** `Scenario.reads("orders.")` makes one scenario per `query` operation
  under the prefix that it can fill without guessing: one with no input, or a JSON body that
  accepts `{}`, or an `id` sampled from the sibling `list` operation. The mix weights the
  group by its name, `reads` by default. Every route it skips is listed with its reason under
  `plan.skipped`, and `forze load run` prints them.
- **The document.** When the plan names an operation, the run fetches `/openapi.json` from
  the target, without credentials. For a deployment that serves no docs, pass
  `openapi=Path("openapi.json")`. When FastAPI has a `root_path` (`/api/v1`), `base_url` must
  end with it. An operation whose OpenAPI entry names a server of its own, neither the
  document's nor `base_url`, is refused when a scenario names it, and a sweep skips it.

## Keep writes off shared stacks

A scenario declared `writes=True` is refused unless the plan sets `allow_writes=True`, and
even then the target host must be a loopback address, `localhost`, in the plan's
`write_hosts`, or in the comma-separated `FORZE_LOAD_WRITE_HOSTS` variable. Hosts are compared
exactly, never by suffix. The run never deletes what it wrote: point a write plan at a seeded,
disposable stack.

## Run a ramp and read the knee

```bash
forze load run myapp.load:plan --steps 1,2,4,8,16,32 --step-s 30 --warm-s 5 --out ramp.jsonl
```

The argument can also name a function that takes no arguments and returns the plan. From
Python, `run` returns the same rows. To serve the app in the same process, pass it as an
`httpx` transport. `ASGITransport` sends no lifespan events, so open the lifespan yourself:

```python
import httpx

from forze_load import ClosedRamp, LoadPlan, run


async def ramp_in_process(app, plan: LoadPlan):
    async with app.router.lifespan_context(app):
        return await run(
            plan,
            ClosedRamp(steps=[2, 4], step_s=0.6, warm_s=0.1),
            out="ramp.jsonl",
            transport=httpx.ASGITransport(app=app),
        )
```

In process, the app runs on the client's event loop, so the loop's CPU is reported but not
judged (`loop_cpu_judged: false`). Use it to check that a plan resolves, not to measure.

Reading the rows:

- **The knee** is the summary's answer: the first step whose goodput rose less than 5 % over
  the step before, confirmed by the step after it. The summary's `note` says why no knee was
  named.
- **`client.saturated`** marks a step where the load client ran out of room: its event loop
  used 0.85 of a core, its lag p99 passed 25 ms, or open-model sends fell behind. Such a step
  is the client's limit, not the server's, and is never named the knee.
- **`misconfigured`** means more than 5 % of calls answered 401, 403 or another unexpected
  4xx. The ramp stops there: fix the credentials or the mix.
- **The stop rule** ends the ramp at the first step where 5xx answers and transport failures
  pass `stop_error_rate` (5 %). That run still exits 0: the stop is a result.
- **A closed ramp finds capacity.** Its rows say `"latency": "closed"`: a stall is counted
  once per worker. For a latency objective, run an `OpenRamp(rates=[...])`.

## Soak for drift

A soak holds one load level, writes a row per window, and flags series that drift upward:

```python
import asyncio

from forze_load import Soak, run


def soak(plan):
    return asyncio.run(run(plan, Soak(duration_s=4 * 3600, workers=32), out="soak.jsonl"))
```

The CLI form is `forze load run myapp.load:plan --steps 32 --soak-s 14400 --window-s 60`.
The soak scrapes `LoadPlan.metrics`, `/metrics` by default, without a credential. It keeps
the `forze_*` series and the process gauges, so the app must serve that route and call the
`instrument_*` functions for what you want watched (see [logging and metrics](logging-metrics.md)).
A route it cannot read before load stops the run. Set `metrics=None` to soak on client
numbers only.

- Drift is fitted only from scrapes of one process, named by the `service_instance_id` that
  `bootstrap_telemetry` sets. A pre-fork server without one scrape for every worker comes out
  `interleaved`, and its series are not fitted.
- Drift flags never fail the run: it exits 0, and reading the summary's `drift` is yours.

## Time realtime delivery

A `Realtime` profile holds SSE or WebSocket streams open, sends writes at a set rate, and
times each write's frame at every stream it should reach. It then drops every stream at once
and measures the resume. It runs from Python only:

```python
import asyncio

from forze_load import Frame, Realtime, run


def shipped(frame: Frame):
    return frame.data.get("order_id") if frame.event == "order.shipped" else None


def realtime_run(plan):
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
    return asyncio.run(run(plan, realtime, out="realtime.jsonl"))
```

- **Matching is yours.** `key_of` names the key a write's event carries, `match` reads it
  back from a frame, and `audience` names the principals the event reaches (`None` means
  every stream).
- **Each stream sends its own `device_id`**, which keys its stored cursor. The SSE route
  reads it itself. A WebSocket route takes the key from its resolver:
  `build_ws_connection_resolver` fills it from `device_id`, and a hand-written resolver must
  too, or a principal's streams share one cursor.
- **The storm.** `storm=True` (the default) drops every stream after the steady phase and
  reopens them, each resuming from the last durable id it saw. For a live-only SSE route,
  which ignores `Last-Event-ID`, set `resume=False`.
- **Reading `delivery.share`.** A share below 1 is a server finding when `client.saturated`
  is false. A slow reader fills its queue on the server, which drops what does not fit. With
  `instrument_realtime_hub`, the app counts each drop as `forze.realtime.hub.dropped`. The run
  scrapes nothing, so read that from your own metrics.

## Gate on a comparison

```bash
forze load compare base.jsonl head.jsonl --fail-on p95:15%,goodput:10%
```

`--fail-on` takes `goodput`, `p50`, `p95` and `p99`. Without it, `compare` only reports. The
exit codes:

- **0**: `run` finished, a stop included; `compare` crossed no threshold.
- **1**: `compare` found a metric worse than its threshold.
- **2**: no verdict. The plan, an operation, the OpenAPI document, a soak's metrics route, a
  credential, a sign-in or a sample failed; a scenario raised; a run file cannot be read; the
  `--fail-on` spec is invalid; `--fail-on` matched no whole step in both runs, as with two
  realtime runs; or the `load` extra is missing.

When the two runs' meta lines differ, as in their `openapi_sha256`, host, cores or mixes,
`compare` warns and still compares.

## Anti-patterns

- **Allowlisting a production host for a write plan.** `FORZE_LOAD_WRITE_HOSTS` and `write_hosts` exist for disposable stacks; the run never cleans up what it created.
- **A realtime run against a shared stack.** The kit never acks, and every first open replays up to 1,000 mailbox entries, so `forze.realtime.mailbox.replayed` and `replay_size` rise while the run adds nothing to `acked`. Give it a stack of its own.
- **Reading a saturated client as a slow server.** Rows with `client.saturated` measure the load client's own queue. Read only the steps before it as the server's numbers.
- **Gating across OpenAPI digests.** Two runs that read different documents may have resolved the plan to different routes or bodies: `compare` warns on `openapi_sha256` and still gates. Check what changed before you read a failure as a regression.
- **Hand-typed paths.** A renamed route turns into a run of 404s; `Scenario.op` fails before load instead.
- **One admin principal for everything.** It measures the admin's authorization path and data, not your users'. Give each role its own principal and mix.
- **Following a pagination `next` link.** `Call.request` refuses a URL with a scheme or a host before sending, so the credential never leaves the target.
- **Reading closed-ramp latency as an SLO.** Use an `OpenRamp` for latency.
- **Soaking a pre-fork server on a plain `/metrics` route.** Its series come out `interleaved` and are never fitted; serve one scrape for every worker.

## Reference

- [Load testing](https://morzecrew.github.io/forze/latest/testing/load-testing/)
- [Metrics catalog](https://morzecrew.github.io/forze/latest/reference/metrics/)
- [Generated routes and operation keys](fastapi-generated-routes.md)
- [Identity at the boundary](fastapi-identity.md)
- [Logging and metrics](logging-metrics.md)
- [Realtime transports](realtime-transports.md)
