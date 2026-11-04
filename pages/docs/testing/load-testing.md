---
title: Load testing
icon: lucide/gauge
summary: Ramp a running app as several principals, find where its throughput stops rising, and compare two runs
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
principal's credential and tenant headers.

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
  [Writes and shared environments](#writes-and-shared-environments)).
- **`weight`** is the share of workers that run as a principal; **`mix`** is how often its
  workers pick each scenario.
- **Credentials come from environment variables only.** `api_key` sends a key in
  `X-API-Key` (or the header you name), `bearer` sends a pre-issued token, and
  `password_login` signs in through the `/login` and `/refresh` routes that
  `attach_authn_routes` serves. Any async callable that takes the client and returns headers
  works too, for cookies or an external IdP.
- **`tenant`** is sent as `X-Tenant-Id`. A token's tenant claim wins over it, and the app
  reads the header only with `trust_tenant_header=True`.

## Run a ramp

Point the CLI at the plan by import string:

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
  In-flight requests are capped at rate × timeout; a send past the cap counts as `transport`.

Before any load, the plan is validated and every principal signs in, at most four at a time:
forze hashes passwords with Argon2 four at a time by default, and a mass sign-in would
measure that queue. The first failed sign-in stops the run.

## Read the result

Each step's warm-up (`warm_s`) is left out of its numbers. A step row carries:

| Field | Meaning |
| --- | --- |
| `goodput_rps` | 2xx answers per second |
| `answered_rps` | Answers of any status per second |
| `ms` | `p50`, `p90`, `p95`, `p99`, `p999` and `max`, in milliseconds |
| `outcomes` | Counts of `ok`, `conflict` (409), `shed` (429), `deadline` (504), `forbidden` (401/403), `not_found`, `client_error`, `server_error`, `transport` |
| `misconfigured` | More than 5 % of calls answered 401/403 or another unexpected 4xx |
| `client` | `loop_cpu`, `process_cpu`, `loop_lag_p99_ms`, `behind_schedule`, `saturated` |
| `scenarios` | Calls, 2xx and latency per scenario |

The ramp stops at the first step where 5xx answers (504 included) and transport failures pass
`stop_error_rate` (default 5 %), or that is misconfigured: a mix that answers 403 quickly looks
like excellent throughput and measures nothing.

The summary names the **knee**: in a closed ramp, the first step whose goodput is less than
5 % above the previous step's, confirmed by the step after it; in an open ramp, the first rate
whose p99 more than doubles, or whose answers fall more than 5 % behind the offered rate.
A step whose client was saturated is never named, and the summary says so instead.

## Compare two runs

```bash
forze load compare base.jsonl head.jsonl
forze load compare base.jsonl head.jsonl --fail-on p95:15%,goodput:10%
```

Steps are matched by worker count or rate, and goodput, p50, p95 and p99 are printed side by
side. Without `--fail-on` the command only reports: load runs vary far more than
micro-benchmarks, and a default threshold would make a CI gate flaky. With it, the command
exits 1 when a named metric is worse by more than its share. Runs against a different target,
plan or client machine are compared with a warning.

## Writes and shared environments

Writes are off by default, and turning them on is not enough on its own. A run with a writing
scenario needs `allow_writes=True` **and** a target host that is a loopback address,
`localhost`, listed in the plan's `write_hosts`, or listed in the comma-separated
`FORZE_LOAD_WRITE_HOSTS` environment variable. Hosts are compared exactly, never by suffix:
`localhost.example.com` is refused. The check runs before anything is sent.

The run never cleans up what it wrote. Run write-enabled plans against a seeded, disposable
stack, never against production.

## Notes

- **One process drives the load, on one core.** A step's `client.saturated` is set when the
  event loop's thread uses 0.85 of a core, its lag p99 passes 25 ms, or more than 1 % of
  open-model sends start late. Past that point the client's queue is what the latency
  measures: read those rows as the client's limit, not the server's.
- **One session per principal.** All of a principal's workers share its sign-in, and one of
  them refreshes it, under a lock, shortly before the access token expires. forze revokes a
  whole session family when a rotated refresh token is presented again, so two workers
  refreshing one token would sign each other out.
- **The file holds numbers only:** counts, latencies and outcome classes. No bodies, headers,
  tokens or ids, and the target is written without its query string.
- **Not yet covered:** scenarios built from your OpenAPI operations, realtime subscribers, and
  soak runs that watch server gauges for drift. They are later phases of RFC 0084.
