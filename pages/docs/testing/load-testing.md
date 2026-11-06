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
  explicit `Cookie` header. `password_login`'s routes are paths on the target; a URL with a
  host is refused. Each run opens a fresh session, so a plan can be run again.
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
  type, and a body must carry the fields its schema requires at the top level. An
  operation's parameter replaces the path's of the same name and location, as OpenAPI
  says. Path values are URL-encoded. Header and cookie parameters are not checked: a
  principal's credential may be what supplies them.
- **Where the document comes from.** `openapi` defaults to `/openapi.json` on the target,
  fetched without credentials and only when the plan names an operation. For a deployment
  that serves no docs, or guards them, save the document and pass
  `openapi=Path("openapi.json")`, or the parsed document itself. When the document's first
  server is a path, as FastAPI writes a `root_path` (`/api/v1`), `base_url` must end with
  it. The meta line records the document's digest as `openapi_sha256`, so `compare` warns
  when two runs read different documents.

### Sample ids from live data

`sample("products.list", field="id", size=500)` calls the list operation once as each
principal that uses it, after sign-in and before the first step, and keeps the values of
`field` from the answer's `hits`, at most `size` of them. `size` is sent as the body's
`size`, or as the query's when the body has none, and is at most 10,000. Each call picks
one value at random. Each principal samples for itself, because an id one principal can
read may be forbidden to another.

- A sample must read a `query` operation, and an empty one stops the run before load,
  naming the principal and the operation.
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

- no input, or a JSON body whose schema takes an object and requires no field, such as a
  paginated list, sent as `{}`;
- an `id`, in the path or the query, with an optional `rev`. Ids are sampled from the `list`
  operation beside the route: `orders.list` for `orders.get`.

Every other `query` route is recorded with its reason under `plan.skipped` in the meta line,
and `forze load run` prints them: one that needs a header or a cookie, or whose body schema
is composed, cannot be resolved, or takes no empty object, among them. `command` routes are
never generated. A mix weights the group by its name, `reads` by default, and the weight is
split evenly between the generated scenarios; the rows name each by its operation key. An
operation another scenario of the plan already calls, by its name or as a `Scenario.op`
under another name, is left to that scenario, and a sweep that generates nothing stops the
run.

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
  In-flight requests are capped at the 1 − 10⁻⁶ quantile of a Poisson count with mean
  rate × timeout (9 at a mean of 1, 151 at 100), so a healthy server is never refused and a
  refusal means a real backlog. A refused send counts as a call and as `transport`.

Before any load, the plan is validated and every principal signs in, at most four at a time:
forze hashes passwords with Argon2 four at a time by default, and a mass sign-in would
measure that queue. The first failed sign-in stops the run, and the command exits 2. A ramp
that stops on its stop rule exits 0: the stop is a result, recorded in the summary. To gate
on a run, compare it with `--fail-on` (see [Compare two runs](#compare-two-runs)).

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

Steps are matched by worker count or rate, and goodput, p50, p95 and p99 are printed side by
side. Without `--fail-on` the command only reports: load runs vary far more than
micro-benchmarks, and a default threshold would make a CI gate flaky. With it, the command
exits 1 when a named metric is worse by more than its share, or has a value in the base run
and none in the head (no call answered), and 2 when a file cannot be read as a run or no step
appears in both runs: a gate that compared nothing does not pass. A step either run cut
short (`cut_short`) is printed with a warning and never gated, since its window was shorter;
a gate left with only such steps compared nothing, and exits 2 too.
When the two meta lines differ in any field but the start time (the target,
the client's cores, Python or event loop, the `forze_load` version, the profile, the
principal weights or mixes), the command compares anyway and names each field in a warning:
another host, runtime or mix is another measurement, not a regression.

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
- **Not yet covered:** realtime subscribers, and soak runs that watch server gauges for
  drift. They are later phases of RFC 0084.
