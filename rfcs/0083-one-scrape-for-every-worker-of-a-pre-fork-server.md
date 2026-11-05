# RFC 0083 — One scrape for every worker of a pre-fork server

- **Status:** 🚧 In progress — **P1 shipped 2026-11-05** (#846): the snapshot writer, the
  union route, the age gauge and `worker_metrics_reader()`. §10 Q1 to Q7 were settled by the
  maintainer on 2026-11-05, each the lean's way (rows 4 to 10), and the rows execution proposed
  in [`EXECUTION-LOG.md`](EXECUTION-LOG.md) Unit 19 were accepted the same day (rows 19 to 22).
  **P2 (the collector recipe) remains, and P3 (a Redis/Valkey store) is demand-gated.**
- **Scope:** A supported way for one Prometheus scrape to cover every worker of a pre-fork
  server (`uvicorn --workers N`, gunicorn's uvicorn worker) without an OTLP collector. The
  recommendation:
  - each worker writes a snapshot of its own metrics, labelled with its `service.instance.id`,
    into a directory every worker of the group can read, on an interval;
  - `attach_metrics_route` gains an opt-in mode that answers from that directory: the union of
    every fresh snapshot, plus a `forze_metrics_snapshot_age_seconds` gauge per worker;
  - a short collector recipe (OTLP in, Prometheus out) for deployments that span hosts.

  It does **not** sum, average or otherwise aggregate across workers: each series stays per
  instance, and aggregation is the query's job. It does not change the route's single-process
  behaviour, `bootstrap_telemetry`, or the OTLP push path. It does not add a dependency or an
  extra: the route already needs `opentelemetry-exporter-prometheus`. A Redis or Valkey store
  is designed but demand-gated (P3), because it needs a new core port. Fan-out at scrape time
  is rejected (§5.8 A2). There is no contract change and no core change in P1 or P2.
- **Related:**
  - [`src/forze_fastapi/routes/health.py`](../src/forze_fastapi/routes/health.py):
    `attach_metrics_route` (176-245), its one-process caveat (190-195), the render gate
    (211-243), the content type (31), and `_prometheus_renderer` (272-298);
  - [`src/forze/base/telemetry/bootstrap.py`](../src/forze/base/telemetry/bootstrap.py):
    `bootstrap_telemetry` (90), the per-process `service.instance.id` (121-125, 242),
    `extra_metric_readers` (138-139) and the meter provider's readers (338-339);
  - [`src/forze/base/telemetry/handle.py`](../src/forze/base/telemetry/handle.py): the
    handle's `resource` (69);
  - [`src/forze/application/execution/background/periodic.py`](../src/forze/application/execution/background/periodic.py):
    `periodic_lifecycle_step` (133-157), the per-worker writer's seam;
  - [`src/forze/application/execution/lifecycle/plans.py`](../src/forze/application/execution/lifecycle/plans.py):
    `LifecyclePlan.with_steps` (93);
  - [`src/forze_fastapi/lifespan.py`](../src/forze_fastapi/lifespan.py): `runtime_lifespan`
    (183), where every worker builds its own runtime;
  - [`src/forze/application/execution/observability.py`](../src/forze/application/execution/observability.py):
    `instrument_operations` (79), `instrument_resilience` with the bulkhead gauges (127,
    200-240), `instrument_tenant_pools` (247), `instrument_crypto` (322);
  - [`src/forze/application/contracts/fleet/specs.py`](../src/forze/application/contracts/fleet/specs.py)
    (`report_max_bytes`, 29) and
    [`src/forze_redis/adapters/fleet.py`](../src/forze_redis/adapters/fleet.py) (server
    clock, 1-6): prior art for the Redis store;
  - [`pyproject.toml`](../pyproject.toml): the exporter as a dev dependency only (80-81),
    the `observability` extra (184), the integration-isolation contract (1039-1072), and the
    `prometheus_client` guarded-import exemption (1199-1202);
  - [`pages/docs/running-in-prod/grafana-stack.md`](../pages/docs/running-in-prod/grafana-stack.md):
    metric names (143-171), label discipline (174-198), counter resets (242-253), and "Pull
    metrics, and why not" (255-276);
  - [`pages/docs/reference/metrics.md`](../pages/docs/reference/metrics.md) (conventions,
    17-26) and its parity test
    [`tests/unit/test_metric_catalog_parity.py`](../tests/unit/test_metric_catalog_parity.py);
  - [`tests/unit/test_forze_fastapi/test_serve_workers.py`](../tests/unit/test_forze_fastapi/test_serve_workers.py):
    the `uvicorn --workers 2` subprocess precedent.
- **Discussion:** [issue #838](https://github.com/morzecrew/forze/issues/838).

---

## 1. Summary

Each worker of a pre-fork group renders its own Prometheus exposition every `interval` and
writes it to a shared directory, atomically, as one file per process. Every series in the file
carries `service_instance_id`, the worker's `service.instance.id`. The metrics route, in its new
snapshot mode, reads every snapshot younger than `ttl`, merges them family by family, and adds
one `forze_metrics_snapshot_age_seconds{service_instance_id=…}` sample per worker. Whichever
worker accepts the scrape gives the same answer. A worker that dies stops writing, and its
series drop out once its snapshot is older than `ttl`. A worker that restarts comes back as a new
instance, so a counter is never summed across processes and `rate()` sees no phantom reset.

The route without the new argument stays exactly as it is. A deployment across hosts gets a
documented collector recipe instead, and a Redis store waits for demand.

## 2. Motivation

`attach_metrics_route` serves one process, and its docstring says so (`health.py:190-195`).
Under a pre-fork server, the worker that accepted the connection answers the scrape. Successive
scrapes then sample different processes: cumulative counters interleave, `rate()` sees resets
that never happened, and gauges flap between workers. The documented answer is OTLP push
(`grafana-stack.md:255-276`), which means running and operating a collector. For a small,
self-hosted deployment, that collector is often the only reason to add a service.

The consumer apps match that profile (read-only survey, 2026-11-05):

| App | Workers | Metrics today |
|---|---|---|
| erp-backend | `uvicorn.run(..., workers=settings.api.workers)` (`app/src/api/cli.py:13-18`). Compose defaults to `API__WORKERS: 2` (`compose/compose.yml:185`). | None. Valkey is in the stack. |
| bi-backend | The same entrypoint. Compose defaults to 2 (`compose/compose.yml:79`). | None. Its product API already owns `/metrics/*` (`app/src/api/v1/routers/serving.py:22`), so the route would need another path. |
| eis-backend | The same entrypoint. Compose defaults to 1 (`compose/compose.yml:172`). | **The pull route, no collector.** `bootstrap_telemetry(exporter="none", extra_metric_readers=[PrometheusMetricReader()])` (`app/src/wire/telemetry.py:15-25`), commented "No collector needed" (8-10). The data service's own `start_http_server` loses its port to the second worker on one host (28-38). |
| hh-backend-v2 | One process per container. It scales by replicas. | OTLP push to its own collector. It does not need this. |

No app runs a Prometheus server yet, and none has hit the bug. The demand is latent but direct.
eis-backend picked the pull route because it needs no collector. It breaks the moment
`API__WORKERS` goes above 1. erp-backend and bi-backend already run two workers, so copying the
eis wiring would give them the bug on day one. A load test that wants one sample of every
worker's pool, bulkhead and realtime gauges hits the same wall.

## 3. Current state

Verified against the tree at `3535960d9` and the locked dependencies (exporter 0.66b1,
`prometheus_client` 0.26.0, uvicorn 0.51.0).

**The route.** `attach_metrics_route(router, reader, *, path="/metrics")` checks that `reader`
is a `PrometheusMetricReader`, takes the registry back from the reader (`health.py:298`), and
renders it with `prometheus_client.generate_latest` in a thread. Text format 0.0.4 is the only
content type (31). Renders are serialized through a shared future, because the exporter's
collector drains an unsynchronized deque, and two collections in flight steal each other's data
(218-229). Rendering is collection: every scrape runs every observable callback in the process
(232-235).

**forze does not use `prometheus_client`'s metric objects.** Instruments are OpenTelemetry,
and the `instrument_*` helpers register observable instruments on the global meter: pools
(`observability.py:247`), keyrings (322), bulkheads (200-240), L1 caches, realtime gateway and
backplane, leadership and jobs. `prometheus_client` is only the renderer, which ships inside
`opentelemetry-exporter-prometheus`. That package is a dev dependency (`pyproject.toml:80-81`)
and a guarded import (1199-1202). It is not part of the `observability` extra (184).

**`prometheus_client`'s multiprocess mode does not apply.** That mode swaps the value class of
`prometheus_client`'s own metric objects for per-pid mmap files
(`prometheus_client/values.py:157-163`). The OTel exporter registers a custom collector and never
touches that class, so `PROMETHEUS_MULTIPROC_DIR` has no effect on any forze metric. The mode
also **sums** counters and histograms across pids (`prometheus_client/multiprocess.py:117-150`),
which is the opposite of the issue's per-instance constraint. It reaches the shape, not the
mechanism.

**Instance identity exists and is not on the series yet.** `bootstrap_telemetry` mints a UUID
per process as `service.instance.id` (`bootstrap.py:242`). The exporter puts resource
attributes only on `target_info` by default. Since 0.66b0 it takes
`resource_attribute_filter`, which copies the selected resource attributes onto every series.
0.65b0 does not have it, and the dev floor is `>=0.60b0`. Measured on 0.66b1, with the filter
selecting `service.instance.id`:

```text
forze_operations_total{forze_outcome="success",otel_scope_name="forze",…,service_instance_id="w1"} 3.0
forze_operation_duration_milliseconds_bucket{le="25.0",…,service_instance_id="w1"} 1.0
```

**Temporality is cumulative by construction.** `PrometheusMetricReader` asks for cumulative
temporality on every instrument kind (exporter `__init__.py:265-272`). The OTLP
`OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE` setting applies to the OTLP reader only, so
a delta preference never reaches the pull path.

**Surprising facts the design inherits:**

- The pull path's names differ from the Alloy path's. The pull path renders
  `forze_operations_total` and `forze_operation_duration_milliseconds_*`. The shipped Alloy
  config turns suffixes off (`grafana-stack.md:146-161`), and the shipped dashboards and alert
  rules assume those names and group `by (job)`. They do not apply to the pull path as they
  stand, today or after this RFC.
- The exporter does not render exponential histograms. Any data type other than sum, gauge and
  explicit histogram logs "Unsupported metric data" and is dropped (exporter
  `__init__.py:421`). `bootstrap_telemetry(exponential_histograms=True)` with the pull route
  therefore loses its duration histograms today.
- Text format 0.0.4 carries no exemplars.
- `PrometheusMetricReader()` without `registry=` renders the global `REGISTRY`, which
  `prometheus_client` populates with `process_*`, `python_gc_*` and `python_info` for the
  answering process. eis-backend's reader does exactly this. Those series carry no instance
  label, so they would collide in any union.

**The worker model offers no sibling channel.** Uvicorn starts workers with the `spawn` context
(`uvicorn/_subprocess.py:18`). They share the listening sockets the supervisor bound. The only
pipe runs from the supervisor to each worker, for health checks (`supervisors/multiprocess.py:
26-36`). The supervisor kills and replaces a hung worker (214-222), and `restart_all` starts the
new worker before it stops the old one (173-183), so two generations overlap for a moment.
Gunicorn forks from its arbiter. It has no worker-to-worker channel either, recycles workers
under `--max-requests`, and with `--preload` imports the app before the fork. All workers of
one group share a host, a filesystem, a pid namespace and a clock.

**A per-worker background seam exists.** `runtime_lifespan(build=...)` builds one runtime per
worker (`lifespan.py:205-208`), so each worker runs its own lifecycle startup.
`periodic_lifecycle_step` runs a tick on an interval, survives a failing tick, and stops between
ticks on shutdown (`periodic.py:1-11`, 133-157). `forze_fastapi/realtime/lifecycle.py:561`
already builds a step with it. The fleet lifecycle awaits its first beat at startup, so a refused
report fails the boot once (`forze_kits/integrations/fleet/lifecycle.py:133`).

**A Redis-backed registry exists, for small payloads.** `FleetPort` (`contracts/fleet/ports.py:
28`) has a Redis adapter that keeps expiry on the server's `TIME` (`forze_redis/adapters/fleet.py:
1-6`). Its reports are capped at 1 KiB by default (`specs.py:29`). `forze_fastapi` cannot
import `forze_redis` (`pyproject.toml:1039-1072`), so any Redis store needs a core port.

**The collector recipe exists, and it is the whole stack.** `grafana-stack.md` ships Alloy,
Prometheus by remote-write, Loki, Tempo and Grafana. No minimal "OTLP in, Prometheus out" recipe
exists.

## 4. Goals / Non-goals

**Goals**

- One scrape of any worker returns every live worker's series, each labelled by instance.
- Staleness is bounded by `ttl` and visible per worker as `forze_metrics_snapshot_age_seconds`.
- A dead worker drops out by itself. A restarted worker is a new series, never a reset.
- Single-process apps and OTLP apps see no change.
- P1 fits one PR, with an end-to-end test under a real `uvicorn --workers 3`.

**Non-goals**

- Aggregating across workers. A summed counter hides a restart, and the issue wants the query
  side to decide.
- A union across hosts or containers. That is Prometheus service discovery's job, or the
  collector's (P2). Redis is P3, demand-gated.
- Matching the Alloy path's names or the shipped dashboards. That is a separate naming decision,
  and it would break today's pull users.
- Process-level collectors (`process_*`, `python_gc_*`) in the union. §8 says why.

## 5. Design

### 5.1 The three shapes, and where each one stands

| | Shared snapshots (directory) | Shared snapshots (Redis/Valkey) | Fan-out at scrape time | Collector recipe |
|---|---|---|---|---|
| Reach | One host: exactly the pre-fork group | Any host that reaches the store | One host | Any host |
| New moving parts | A directory (tmpfs) | A store, a core port, an adapter | A listener per worker, plus a registry | One container |
| Freshness | Up to `interval` old, shown | Up to `interval` old, shown | Live | Collector's export interval |
| Scrape cost | Read and merge files, about 2 ms for 4 workers | N `GET`s, about 1 MB each | N HTTP calls, as slow as the slowest worker | None in the app |
| Clock | One kernel clock | Needs the server clock | None | Collector's |
| forze code | About 300 lines in `forze_fastapi` | A port in core, an adapter in `forze_redis`, a mock | A second server per worker | Docs and one config asset |

Recommendation (row 4): directory snapshots in P1, the collector recipe in P2, Redis in P3 on
demand. Fan-out is rejected (§5.8 A2).

### 5.2 W1: the per-worker snapshot writer

The writer is a lifecycle step in the runtime's plan, so every worker runs its own:

```python
from datetime import timedelta
from pathlib import Path

from forze.application.execution import ExecutionRuntime
from forze.base.telemetry import bootstrap_telemetry
from forze_fastapi import runtime_lifespan
from forze_fastapi.routes import MetricsSnapshots, attach_metrics_route, worker_metrics_reader

reader = worker_metrics_reader()  # row 8
snapshots = MetricsSnapshots(directory=Path("/run/forze-metrics"))  # row 7 defaults


def build_runtime() -> ExecutionRuntime:  # runs in each worker, after any fork
    bootstrap_telemetry(service_name="erp-api", exporter="none", extra_metric_readers=[reader])
    lifecycle = plan.with_steps(snapshots.writer_step(reader)).freeze()
    return ExecutionRuntime(deps=deps, lifecycle=lifecycle)


lifespan = runtime_lifespan(build=build_runtime)
attach_metrics_route(router, reader, snapshots=snapshots)  # row 10
```

`worker_metrics_reader()` returns
`PrometheusMetricReader(registry=CollectorRegistry(), resource_attribute_filter=<service.instance.id only>)`.
The dedicated registry keeps the process collectors out. The filter puts the instance label on
every series. On an exporter older than 0.66b0, the helper raises `configuration` and names the
floor.

`bootstrap_telemetry` runs inside the per-worker build, never at import. It mints the
`service.instance.id` (`bootstrap.py:242`), and gunicorn `--preload` runs import-time code once
in the arbiter, before the fork: every worker would inherit one id, and §5.3 step 3 would
withhold them all. The reader and the `MetricsSnapshots` value carry no identity, so they can be
built at import and handed to the route before any worker exists. The docs (§7) state this rule.

**Startup** (one bounded step, then the loop):

1. Prepare the directory. Create it with mode `0700` if it is missing. Then open it once with
   `O_DIRECTORY | O_NOFOLLOW` and check the open descriptor (`fstat`), not the path: a real
   directory, owned by the effective uid, and not group- or world-writable. Anything else
   raises `configuration` (`metrics_snapshot_directory_unsafe`). Every later operation (each
   tick, the sweep, the final snapshot, and the route, which reaches the descriptor through the
   shared `MetricsSnapshots` value) goes through that descriptor (`dir_fd=`, `os.listdir(fd)`)
   and never resolves the path again. A user who can write to a parent directory therefore
   cannot swap the directory after the check.
2. Render once and check the result. Every sample line must carry exactly one
   `service_instance_id` label, with one value across the whole render. A family's first line
   is not enough: the exporter lets a data point's own attributes override the copied resource
   label (exporter `__init__.py:455-464`), so a later sample can name another instance. The
   check is a regex per line, measured at about 7 ms per MB, off the loop. A render that fails
   the check raises `configuration` (`metrics_snapshot_unlabelled`). The remedy names
   `worker_metrics_reader()`. This is what refuses eis-backend's global-registry reader,
   instead of letting it serve colliding series. A render with no family yet passes. There is
   nothing to collide, and nothing to name the snapshot's `instance` by, so no file is written.
3. Write the first snapshot if the render named an instance, then start
   `periodic_lifecycle_step` with the same tick. Until a tick's render carries the label, the
   worker writes nothing and is not served: it has no series to serve. The instance a worker
   first names is its instance for good; a later render naming another fails the label check.

**Each tick** renders off the loop, checks, and writes atomically: it creates `.tmp-<pid>-<rand>`
with `O_CREAT | O_EXCL | O_NOFOLLOW` and mode `0600` relative to the directory descriptor, then
calls `os.replace` onto `<pid>-<token>.prom` with that descriptor as both `dir_fd`s. The token is
a random hex string minted once per writer. The file is never visible half-written. A family
that fails the label check on a later tick is left out of that snapshot and logged once per name.
It is not a boot failure, because new families can appear at any time. A failed render or write
is logged and the loop goes on: the previous snapshot ages, and drops out at `ttl`.

**The snapshot format** is the exporter's text output with one header line:

```text
# forze-metrics-snapshot v1 written_at=1762336812.418 mono=4087760.231 pid=4182 instance=6f1c…e2
# HELP forze_operations_total
# TYPE forze_operations_total counter
forze_operations_total{…,service_instance_id="6f1c…e2"} 3.0
…
```

`instance` is the value the check read off the series, escaped as a Prometheus label value. The
writer learns it from what it is about to serve. It needs no access to the meter provider, and it
works under an SDK the app installed itself.

**Freshness.** `written_at` is `time.time()` at the write, and `mono` is
`time.clock_gettime(time.CLOCK_MONOTONIC)`. The monotonic clock is one per host and keeps
counting through wall-clock steps: measured on Linux, a child process's reading always falls
between its parent's readings before and after it. A snapshot's monotonic age is the reader's
`CLOCK_MONOTONIC` minus `mono`; its wall-clock age is `time.time()` minus `written_at`. It is
**fresh** while both ages lie between 0 and `ttl`. The monotonic age bounds a dead worker's
lifetime in real time, whatever the wall clock does. The wall-clock age catches a file left from
before a reboot, where the monotonic clock started again. A wall-clock step therefore withholds
snapshots until each worker writes again, at most one `interval`, and never extends one.

**Sweeping.** Each tick also deletes snapshots that are not fresh by their header (a malformed
header counts as not fresh) and whose pid is gone (`os.kill(pid, 0)` raises
`ProcessLookupError`). A leftover temp file is deleted once its pid is gone, since nothing else
can finish it. A live worker that stalled is never swept; it is only not served. All workers
share one pid namespace (§3).

**Shutdown** (row 6) writes one final snapshot within a bounded time and leaves it to age out.
The writer's ticks and its final render go through the route's render gate (§3), which moves
from the route to the reader: one shared future per reader. The snapshot route renders nothing
and takes no gate (row 5). An app can still attach a plain route to the same reader, and two
collections in flight on one reader steal each other's data, so the writer and every plain
route on that reader wait on one future. A tick cancelled at shutdown leaves its render thread
running, so the final render waits on that future first. If waiting would pass the shutdown
budget, the final render is skipped and the last tick's snapshot stands: a render never runs
beside an unfinished one.

### 5.3 W2: the union route

`attach_metrics_route(router, reader, *, path="/metrics", snapshots=None)`. With
`snapshots=None` the route is the current code path, unchanged (row 3). With `snapshots`, the
handler renders nothing (row 5). It runs this off the loop:

1. List the directory through its descriptor (§5.2 step 1). Keep regular files named
   `<pid>-<hex>.prom`, opened with `O_NOFOLLOW` relative to it, owned by the effective uid, and
   no larger than `max_snapshot_bytes` (16 MiB by default), each checked with `fstat` on the
   open file. Skip anything else, and log it with a rate limit.
2. Parse each header. Skip a file with a malformed header, or one that is not fresh (§5.2).
3. If two fresh files carry the same `instance`, serve **neither**, and log an error naming both
   pids. Two processes sharing one id interleave exactly as the bug this RFC fixes. The usual
   cause is gunicorn `--preload` with `bootstrap_telemetry` at import time.
4. Merge by family. Split each body where a line starts with `# HELP` and a space. Keep the
   first `HELP`/`TYPE` pair per family name, and append every snapshot's sample lines under it. A
   later snapshot whose `TYPE` disagrees for the same name loses that family, with a warning.
   That can happen while a rolling restart runs two code versions. Label values cannot contain
   a raw newline in this format, so a line-start split cannot cut a sample.
5. Append the age family (§5.4). The route owns that name: a snapshot family called
   `forze_metrics_snapshot_age_seconds` is left out of the merge, with a warning, so the
   response never carries a second `HELP`/`TYPE` pair for it.
6. With no snapshot left to serve after steps 2 and 3, answer `503`, so Prometheus records
   `up == 0`. That includes every fresh file withheld as a duplicate, which is what gunicorn
   `--preload` produces. Otherwise answer `200` with the 0.0.4 content type.

The merge is string work, not a parse. Measured on 0.66b1 with 200 operation label sets per
worker: one snapshot is 0.81 to 1.08 MB, merging four takes 2.3 ms, and `prometheus_client`'s
parser takes 91 to 132 ms **per snapshot**. A parse-and-re-render union would pay about half a
second of GIL per scrape at four workers.

### 5.4 W3: the age gauge

```text
# HELP forze_metrics_snapshot_age_seconds Seconds since this worker wrote the snapshot served.
# TYPE forze_metrics_snapshot_age_seconds gauge
forze_metrics_snapshot_age_seconds{service_instance_id="6f1c…e2"} 4.2
```

The route renders the family itself, from the snapshot's monotonic age (§5.2), which a
wall-clock step cannot move. Every worker of a group reads the same kernel clock, so there is no
skew between writer and reader. The name is the one the exporter would derive from
`forze.metrics.snapshot.age` with unit `s`. That OTel name is declared as a `_GAUGE` constant, so
the catalog parity test requires its row in `metrics.md`. The gauge describes a worker's
snapshot, not each series in it. `count(forze_metrics_snapshot_age_seconds)` is the number of
workers served, which is the alert for a worker that stopped writing.

### 5.5 Correctness

| Concern | How the design handles it |
|---|---|
| Counter resets | A restart is a new `service_instance_id` and a new series. Nothing is summed, so `rate()` sees no phantom reset. A counter's increments since its last successful snapshot are lost on a crash. That is one `interval` worth while ticks succeed, the same order as a scrape interval today. After failed ticks it is everything since the last successful write, and a worker whose last write is older than `ttl` is not served at all (§5.6). A graceful exit writes a final snapshot (row 6). |
| Per-instance labels | `service_instance_id` on every series, checked at write (§5.2). Prometheus's own `instance` stays the scrape target, identical for all workers. Row 9 decides the name. |
| Temporality | Cumulative only. The reader forces it (§3), so a delta preference on the OTLP side cannot leak in. |
| Histograms | Explicit-bucket histograms carry over per instance. Families are concatenated, never bucket-summed, so two code versions with different ladders stay valid. Exponential histograms are dropped by the exporter today (§3); the union inherits that. |
| Exemplars | None. Format 0.0.4 carries none, as today. |
| Gauge staleness | A gauge is at most `interval` plus one render old while its worker is live, and the age gauge says how old. After `ttl` the worker's series are not served, and Prometheus marks them stale. |
| Worker death and restart | No final snapshot after a crash. The last one is served until `ttl`, then dropped, then swept once its pid is gone. Uvicorn's replacement worker, and the overlap during `restart_all`, appear as distinct instances. |
| Shared instance id | Neither process is served (§5.3 step 3). |
| Clock skew | None within one host. Ages run on the host's monotonic clock. A wall-clock step withholds snapshots for at most one `interval` and never extends a dead worker's. A cross-host store would need the server's clock, as the fleet adapter does. |
| Snapshot size | 0.81 to 1.08 MB per worker per 200 operation label sets, and linear in label sets. Capped at `max_snapshot_bytes`. On tmpfs a write goes to memory. |
| Scrape cost | File reads plus a 2 ms merge for four workers. No observable callback runs on a scrape. The response is N snapshots: about 4 MB at four workers, uncompressed. |
| Writer cost | One render per `interval` per worker, measured at 24 to 36 ms of mostly-GIL time for 200 label sets. At the leaned 10 s interval, that is today's cost of one scrape every 10 s. |
| Route security | Unchanged class of exposure: the route serves metric names and labels, and stays internal (eis-backend's gateway already answers `/metrics` with 404). It now shows every worker, not one. |
| Store security | `0700` directory owned by the process uid, checked once and then held open, so its path is never resolved again. `0600` files, served only when owned by the process uid. No symlinks followed, regular files only, a size cap. A writer to the directory can inject series, but it already runs as the app's uid. |
| Tenant and cardinality | No tenant label anywhere, by doctrine (`metrics.md:24`). Series count is N times one worker's, as under OTLP push. Every restart adds a new set until the old one leaves the TSDB head. |

### 5.6 Failure semantics

- An unsafe directory, a first render whose samples lack the label or name more than one
  instance, or an exporter older than 0.66b0 in `worker_metrics_reader()` raises
  `configuration` at boot. A first render with no family is the one exception: it passes and
  writes no file (§5.2 step 2).
- A failed tick (render error, `ENOSPC`, permission lost) is logged, and the worker keeps
  serving traffic. Its snapshot ages and drops out at `ttl`. `count()` on the age gauge shows
  the gap.
- A malformed, oversized, symlinked, foreign-owned or misnamed file is skipped and logged with
  a rate limit. It is never served.
- A duplicate instance id is never served.
- No snapshot left to serve, because none is fresh or every fresh one is a duplicate, answers
  `503`. Any other partial state answers `200` with what is served.

### 5.7 What stays unchanged

- `attach_metrics_route(router, reader)` and `attach_metrics_route(router, reader, path=...)`:
  same code path, same names, same content type, and the same render gate, now held per reader
  rather than per route (§5.2). With one route per reader, as today, nothing changes. The
  existing tests in `tests/unit/test_forze_fastapi/test_probe_routes.py` pass unmodified.
- `bootstrap_telemetry`: no new parameter, no new default.
- OTLP push: untouched, and still the documented answer across hosts.
- Dependencies: no new runtime dependency, no new extra. The snapshot mode needs exporter
  0.66b0 or newer, checked at wiring, only when the app opts in.

### 5.8 Alternatives considered

- **A1, a Redis or Valkey store.** It gives a union across containers and hosts, and Valkey
  already runs in erp-backend and eis-backend. It costs a new core port, because
  `forze_fastapi` cannot import `forze_redis` (`pyproject.toml:1039-1072`), plus an adapter, a
  mock and a conformance battery. It moves about 1 MB per worker per interval through the store.
  Ages need the server clock. Operation names become readable to anything with Redis access. A
  cross-host union also overlaps what Prometheus service discovery already does per replica.
  Kept as P3, demand-gated, with that design.
- **A2, fan-out at scrape time.** It gives live data and no periodic cost. It needs a second
  listener per worker on a loopback port, a registry to find the siblings (the directory, or a
  fleet report), and per-sibling timeouts. A scrape then waits for the slowest worker, and a
  worker busy in a CPU-bound handler stalls everyone's scrape or leaves a hole. Each loopback
  listener is an endpoint any local process can read. It is shape 1's registry plus a server,
  so it loses on every count except freshness. Rejected; row 5's lean gets most of the freshness
  back.
- **A3, `prometheus_client` multiprocess mode.** It does not see OTel instruments (§3). It sums
  across processes, against row 1. Rejected.
- **A4, one target per worker through `http_sd_configs`.** Any worker serves `/metrics/targets`
  and `/metrics/<instance>` from the directory, and Prometheus scrapes each worker as its own
  target. That needs no merge and gives true per-target `up`. It costs a service-discovery block
  in every user's Prometheus config, and the issue asks for one scrape. It could be added later
  on top of the same directory.
- **A5, a stable worker slot (0..N-1) instead of the UUID.** It bounds cardinality under
  recycling. But neither server exposes a worker index, slots claimed through the directory
  collide during `restart_all`'s overlap, and the issue locks the instance id. Rejected.
- **A6, merging by parse and re-render.** It validates everything. It costs 91 to 132 ms per
  snapshot (§5.3). Rejected for the line-start split, plus the write-time label check.

## 6. Tests

Unit (`tests/unit/test_forze_fastapi/`, `tmp_path`):

- The writer creates the directory `0700` and refuses a world-writable one, a symlink, and a
  file in its place. It writes `0600` files through a temp name and `os.replace`, with the header
  fields set.
- After startup, moving the directory away and putting another directory or a symlink at its
  path changes nothing: ticks and the route keep using the directory that was checked.
- The first render refuses a reader on the global registry (the process collectors carry no
  instance label) and a reader without the filter. It accepts `worker_metrics_reader()`. A
  family whose later sample names another `service_instance_id`, through a data point
  attribute, fails the check.
- The union of three synthetic snapshots parses with `text_string_to_metric_families` into one
  family per name. The family's samples are the sum of the inputs, and each sample keeps its own
  `service_instance_id`.
- A stale snapshot is left out. A file with a bad header, an oversized file, a symlink, a file
  owned by another uid and a misnamed file are skipped. A duplicate instance id serves neither
  file. A conflicting `TYPE` drops the later family. A snapshot family named
  `forze_metrics_snapshot_age_seconds` is dropped, and the route's own age family is served once.
- A label value containing `# HELP` and a space does not split a family.
- The age gauge has one sample per fresh snapshot, close to its monotonic age. A wall-clock
  step back or forward (injected clocks) withholds the old snapshots instead of extending their
  life, and the next writes are served. A file whose `mono` is ahead of the reader's monotonic
  clock, as after a reboot, is not served.
- No fresh snapshot answers `503`, and so do fresh snapshots that all share one instance.
- A worker whose renders hold no family yet writes no file, and writes its first on the tick
  whose render carries the label.
- Shutdown during a slow tick waits for its render before the final one, and skips the final
  render when the wait would pass the budget; two renders never run at once.
- A plain route on the writer's reader, scraped during a slow tick, waits for that render
  instead of starting its own.
- The sweep deletes stale files of dead pids, judged by their headers, keeps stale files of live
  ones, and deletes temp files of dead pids.
- `worker_metrics_reader()` raises `configuration` when the exporter lacks
  `resource_attribute_filter`.
- Catalog parity: the new constant and its row in `metrics.md`.

End to end (precedent: `test_serve_workers.py`): `uvicorn --workers 3` serves an app factory
with the writer step and the snapshot route. Twenty scrapes in a row each contain three distinct
`service_instance_id` values for `forze_operations_total`, and every instance's counter is
monotonic across the scrapes. After `SIGKILL` on one worker, uvicorn replaces it, a new instance
appears, and the old one is gone after `ttl`. The same test without `snapshots` must fail the
"three instances per scrape" assertion, which proves it detects the bug.

Not tested: gunicorn, which is not a dev dependency (same fork model, documented); Redis, which
is not built.

## 7. Docs

- `grafana-stack.md`: "Pull metrics, and why not" becomes two cases. One process: today's
  wiring. Several workers on one host: the §5.2 wiring, with `bootstrap_telemetry` in the
  per-worker build and never at import (gunicorn `--preload`), and a tmpfs mount, the queries
  (`sum without (service_instance_id) (rate(forze_operations_total[5m]))`), and the age-gauge
  alert. The page must say plainly that the union is concatenated and not aggregated, that a
  worker restart starts a new series, and that the pull path's names and labels differ from the
  Alloy path's, so the shipped dashboards and alert rules do not apply as they stand.
- `reference/fastapi-routes.md`: `snapshots=`, `MetricsSnapshots`, `writer_step`,
  `worker_metrics_reader`.
- `reference/metrics.md`: the `forze.metrics.snapshot.age` row.
- The docstring caveat in `health.py:190-195` points to the snapshot mode.
- P2: a minimal collector recipe, `otelcol-contrib` with an `otlp` receiver and a `prometheus`
  exporter, as a compose service and config asset, for deployments across hosts or replicas.
- `CHANGELOG.md` `[Unreleased]` → `Added`, per phase.

## 8. Out of scope

- **A union across hosts.** Prometheus discovers each replica (DNS or Docker SD in compose) and
  the in-host union covers each replica's workers. The P2 recipe covers the rest. Redis (A1)
  would change that, on demand.
- **Process collectors** (`process_*`, `python_gc_*`, `python_info`). They are not OTel
  instruments and carry no instance label, so the dedicated registry leaves them out. Adding them
  needs label injection into foreign collectors. Demand-gated.
- **Exponential histograms and exemplars on the pull path.** These are exporter and format
  limits (§3), not this RFC's to lift.
- **Renaming the pull path to Alloy's names.** That breaks today's pull users and needs its own
  decision.
- **Pre-fork servers without FastAPI.** The writer step is framework-neutral. A stdlib listener
  that serves the union, after `probe_listener_step`'s model, would follow on demand.
- **Shared instance ids under OTLP push.** The same `--preload` mistake breaks OTLP, and this RFC
  only guards the snapshot mode (§5.3 step 3).

## 9. Risks

- **Series churn under recycling.** Gunicorn `--max-requests` creates a new series set per
  recycle. That is the price of row 1, and OTLP pays it too. The docs state it, with the
  head-series arithmetic.
- **A large scrape.** About 4 MB at four workers and 200 label sets, uncompressed. The docs name
  `GZipMiddleware` and a `scrape_timeout` that fits. If measured scrapes in P1 show it is needed,
  built-in compression is a row for execution to propose.
- **Event-loop latency from the writer.** 24 to 36 ms of mostly-GIL render per interval per
  worker. Accepted at the leaned 10 s. Measured again in P1.
- **"Union" read as "aggregate".** Someone graphs `forze_operations_total` without
  `sum without (service_instance_id)` and sees N lines. The docs lead with the query.
- **The age gauge read as per-series freshness.** It describes a worker's snapshot. The catalog
  row says so.
- **A directory shared by two groups,** for example one volume mounted into an API container and
  an admin container. One scrape then serves both groups' series. The docs require one directory
  per group, and recommend a tmpfs mount private to the container.
- **`up == 1` with a worker missing.** Only `count(forze_metrics_snapshot_age_seconds)` shows it.
  The docs ship that alert.

## 10. Unresolved questions

These had to be settled before P1 started. The maintainer settled all seven on 2026-11-05,
each the lean's way (rows 4 to 10).

- Q1 (row 4): which shapes forze builds, and in which phase.
- Q2 (row 5): whether the answering worker serves itself live or from its snapshot.
- Q3 (row 6): what a graceful shutdown leaves behind.
- Q4 (row 7): the default `interval` and `ttl`.
- Q5 (row 8): how the instance label reaches the series, and the exporter floor that follows.
- Q6 (row 9): the instance label's name.
- Q7 (row 10): the public API's shape.

Implementation is free to settle the rest: module layout, log wording, and the size cap's
default.

## 11. Decisions

| # | Grade | Decision |
| --- | --- | --- |
| 1 | `LOCKED` | Each series stays per instance. Every series in the union carries the worker's `service.instance.id` as a label, and forze never sums, averages or merges samples across workers. Aggregation is the query's job. From the issue's constraints. It locks in series churn per restart (§9), and a later "summed" mode would need its own RFC. |
| 2 | `LOCKED` | Staleness is bounded and visible. A snapshot older than `ttl` is not served, and every served worker has one `forze_metrics_snapshot_age_seconds{service_instance_id}` sample, computed when the route answers. From the issue's constraints. |
| 3 | `LOCKED` | Nothing changes for single-process apps or OTLP apps. `attach_metrics_route` without `snapshots` keeps today's code path, names and content type. `bootstrap_telemetry` gains no parameter. No dependency or extra is added. From the issue's constraints. |
| 4 | `LOCKED` | Accepted by the maintainer on 2026-11-05 (§10 Q1): directory snapshots in `forze_fastapi` ship in P1, the collector recipe in P2, and a Redis/Valkey store through a new core port in P3, on demand. Fan-out at scrape time is rejected (§5.8 A2). The directory covers exactly the pre-fork group, which shares a host, with no new port and no new service. A cross-host store needs core contract work for a union Prometheus SD already gives. |
| 5 | `LOCKED` | Accepted by the maintainer on 2026-11-05 (§10 Q2): every worker, including the answering one, is served from the directory, and a scrape renders nothing. Every worker's data has the same age semantics, a scrape never runs observable callbacks, and no render gate is shared between route and writer. Serving the answering worker live was rejected: it makes one worker fresher than the others at random. |
| 6 | `LOCKED` | Accepted by the maintainer on 2026-11-05 (§10 Q3): a graceful shutdown writes one final snapshot and leaves it until `ttl`. One more scrape then captures the counters' final increments since the last tick. The price is a dead worker's gauges served for up to `ttl`, with their age shown. Deleting the file at shutdown was rejected: it loses those increments on every deploy. |
| 7 | `LOCKED` | Accepted by the maintainer on 2026-11-05 (§10 Q4): `interval` defaults to 10 s, and `ttl` to 3 × `interval` (30 s) unless set. A `ttl` below 2 × `interval` is refused (`configuration`). At 10 s the writer costs about what one scrape every 10 s costs today, and staleness stays under a 15 s scrape interval plus one tick. A load test sets 1 s. |
| 8 | `LOCKED` | Accepted by the maintainer on 2026-11-05 (§10 Q5): forze ships `worker_metrics_reader()`, which builds `PrometheusMetricReader(registry=CollectorRegistry(), resource_attribute_filter=<service.instance.id only>)`, and the writer checks every render. Snapshot mode requires `opentelemetry-exporter-prometheus` 0.66b0 or newer, checked at wiring. The check refuses the global-registry reader eis-backend uses today. Rewriting sample lines for older exporters was rejected. |
| 9 | `LOCKED` | Accepted by the maintainer on 2026-11-05 (§10 Q6): the instance label is `service_instance_id`, the exporter's sanitized resource attribute. It needs no scrape configuration and cannot be overwritten silently. `instance` with `honor_labels: true` was rejected: the pull path already differs from Alloy's in names (§3), so it buys no dashboard compatibility by itself. |
| 10 | `LOCKED` | Accepted by the maintainer on 2026-11-05 (§10 Q7): a `MetricsSnapshots(directory, interval, ttl, max_snapshot_bytes)` value with `writer_step(reader)`, and `attach_metrics_route(..., snapshots=...)`. One value configures both halves, so the writer and the route cannot disagree on directory or `ttl`, and the route stays one function. A separate route function and discovery from an environment variable were rejected. |
| 11 | `ASSUMED` | A snapshot is the reader's own text rendering, with one header line. The route merges by splitting where a line starts with `# HELP` and a space, and never parses samples (2.3 ms for four workers against 91 to 132 ms per snapshot, §5.3). The writer checks the label on every sample line (about 7 ms per MB), because a data point's own attribute can override the resource label. |
| 12 | `ASSUMED` | Directory and file protocol, per §5.2 and §5.3: the directory is created `0700`, or refused when it is a symlink, foreign-owned, or group- or world-writable, then held open; every later operation goes through its descriptor, never its path. Files are `0600`, written through an exclusive temp file and `os.replace`, named `<pid>-<token>.prom`, and read with `O_NOFOLLOW`, only when owned by the effective uid, under a size cap. A refusal at startup fails the boot, after the fleet step's precedent. |
| 13 | `ASSUMED` | Two fresh snapshots claiming one instance id are both withheld, and an error names the pids. Serving either one would interleave two processes into one series. |
| 14 | `ASSUMED` | The age is the monotonic age: the answering worker's `CLOCK_MONOTONIC` minus the header's `mono`. A snapshot is fresh only while that age and the wall-clock age (`time.time() - written_at`) both lie between 0 and `ttl`, so a wall-clock step cannot extend a dead worker's life and a file from before a reboot is not served. That is sound only because every worker of a group shares one host clock. A cross-host store (P3) must use the store's clock. |
| 15 | `ASSUMED` | One directory serves one worker group. No group key is in P1, and the docs require a directory per group. |
| 16 | `ASSUMED` | With no snapshot left to serve, because none is fresh or every fresh one is withheld as a duplicate, the route answers `503`, so `up == 0` shows that no worker is served. Otherwise it answers `200` with whatever is served. |
| 17 | `ASSUMED` | Each tick sweeps files older than `ttl` whose pid no longer exists, and stray temp files by the same rule. A stalled but live worker is withheld, not swept. |
| 18 | `ASSUMED` | The code lives in `forze_fastapi.routes`, behind a private module, with the age gauge's constant declared there. There is no core change, no new dependency and no new extra. The Redis store, if built, adds the only core port. |
| 19 | `ASSUMED` | The snapshot file's token is derived from the process's `instance_id()`, so every writer in one process writes the same file. (D-211) |
| 20 | `ASSUMED` | The final render's budget is one `interval`, accepted by the maintainer over a smaller fixed bound that would skip a wedged final render before the runtime's step timeout. A final render past the budget is skipped and writes nothing, so row 6's final snapshot then holds the last tick's counters, and increments since that tick are lost. (D-212) |
| 21 | `ASSUMED` | `MetricsSnapshots.union()` is public: blocking, returning `(status, body)` exactly as the route answers. (D-213) |
| 22 | `ASSUMED` | One descriptor per process, opened and checked by whichever of the writer and the route comes first. A route that cannot open it answers `503`. (D-217) |

Rows 19 to 22 were added by execution and accepted by the maintainer on 2026-11-05; each cites
the [`EXECUTION-LOG.md`](EXECUTION-LOG.md) entry it came from, and the sections above are left
as designed. Row 20 bounds the final snapshot of row 6, which stays `LOCKED` as written.

## 12. Phasing

- **P1, one PR:** W1, W2 and W3 with `worker_metrics_reader()`, the unit and end-to-end tests
  (§6), the docs for the snapshot mode (§7), and a changelog `Added` entry. Rows 4 to 10
  are settled.
- **P2, docs only, independent:** the minimal collector recipe and its config asset, covered by
  the observability-assets integration test the way the Alloy config is. It can land before P1.
- **P3, demand-gated, not scheduled:** the Redis/Valkey store, built on the first deployment
  that needs one scrape across containers without service discovery. It needs a core port, a
  `forze_redis` adapter, a mock and a shared battery, plus the server clock for ages (row 14).
