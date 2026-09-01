---
title: The Grafana stack
icon: lucide/layout-dashboard
summary: From instrument_operations to a populated Grafana — compose, Alloy, dashboards, alerts, and the label discipline that keeps it affordable
---

[Observability](observability.md) gets your application emitting. This page is the other
half: where those signals go, and what you look at when something is wrong. It is a
prescription, not an abstraction — the framework ships contracts (JSON on stdout, OTLP,
probes) plus these assets, and never ships deployment code.

## The wiring picture

Your application talks to exactly one thing: **Alloy**. Everything else is behind it,
which is what lets you swap Loki for Elasticsearch or Prometheus for Mimir without
touching a service.

```
  your app ──stdout JSON──► Alloy ──► Loki    ─┐
           ──OTLP/http────► Alloy ──► Tempo   ─┼──► Grafana
                                  └─► Prometheus (remote-write)
  node exporter ─────────────────────► Prometheus     (infrastructure; framework-uninvolved)
```

Three facts make this work without any glue code of yours:

- Metrics and traces already leave through the **global OTel providers**, so pointing them
  somewhere is a matter of installing an SDK — that is all `bootstrap_telemetry` does.
- Logs are already **JSON on stdout** with `trace_id` and `span_id` injected from the
  active span, so a log-to-trace jump in Grafana is a datasource setting, not a code
  change.
- `traceparent` already crosses the outbox → broker → inbox boundary, so a trace that
  starts in an HTTP request continues in the consumer that handles its event.

## One `docker compose up`

Download the stack and start it:

<div class="grid cards" markdown>

- :material-download: **[docker-compose.yml](assets/grafana/docker-compose.yml)** — Alloy, Prometheus, Loki, Tempo, Grafana
- :material-download: **[config.alloy](assets/grafana/config.alloy)** — the whole ingest pipeline
- :material-download: **[prometheus.yml](assets/grafana/prometheus.yml)** · **[tempo.yml](assets/grafana/tempo.yml)** — backend config
- :material-download: **[alerts.yml](assets/grafana/alerts.yml)** — the starter rule pack
- :material-download: **provisioning:** [datasources](assets/grafana/provisioning/datasources/datasources.yml) · [dashboards](assets/grafana/provisioning/dashboards/dashboards.yml)
- :material-download: **dashboards:** [operations](assets/grafana/dashboards/forze-operations.json) · [resilience](assets/grafana/dashboards/forze-resilience.json) · [data planes](assets/grafana/dashboards/forze-data-planes.json) · [realtime](assets/grafana/dashboards/forze-realtime.json)

</div>

Keep the layout — the compose file mounts `./provisioning` and `./dashboards` by path:

```text
grafana/
  docker-compose.yml   config.alloy   prometheus.yml   tempo.yml   alerts.yml
  dashboards/          forze-*.json
  provisioning/        datasources/datasources.yml   dashboards/dashboards.yml
```

```bash
docker compose up -d
open http://localhost:3000
```

Grafana comes up with the datasources wired, the four dashboards in a **Forze** folder,
and Prometheus already evaluating the alert rules. Nothing is populated yet, because
nothing is pointed at it.

## Point your application at it

Two calls at process start, before assembly:

```python
from forze.base.logging import bootstrap_logging
from forze.base.telemetry import bootstrap_telemetry

bootstrap_logging(render_mode="json", otel_config={"enable": True})

telemetry = bootstrap_telemetry(
    service_name="orders-api",
    service_version=APP_VERSION,
)
```

and one environment variable:

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=http://alloy:4318
```

Endpoint, headers, timeouts and the head sampler all come from the standard `OTEL_*`
variables; `bootstrap_telemetry` deliberately adds no second vocabulary for them. Install
the exporter with the `observability` extra:

```bash
uv add "forze[observability]"
```

Both argument lists are also a settings model, so a deployment configures them from the
environment instead of retyping the defaults:

```python
from forze.base.settings import RuntimeSettings

rt = RuntimeSettings(version=APP_VERSION, build_id=BUILD_ID, telemetry="otlp")

bootstrap_logging(level=rt.log_level, render_mode=rt.log_render)
bootstrap_telemetry(
    service_name="orders-api",
    service_version=rt.full_version,
    exporter=rt.telemetry,
)
```

`RuntimeSettings` is a plain `BaseModel` — mount it on your own `BaseSettings` root, which
owns the environment prefix and delimiter. It defaults `log_render` to `json`, unlike
`bootstrap_logging` itself: a settings object exists because something is being deployed.

Then flush on the way out, after the drain gate flips:

```python
@asynccontextmanager
async def lifespan(app: FastAPI):
    async with runtime.scope():
        yield

    # After the scope drained and tore down, but before the process exits: the last export
    # interval is exactly the one that says why the pod went away.
    await telemetry.shutdown()
```

!!! warning "Order matters at shutdown"

    Shut telemetry down **before** closing clients and pools. The final metric collection
    runs observable callbacks, and those read live objects — pool stats, keyring stats,
    bulkhead depths. Tear the clients down first and the last collection reports numbers
    from a half-disposed object, or raises inside the exporter thread.

The dashboards stay empty until something is actually instrumented — `bootstrap_telemetry`
installs the SDK, it does not emit. Pair it with the `instrument_*` calls from
[Observability](observability.md), and the operations dashboard fills in within one export
interval of the first request.

## Metric names in Prometheus

The shipped Alloy config sets `add_metric_suffixes = false` on the Prometheus exporter,
which makes the mapping trivial: **the OTel name with dots turned into underscores**.

| OpenTelemetry | Prometheus |
|---|---|
| `forze.operations` | `forze_operations` |
| `forze.operation.duration` | `forze_operation_duration_bucket` / `_sum` / `_count` |
| `forze.crypto.cold_miss` | `forze_crypto_cold_miss` |

Leave suffixes on — the collector default — and the same metrics arrive as
`forze_operations_total` and `forze_operation_duration_milliseconds_bucket`. Both are
fine; they are just different, and the shipped dashboards and alert rules assume the
first. A unit test checks every expression in those files against the metric constants in
`src/`, which is only possible because the mapping is pinned — and an integration test
puts a real Alloy in front of a real Prometheus and asserts the names that actually
arrive, so the pinning is verified rather than assumed.

Attribute names follow the same rule: `forze.outcome` becomes the label `forze_outcome`.
Alloy also derives `job` from `service.name` and `instance` from `service.instance.id`.

!!! warning "Leave the temporality preference alone"

    Every `rate()` in the shipped dashboards and rules assumes **cumulative** temporality,
    which is the OTLP exporter's default. Setting
    `OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE=delta` makes each export carry the
    interval's delta instead of a running total, and `rate()` over that is wrong — quietly,
    with plausible-looking numbers. Delta is the right choice for some backends; it is not
    the one these assets were written against.

## Label discipline

This is the one section worth reading twice, because both indexes here are priced by
cardinality and both are easy to ruin from application code.

**Loki labels come from container metadata only.** `tenant_id`, `trace_id`,
`correlation_id`, `principal_id` stay *inside* the JSON line and are queried with `| json`:

```logql
{service="orders-api"} | json | tenant_id="acme" | level="error"
```

Promote `tenant_id` to a label and Loki's stream count multiplies by your tenant count —
an unbounded dimension in the index, for a field you can filter on perfectly well at query
time. `trace_id` is handled separately, as structured metadata, which is what makes the
Grafana log-to-trace link work without indexing it.

**`tenant_id` is never a metric label.** Not in Forze, not in your own instruments. It is
a span attribute and a log field, both of which already carry it, and per-tenant questions
are answered from those. A per-tenant metric is a time series per tenant per metric per
process, forever — including for tenants that churned out a year ago.

**Alert on fields, not on message text.** The log sanitizer rewrites message strings to
scrub secrets and PII. A LogQL rule matching a substring of a message is matching
something the scrubber is allowed to change; match structured fields instead.

## Sampling

Two places, and they are not interchangeable.

**Head sampling** is `OTEL_TRACES_SAMPLER` in the application. It is cheap and it is blind:
the decision happens before anyone knows whether the trace was interesting.

**Tail sampling** is a commented-out block in `config.alloy`. The collector has seen the
whole trace, so it can keep every error and every slow request and 5% of the rest. That is
almost always the policy you want; it costs the collector memory for the decision window.

One volume note specific to Forze: outbound port spans are emitted **per retry attempt**.
A dependency that flaps does not add spans linearly — a policy with five attempts
quintuples that route's span volume exactly when the system is least healthy. Tail
sampling handles this well (those traces are the interesting ones); a flat head sample
does not.

## Histogram buckets

`bootstrap_telemetry` installs views on `forze.operation.duration` and
`forze.durable.run.duration` with an explicit **millisecond** ladder — 1, 2, 5, 10, 25, 50,
100, 250, 500, 1 000, 2 500, 5 000, 10 000, 30 000, 60 000.

The SDK's default boundaries are second-oriented. Applied to millisecond values they put
every sub-5 ms operation in one bucket and everything over 10 s in the overflow, which
makes both the p50 of a fast handler and the p99 of a slow one unreadable.

For Prometheus native histograms, pass `exponential_histograms=True`. To add your own
views without losing these, compose them:

```python
from forze.base.telemetry import bootstrap_telemetry, millisecond_histogram_views

bootstrap_telemetry(
    service_name="orders-api",
    histogram_views=[*millisecond_histogram_views(), *my_views],
)
```

The views apply only to the provider `bootstrap_telemetry` creates. An application that
installed its own SDK owns its aggregation, and the bootstrap defers to it entirely.

## Counter resets

Most of the framework's gauges and counters are **observable** and **cumulative per
process** — pool stats, keyring stats, L1 stats, realtime counters, signing counters. A
restart resets them to zero.

`rate()` handles resets correctly *if* it can tell one process from another. That is what
`service.instance.id` is for, and why `bootstrap_telemetry` mints a fresh UUID per process
by default. Override it only with something equally unique: a pod name is not unique
across a pre-fork server's workers; a pod name plus worker index is.

This is also why the blessed path is push, not scrape — see below.

## Pull metrics, and why not

There is a pull endpoint:

```python
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from forze_fastapi.routes import attach_metrics_route

reader = PrometheusMetricReader()
bootstrap_telemetry(service_name="orders-api", extra_metric_readers=[reader])
attach_metrics_route(router, reader)
```

It is correct for a **single-process** deployment and wrong for any other. Under a pre-fork
server the scrape is answered by whichever worker accepted the connection, so consecutive
scrapes sample different processes: cumulative counters interleave, `rate()` sees phantom
resets, gauges flap between workers. Nothing inside the endpoint can fix that — it is a
property of one socket in front of N independent processes.

Multi-worker deployments push over OTLP, where each process stays its own series.
`prometheus_client` is deliberately not part of the `observability` extra; install
`opentelemetry-exporter-prometheus` yourself if you want this route.

## Probes

Every process gets both probes, including the ones that are not HTTP servers.

=== "FastAPI"

    ```python
    from forze_fastapi.routes import attach_liveness_route, attach_readiness_route

    attach_liveness_route(router)              # /livez
    attach_readiness_route(router, runtime)    # /readyz
    ```

=== "Workers"

    ```python
    from forze.application.execution.background import probe_listener_step

    lifecycle_steps.append(probe_listener_step(runtime, port=8079))
    ```

    Stdlib `asyncio` only — an outbox relay does not grow an HTTP framework to answer two
    endpoints. Serves the same paths, from the same runtime state.

Kubernetes doctrine:

| Probe | Path | Why |
|---|---|---|
| `livenessProbe` | `/livez` | answers `200` while the event loop schedules work — reaching the handler *is* the check |
| `readinessProbe` | `/readyz` | `ready` / `draining` / `unavailable`, from the runtime's drain gate |
| `startupProbe` | `/readyz` | same endpoint, generous `failureThreshold` for slow assembly |

The separation is the point. A draining pod is **alive but not ready**: readiness takes it
out of rotation, liveness keeps the kubelet from killing it mid-drain. One endpoint forced
to answer both questions cannot express that, and a slow drain gets restarted instead of
finished.

Probes are the **restart** signal. They cannot see a loop that is running but making no
progress — for that you keep alerting on the gauges: `forze.realtime.backplane
.seconds_since_ok`, `forze.jobs.staleness.scan_age`, `forze.realtime.mailbox.overflowed`.
Both, not either.

## Dashboards

Four, provisioned into a **Forze** folder:

| Dashboard | What it answers |
|---|---|
| **Operations** | throughput, the `error` vs `failed` split, latency distribution, durable runs, job progress |
| **Resilience** | breaker states, event rates by kind, bulkhead depth against the adaptive limit, hedge delay |
| **Data planes** | tenant-pool size/capacity/churn, KMS round-trips and cache hit ratio, document L1, token signing |
| **Realtime** | gateway delivery outcomes, backplane freshness, offline mailbox |

Every panel queries a metric that exists — checked by a unit test against the constants in
`src/`, in both directions, so a renamed metric fails CI instead of silently emptying a
panel.

## Alerts

[`alerts.yml`](assets/grafana/alerts.yml) codifies the alarms that until now lived only in
docstrings. Each rule carries its reasoning in the annotation, because a threshold without
a "what this means, what to do" is a page nobody can action.

| Alert | The signal |
|---|---|
| `ForzeOperationErrorRateHigh` | genuine faults only — 4xx-class domain failures are excluded by design |
| `ForzeDurableRecoverySustained` | runs keep being reclaimed from processes that died holding them |
| `ForzeJobsStalled` | stuck jobs **or** a dead staleness sweep |
| `ForzeCircuitBreakerOpen` | a route has been shedding for five minutes |
| `ForzeBulkheadSaturated` | continuous queueing plus rejections |
| `ForzeCryptoColdMiss` | a path is skipping `warm` / `ensure_unwrapped` |
| `ForzeTenantPoolThrash` | pool creation while the cache sits at capacity |
| `ForzeTokenVerificationFailures` | rotation gaps, clock skew, or forgeries |
| `ForzeRealtimeBackplaneStale` | cross-node emit is silently down |
| `ForzeRealtimeDeliveriesDropped` | poisoned or untenanted deliveries |
| `ForzeRealtimeMailboxOverflow` | a device lost signals it will never see |

!!! danger "The `or` in `ForzeJobsStalled` is load-bearing"

    ```promql
    sum by (job, forze_job_kind) (forze_jobs_stalled) > 0
      or max by (job) (forze_jobs_staleness_scan_age) > 300
    ```

    The stalled gauge reads a cache the sweep fills. If the sweep loop dies, the gauge
    freezes at its last value — almost always zero — and a rule watching only the count
    goes green at the exact moment it stops knowing anything. The second clause is what
    makes silence loud.

Thresholds are starting points. Tune them against your own traffic; the annotations tell
you what you are trading.

## Exemplars

Grafana can jump from a histogram bucket straight to a trace, if the SDK attached an
exemplar. The shipped datasource leaves `exemplarTraceIdDestinations` empty on purpose:
exemplar behavior at the pinned OpenTelemetry version is not something the framework
promises, and a derived link that never resolves is worse than no link at all. Verify it
in your own stack first, then enable it.

Log-to-trace correlation needs no such caveat — `trace_id` is on every JSON line, and the
provisioned Loki datasource already links it to Tempo.

## Next

- [Metric catalog](../reference/metrics.md) — every metric, its labels, and the alert that matters
- [Observability](observability.md) — what is emitted and how to instrument it
- [Shutdown and fleets](shutdown-and-fleets.md) — the drain window the readiness probe reports on
