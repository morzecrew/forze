---
title: Metric catalog
icon: lucide/gauge
summary: Every metric Forze emits — name, instrument, unit, labels, and the alert that matters
---

Every metric below is emitted through the **global OpenTelemetry meter** named `forze`,
by an `instrument_*` call you make once at assembly. Nothing appears until you make that
call — the instrumentation surface is opt-in per plane. Point an SDK at a collector with
[`bootstrap_telemetry`](../running-in-prod/observability.md#one-call-sdk-setup) and see
them in Grafana with [the stack recipe](../running-in-prod/grafana-stack.md).

Names are **stable**. This table is checked against the constants in `src/` by a unit
test, in both directions: a metric that exists but is not documented fails, and so does a
documented metric that no longer exists.

Conventions that hold throughout:

- **Unit `1`** means a plain count. `ms` is milliseconds, `s` seconds, `By` bytes.
- **Observable** instruments are sampled by the SDK at collection time from a live object
  (pool, keyring, cache, mailbox). They are *cumulative per process*, so a restart resets
  them — `rate()` handles that correctly only if each process carries a distinct
  `service.instance.id`, which is why `bootstrap_telemetry` mints one by default.
- **`tenant_id` is never a metric label**, anywhere, by design. Per-tenant questions are
  answered from traces and logs, which both carry it. See
  [the cardinality doctrine](../running-in-prod/grafana-stack.md#label-discipline).

## Operations

`instrument_operations(registry)` — the outermost middleware on every operation.

| Metric | Instrument | Unit | Labels | Notes |
|---|---|---|---|---|
| `forze.operations` | counter | 1 | `forze.operation`, `forze.operation.kind`, `forze.outcome` | `kind` is `command` / `query`; `outcome` is `success` / `failed` / `error` |
| `forze.operation.duration` | histogram | ms | same as above | ms-ladder buckets installed by `bootstrap_telemetry` |

**The distinction that matters:** `outcome="failed"` is a client-class domain failure
(validation, not-found, conflict, precondition — a 4xx the caller may well handle) and
leaves the span clean. `outcome="error"` is a genuine fault. Alert on `error`, chart both.

## Resilience

`instrument_resilience(executor)` — independent of the tracing gate; a production process
with tracing off still reports these.

| Metric | Instrument | Unit | Labels | Notes |
|---|---|---|---|---|
| `forze.resilience.events` | counter | 1 | `forze.event`, `forze.policy`, `forze.route` | retries, timeouts, rate-limit/bulkhead rejections, budget exhaustion, breaker transitions |
| `forze.resilience.breaker.state` | gauge | 1 | `forze.policy`, `forze.route` | `0` closed, `1` half-open, `2` open. A breaker that never tripped reports nothing — closed by absence |
| `forze.resilience.bulkhead.queue_depth` | observable gauge | 1 | `forze.policy`, `forze.route` | calls queued behind the semaphore |
| `forze.resilience.bulkhead.in_use` | observable gauge | 1 | `forze.policy`, `forze.route` | calls holding a slot right now, for every bulkhead kind |
| `forze.resilience.bulkhead.limit` | observable gauge | 1 | `forze.policy`, `forze.route` | concurrency limit: a fixed bulkhead's cap, or the current AIMD / Gradient2 limit. `in_use / limit` is occupancy |
| `forze.resilience.hedge.delay` | observable gauge | s | `forze.policy`, `forze.route` | effective adaptive hedge delay (windowed P² estimate) |

`breaker_open` in the events counter covers both the open *transition* and every admission
rejected while open, so its rate tracks shed load rather than flap count.

## Tenant pools

`instrument_tenant_pools({"postgres": client, ...})` — one entry per routed client.

| Metric | Instrument | Unit | Labels | Notes |
|---|---|---|---|---|
| `forze.tenancy.pool.size` | observable gauge | 1 | `forze.client` | live tenant pools |
| `forze.tenancy.pool.capacity` | observable gauge | 1 | `forze.client` | `max_cached_tenants` |
| `forze.tenancy.pool.created` | observable counter | 1 | `forze.client` | cumulative pool creations |
| `forze.tenancy.pool.disposed` | observable counter | 1 | `forze.client` | cumulative pool disposals |
| `forze.tenancy.pool.evicted_explicit` | observable counter | 1 | `forze.client` | explicit evictions (rotation signals) |

**The alert that matters:** a sustained `created` rate while `size == capacity` is LRU
thrash — hot tenants' pools evicted by cold one-off traffic, each rebuild paying full
connection establishment.

## Connection pools

`instrument_connection_pools({"postgres": client, ...})` — one entry per client that
implements `SupportsConnectionPoolStats`. Today that is `PostgresClient`; Redis, MongoDB
and outbound HTTP follow.

| Metric | Instrument | Unit | Labels | Notes |
|---|---|---|---|---|
| `forze.connection_pool.in_use` | observable gauge | 1 | `forze.client` | connections checked out, or being opened or reopened by the pool |
| `forze.connection_pool.idle` | observable gauge | 1 | `forze.client` | open connections not checked out |
| `forze.connection_pool.max` | observable gauge | 1 | `forze.client` | configured cap (`max_size`) |
| `forze.connection_pool.waiting` | observable gauge | 1 | `forze.client` | callers queued for a connection; absent where the driver does not expose it |

A client with no open pool reports nothing. A routed client is refused: summing its
per-tenant pools would hide the one that is saturated. Its connections are not exported;
the tenant pool gauges above report only its pool cache (size and churn).

**The alert that matters:** `waiting` above zero across scrapes, or `in_use` pinned at
`max`, means callers queue for connections and will start failing at the acquire timeout.
`in_use` that climbs under steady load and never falls back is a leaked connection.
A database that is down shows the same pattern: the pool's reconnect attempts hold `in_use`
at `max` with `waiting` above zero while no caller saturates the pool, so pair the alert
with a connectivity signal. On Postgres, a caller that timed out still counts in `waiting`
until a connection is returned to the pool, so `waiting` stays up for as long as every
connection is held or the database is unreachable.

## Crypto and KMS

`instrument_crypto({"default": keyring, ...})`.

| Metric | Instrument | Unit | Labels | Notes |
|---|---|---|---|---|
| `forze.crypto.data_keys.generated` | observable counter | 1 | `forze.keyring` | KMS round-trips on the encrypt path |
| `forze.crypto.data_keys.unwrapped` | observable counter | 1 | `forze.keyring` | KMS round-trips on the decrypt path |
| `forze.crypto.cache.hits` | observable counter | 1 | `forze.keyring`, `forze.crypto.path` | `path` is `encrypt` / `decrypt`; data keys reused without a KMS call |
| `forze.crypto.cold_miss` | observable counter | 1 | `forze.keyring` | synchronous crypt that hit a cold cache and raised `cipher_not_warm` |

Hit ratio is `hits / (hits + the matching generated|unwrapped)` — composed at query time,
never precomputed, so it aggregates correctly across processes.

**The alert that matters:** `cold_miss` should sit at ~0. A sustained rate means a
read/write path is skipping `warm` / `ensure_unwrapped`.

## Document L1 cache

`instrument_document_l1()` — reads a process-wide registry of live L1 stores.

| Metric | Instrument | Unit | Labels | Notes |
|---|---|---|---|---|
| `forze.cache.l1.size` | observable gauge | 1 | `forze.document` | live entries |
| `forze.cache.l1.capacity` | observable gauge | 1 | `forze.document` | configured capacity |
| `forze.cache.l1.hits` | observable counter | 1 | `forze.document` | cumulative hits |
| `forze.cache.l1.misses` | observable counter | 1 | `forze.document` | cumulative misses |
| `forze.cache.l1.evictions` | observable counter | 1 | `forze.document` | includes rejected admissions |

Sustained evictions while `size == capacity` is the scan-pollution signature that
justifies W-TinyLFU (`L1Spec(store_factory=tiny_lfu_l1_store)`) or a bigger capacity.

## Process

`instrument_process()` — this process's own CPU and memory, from the standard library. The
names are OpenTelemetry's
[process semantic conventions](https://opentelemetry.io/docs/specs/semconv/system/process-metrics/),
not `forze.*`.

| Metric | Instrument | Unit | Labels | Notes |
|---|---|---|---|---|
| `process.cpu.time` | observable counter | s | `cpu.mode` | `user` / `system`, from `os.times()`; `rate()` is the cores this process uses |
| `process.memory.usage` | observable up-down counter | By | — | resident memory from `/proc/self/statm`. **Linux only**: absent elsewhere |

Outside Linux the standard library has only the *peak* resident size, and a peak exported
as the current value would hide the leak it is meant to show, so the instrument is skipped.

The OpenTelemetry Prometheus exporter renders these as `process_cpu_time_seconds_total` and
`process_memory_usage_bytes`, not prometheus_client's `process_cpu_seconds_total` /
`process_resident_memory_bytes`, so panels built on those need new queries. A default
`PrometheusMetricReader()` renders the default registry, whose own process collector reports
that pair beside these; `worker_metrics_reader()` renders a registry without it.

Each process reports its own series. A pre-fork server scraped through one port answers from
whichever worker takes the scrape, unless every worker writes a snapshot through
`worker_metrics_reader()` and `MetricsSnapshots`, which label each series
`service_instance_id`. See [Several workers, one scrape](../running-in-prod/grafana-stack.md#several-workers-one-scrape).

## Durable execution

`DurableTelemetry.create()`, wired into the durable runner.

| Metric | Instrument | Unit | Labels | Notes |
|---|---|---|---|---|
| `forze.durable.runs` | counter | 1 | `forze.durable.name`, `forze.durable.outcome` | outcome is `completed` / `failed` / `retrying` / `forward_incomplete` / `cancelled` / `timed_out` / `reclaimed` / `interrupted` / `unrecorded` — alert on `failed`, not on the total: `cancelled` means somebody pressed Stop, and `retrying` is an attempt that failed and sent its run back for another under the runner's retry strategy. `unrecorded` describes the *attempt*, not the run: the body finished and its terminal write went unacknowledged, so the row is either already terminal (the write committed, the ack was lost) or still `RUNNING` for recovery — the worker cannot tell. A sustained rate of it means the run store is unreachable. `interrupted` is the neighbouring case where the body never finished at all (drain, shutdown) and no terminal state was written — expected during a deploy |
| `forze.durable.run.duration` | histogram | ms | `forze.durable.name`, `forze.durable.outcome` | ms-ladder buckets installed by `bootstrap_telemetry` |
| `forze.durable.recovered` | counter | 1 | — | runs a recovery sweep claimed: reclaimed from a process that died holding them, or simply due (enqueued, back for a retry, requeued) |
| `forze.durable.schedule.fires` | counter | 1 | `forze.durable.name` | one per schedule fire |

A `recovered` spike means runs are being reclaimed from processes that died holding them —
normal in small numbers after a deploy, a signal when it is sustained.
The counter cannot tell those from runs that were simply due, so enqueued runs, retries
under a retry strategy and requeues set its baseline: alert above it.

## Leadership

`instrument_leadership()` — reads every `leadership_lifecycle_step` in the process.

| Metric | Instrument | Unit | Labels | Notes |
|---|---|---|---|---|
| `forze.leadership.held` | observable gauge | 1 | `forze.leadership.key` | `1` while this process holds the lease, through its term's grace and until the release |
| `forze.leadership.terms_ended` | observable counter | 1 | `forze.leadership.key`, `forze.leadership.reason` | reason is `lost` / `expiring` / `stopped` / `yielded` / `returned` / `raised` / `unclaimed` |

Summed across processes, `held` should be `1` per key. A sustained rate of `lost` or
`expiring` means the lock backend is slow or unreachable from the leader; `raised` is the
body's own failure.

## Job progress

`instrument_job_staleness(monitor)` — see [Operation
progress](../running-in-prod/operation-progress.md).

| Metric | Instrument | Unit | Labels | Notes |
|---|---|---|---|---|
| `forze.jobs.stalled` | observable gauge | 1 | `forze.job.kind` | started, unfinished, silent past the window |
| `forze.jobs.stalled.oldest_silence` | observable gauge | s | `forze.job.kind` | seconds since the quietest stuck job reported; computed at scrape |
| `forze.jobs.staleness.scan_age` | observable gauge | s | — | seconds since the last successful sweep; `-1` before the first |

Undeclared kinds land in the `__other__` bucket rather than vanishing, so a kind added
later is still counted.

**Alarm on `scan_age` too, not only on `stalled`.** The counts are a cache: if the sweep
loop dies they freeze at their last value — almost always zero — and a rule watching only
the count goes green at the exact moment it stops knowing anything.

## Realtime gateway

`instrument_realtime_gateway(stats)` — the live-emit path. All labelled
`forze.realtime.channel`.

| Metric | Instrument | Unit | Notes |
|---|---|---|---|
| `forze.realtime.gateway.emitted` | observable counter | 1 | frames delivered to Socket.IO |
| `forze.realtime.gateway.emit_failed` | observable counter | 1 | `sio.emit` raised, including emit-timeout expiries |
| `forze.realtime.gateway.presence_skipped` | observable counter | 1 | empty principal room; recoverable via the mailbox |
| `forze.realtime.gateway.dedup_skipped` | observable counter | 1 | durable signals already seen |
| `forze.realtime.gateway.admission_rejected` | observable counter | 1 | rejected at the catalog admission gate |
| `forze.realtime.gateway.untenanted_dropped` | observable counter | 1 | no tenant resolved on a `require_tenant` gateway |
| `forze.realtime.gateway.mailboxed` | observable counter | 1 | stored for offline replay |
| `forze.realtime.gateway.bridge_failed` | observable counter | 1 | redelivered if durable, dropped if ephemeral |
| `forze.realtime.gateway.poisoned` | observable counter | 1 | dropped at the delivery ceiling — bounded loss |

**The two to alarm on:** `poisoned` (every increment is a dropped durable delivery) and
`emit_failed` climbing while `emitted` is flat (Socket.IO, or its Redis backplane, stopped
taking frames).

**Several channels in one process:** call once per channel. The gateway, backplane,
connections and hub calls share one set of instruments per meter, so each channel exports
its own series. A repeat call for the same object and channel adds nothing.

```python
from forze.application.execution.realtime import (
    RealtimeGatewayStats,
    instrument_realtime_gateway,
)

orders, chat = RealtimeGatewayStats(), RealtimeGatewayStats()
instrument_realtime_gateway(orders, channel="orders")
instrument_realtime_gateway(chat, channel="chat")
```

## Realtime backplane

`instrument_realtime_backplane(health)`, fed by the heartbeat lifecycle step. Labelled
`forze.realtime.channel`.

| Metric | Instrument | Unit | Notes |
|---|---|---|---|
| `forze.realtime.backplane.seconds_since_ok` | observable gauge | s | seconds since the backplane last accepted a probe; `-1` means never |
| `forze.realtime.backplane.consecutive_failures` | observable gauge | 1 | failed probes since the last success |

A dead `AsyncRedisManager` listener silently stops every cross-node emit and nothing in
python-socketio surfaces it. Alarm on `seconds_since_ok` exceeding a few heartbeat
intervals; `-1` is a wiring problem, not an outage.

## Realtime connections

`instrument_realtime_connections(stats)`, with the same `RealtimeConnectionStats` handed to
`attach_realtime_connection` and `connection_drain_lifecycle_step`. Labelled
`forze.realtime.channel`.

| Metric | Instrument | Unit | Labels | Notes |
|---|---|---|---|---|
| `forze.realtime.connections` | observable up-down counter | 1 | `forze.realtime.namespace` | connections open on this node |
| `forze.realtime.connect_refused` | observable counter | 1 | `forze.realtime.namespace`, `forze.realtime.reason` | `protocol`, `auth`, `draining`, `admission`, `internal`; a `draining` connect is turned away by closing its transport |
| `forze.realtime.drain_closed` | observable counter | 1 | `forze.realtime.outcome` | closes the drain started: `closed`, `failed`, `timed_out` |

**What to watch:** `connect_refused{reason="draining"}` that keeps climbing long after a
deploy means a balancer still routes to a replica it should have dropped, and any
`drain_closed{outcome="timed_out"}` means the drain's tail is too short for this node's
closes.

## Realtime hub

`instrument_realtime_hub(hub)` from `forze_fastapi.realtime`, with the `RealtimeSseHub` the SSE
and WebSocket routes share. Labelled `forze.realtime.channel`.

| Metric | Instrument | Unit | Notes |
|---|---|---|---|
| `forze.realtime.hub.subscribers` | observable up-down counter | 1 | SSE and WebSocket streams subscribed to the live tail on this node |
| `forze.realtime.hub.dropped` | observable counter | 1 | live signals dropped on a full subscriber queue; each is a lost live frame |

These are not folded into `forze.realtime.connections`, which only Socket.IO feeds: the
FastAPI WebSocket route reports to the hub alone, and no Socket.IO connection subscribes to
it. The two never overlap, so their sum is the node's live realtime connections. A
catch-up-only SSE stream or a replay-only WebSocket never joins the live tail, so it is in
neither. **What to watch:** a `dropped` rate means some
client reads slower than signals arrive. With a mailbox wired, its durable signals arrive on
its next reconnect; ephemeral ones are gone.

## Realtime mailbox

`instrument_realtime_mailbox(mailbox, cursors)`. Labelled `forze.realtime.channel`.

| Metric | Instrument | Unit | Notes |
|---|---|---|---|
| `forze.realtime.mailbox.stored` | observable counter | 1 | durable principal signals stored for replay |
| `forze.realtime.mailbox.replayed` | observable counter | 1 | entries fetched on connect-time replay |
| `forze.realtime.mailbox.trimmed` | observable counter | 1 | dropped by retention/ack trimming |
| `forze.realtime.mailbox.acked` | observable counter | 1 | per-device cursor advances |
| `forze.realtime.mailbox.overflowed` | observable counter | 1 | replays that lost their oldest backlog to the cap |
| `forze.realtime.mailbox.replay_size` | histogram | 1 | per connect, the entries past the device's acked cursor that its replay read; recorded only when the replay finishes; the SDK's default buckets cover the default `cap` of 1000 |

**The alert that matters:** every `overflowed` increment is a device that fell more than
`cap` entries behind and lost signals it will never see. `replay_size` warns earlier: a
tail that keeps growing means devices fall behind on acks. It is a trend, not the backlog's
size: no gauge counts mailbox rows, since that takes a store query a scrape must not run.

`replay_size` counts the unacked backlog, not the frames sent. A resume with a last-seen id
(SSE `Last-Event-ID`, WebSocket `last_event_id`) skips the entries delivered before the
tear, and they still count. A replay the client leaves, or the store fails, records nothing.
The counters and the replay hook live on the mailbox passed to `instrument_realtime_mailbox`:
instrument the instance your `mailbox_factory` returns. A factory that builds a new mailbox
per connect records nothing, and neither does the mailbox the gateway or
`RealtimeMailboxWriter` builds inside its own step.

## Access-token signing

`instrument_signing({"default": service, ...})` — the identity plane. Labelled
`forze.signer`, `forze.signer.algorithm`, and `forze.signer.kid` when set.

| Metric | Instrument | Unit | Notes |
|---|---|---|---|
| `forze.authn.tokens.signed` | observable counter | 1 | access tokens issued; for a KMS-held key this tracks sign round-trips |
| `forze.authn.tokens.verified` | observable counter | 1 | verified successfully |
| `forze.authn.tokens.verify_failed` | observable counter | 1 | rejected as expired/invalid |

A rising `verify_failed` rate is the signal that matters: key-rotation gaps, clock skew,
or forgeries.

## Revocation states

`instrument_revocation_states({"default": revocation_states})` — the identity plane's fast
path for sessions and API keys. Labelled `forze.revocation_states` and `forze.credential`
(`session` or `api_key`).

| Metric | Instrument | Unit | Notes |
|---|---|---|---|
| `forze.authn.revocation_states.hits` | observable counter | 1 | verifications answered from Redis alone, refusals included |
| `forze.authn.revocation_states.misses` | observable counter | 1 | verifications that read the database for a state Redis lacked or could not trust |
| `forze.authn.revocation_states.fallbacks` | observable counter | 1 | verifications whose Redis read failed, timed out or met an open breaker |
| `forze.authn.revocation_states.write_failures` | observable counter | 1 | revocations whose write-through failed every attempt |
| `forze.authn.revocation_states.clock_skew` | observable counter | 1 | API-key states refused because the application's clock runs two `ttl` or more ahead of Redis's |

**The alert that matters:** every `write_failures` increment is a revoked credential that
stays admitted for up to the states' `ttl`. See
[Authentication](../integrations/authn.md#the-window) for every cause of that window.

## Grants invalidation

`instrument_grants_invalidation({"default": grants_invalidation})` — the broadcast that keeps
every process's grants cache current. Labelled `forze.grants_invalidation`.

| Metric | Instrument | Unit | Notes |
|---|---|---|---|
| `forze.authz.grants_invalidation.publish_failures` | observable counter | 1 | committed invalidations whose marker write failed |
| `forze.authz.grants_invalidation.flushes` | observable counter | 1 | whole grants caches dropped on a gap in the stream or a catalog change |
| `forze.authz.grants_invalidation.bypasses` | observable counter | 1 | effective-grants reads (decisions, scoping, grant queries) that skipped the cache because no subscription kept it current |

Every `publish_failures` increment leaves other processes on the old grants for up to the
cache's TTL. A steady `flushes` rate is a flapping connection: the cache stays correct and stops
helping. A rising `bypasses` count in a process that should be subscribed means the lifecycle
step is not registered there, or its stream has ended. See [Identity](../identity-tenancy-enc/identity.md#reaching-every-process).

## Worker snapshots

Rendered by `attach_metrics_route(..., snapshots=...)` itself, not by an `instrument_*` call:
one sample per worker the scrape serves, labelled `service_instance_id`. Prometheus sees it as
`forze_metrics_snapshot_age_seconds`. See [Several workers, one scrape](../running-in-prod/grafana-stack.md#several-workers-one-scrape).

| Metric | Instrument | Unit | Notes |
|---|---|---|---|
| `forze.metrics.snapshot.age` | gauge | s | seconds since that worker wrote the snapshot served; never above the `ttl` |

It describes a worker's whole snapshot, not each series in it. `count()` of it is the number of
workers served: alert when it falls below the worker count, because `up` stays 1 while a
worker is missing.
