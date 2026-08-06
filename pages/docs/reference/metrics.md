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

- **Unit `1`** means a plain count. `ms` is milliseconds, `s` seconds.
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
| `forze.resilience.bulkhead.limit` | observable gauge | 1 | `forze.policy`, `forze.route` | current AIMD concurrency limit |
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

## Durable execution

`DurableTelemetry.create()`, wired into the durable runner.

| Metric | Instrument | Unit | Labels | Notes |
|---|---|---|---|---|
| `forze.durable.runs` | counter | 1 | `forze.durable.name`, `forze.durable.outcome` | outcome is `completed` / `failed` / `forward_incomplete` / `cancelled` / `timed_out` / `reclaimed` / `interrupted` / `unrecorded` — alert on `failed`, not on the total: `cancelled` means somebody pressed Stop. `unrecorded` describes the *attempt*, not the run: the body finished and its terminal write went unacknowledged, so the row is either already terminal (the write committed, the ack was lost) or still `RUNNING` for recovery — the worker cannot tell. A sustained rate of it means the run store is unreachable. `interrupted` is the neighbouring case where the body never finished at all (drain, shutdown) and no terminal state was written — expected during a deploy |
| `forze.durable.run.duration` | histogram | ms | `forze.durable.name`, `forze.durable.outcome` | ms-ladder buckets installed by `bootstrap_telemetry` |
| `forze.durable.recovered` | counter | 1 | — | runs reclaimed by a recovery sweep |
| `forze.durable.schedule.fires` | counter | 1 | `forze.durable.name` | one per schedule fire |

A `recovered` spike means runs are being reclaimed from processes that died holding them —
normal in small numbers after a deploy, a signal when it is sustained.

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

## Realtime mailbox

`instrument_realtime_mailbox(mailbox, cursors)`. Labelled `forze.realtime.channel`.

| Metric | Instrument | Unit | Notes |
|---|---|---|---|
| `forze.realtime.mailbox.stored` | observable counter | 1 | durable principal signals stored for replay |
| `forze.realtime.mailbox.replayed` | observable counter | 1 | entries fetched on connect-time replay |
| `forze.realtime.mailbox.trimmed` | observable counter | 1 | dropped by retention/ack trimming |
| `forze.realtime.mailbox.acked` | observable counter | 1 | per-device cursor advances |
| `forze.realtime.mailbox.overflowed` | observable counter | 1 | replays that lost their oldest backlog to the cap |

**The alert that matters:** every `overflowed` increment is a device that fell more than
`cap` entries behind and lost signals it will never see.

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
