---
title: Resilience tuning
icon: lucide/sliders
summary: Every resilience strategy's parameters, defaults, and the algorithm behind the adaptive controllers
---

The narrative is in [Resilience](../running-in-prod/resilience.md); this is the
exhaustive surface — each strategy's constructor parameters and defaults, and the
algorithm behind the adaptive controllers. A `ResiliencePolicy` composes these
outer→inner: rate limit → bulkhead → circuit breaker → retry → timeout, plus
optional fallback and hedge. The policy itself carries one more knob —
`fail_open_on_store_error` (default `True`): when a shared breaker / rate-limit
store errors, admit the call instead of failing it.

## Rate limit

`RateLimitStrategy` — token bucket, sustained `permits/per`, capacity `burst or
permits`. The bucket starts full and refills continuously; an empty bucket rejects
immediately (no queuing) with `exc.throttled(code="rate_limited")`. State is keyed
`(policy, route)`.

| Param | Default | Meaning |
|-------|---------|---------|
| `permits` | — | tokens issued per `per` window (`>= 1`) |
| `per` | — | refill window |
| `burst` | `permits` | bucket capacity (max tokens saved up) |

## Retry & backoff

`RetryStrategy` re-runs on classified-**retryable** kinds only (`concurrency`,
`infrastructure`, `throttled`); a non-retryable kind in `retry_on` fails at wiring.

| Param | Default | Meaning |
|-------|---------|---------|
| `max_attempts` | — | total attempts including the first (`>= 1`) |
| `backoff` | — | a `BackoffStrategy` (`base`, `max`, `multiplier`, `jitter`) |
| `retry_on` | — | the `ExceptionKind`s that trigger a retry (all must be retryable) |
| `budget` | `None` | optional `RetryBudget` token-bucket cap on retries across calls |

## Timeout

`TimeoutStrategy(timeout=…)` — a per-attempt timeout; cancellation on expiry.

## Circuit breaker

`CircuitBreakerStrategy` — rolling-window breaker; trips open when the failure
fraction exceeds `failure_ratio` over `sampling_window` (once `min_throughput` calls
have been seen), then probes half-open after `break_duration`.

| Param | Default | Meaning |
|-------|---------|---------|
| `failure_ratio` | — | failure fraction in the window that trips open (`(0, 1]`) |
| `sampling_window` | — | rolling window outcomes are counted over |
| `min_throughput` | — | minimum calls in the window before it may trip |
| `break_duration` | — | how long it stays open before probing half-open |
| `half_open_max_calls` | `1` | probe calls allowed while half-open |

## Bulkheads

Cap concurrent in-flight calls. The three kinds are **mutually exclusive within one
policy**; all share the queue-management controls below.

### Fixed — `BulkheadStrategy`

| Param | Default | Meaning |
|-------|---------|---------|
| `max_concurrency` | — | max concurrent in-flight calls |
| `max_queue` | `0` | calls allowed to wait for a slot before rejection |

### Adaptive (AIMD) — `AdaptiveBulkheadStrategy`

Latency sets the limit. Starts at `max_concurrency`; a completion over
`latency_threshold` decreases it multiplicatively (`*= backoff_ratio`, at most once
per `cooldown`), in-budget completions recover it additively (`+= increase_step /
limit` — ~one slot per `limit` successes). This is **AIMD** (the TCP-congestion
algorithm): N process-local limits sharing one downstream converge like N TCP flows
sharing a link, so uncoordinated replicas need no shared state. The congestion signal
is **latency only** — errors stay the breaker's job — and shrinking never evicts
in-flight work, only gates admission.

| Param | Default | Meaning |
|-------|---------|---------|
| `latency_threshold` | — | completion latency above this counts as congestion |
| `max_concurrency` | — | ceiling and initial limit |
| `min_concurrency` | `1` | floor the limit never drops below |
| `max_queue` | `0` | calls allowed to wait for a slot |
| `backoff_ratio` | `0.9` | multiplicative decrease on a breach (`(0, 1)`) |
| `increase_step` | `1.0` | additive recovery: `increase_step / limit` per in-budget completion |
| `cooldown` | `1 s` | minimum spacing between decreases (coalesces a slow burst into one backoff) |
| `latency_quantile` | `None` | distributional signal — see below |

**`latency_quantile` (the p-quantile signal).** By default *any single* completion
over the threshold is a breach — instant, but one GC pause halves concurrency. Set
`latency_quantile=0.95` to breach only when the observed p95 of recent completions (a
windowed streaming **P²** estimate — five floats, no sample storage) exceeds the
threshold: outliers can't move a quantile, only a genuinely shifted distribution can.
A backoff opens a fresh measurement epoch, so a stale-high quantile never ratchets the
limit to the floor after recovery. In a fleet, the per-replica sketch can be shared
via `latency_digest_store` (see [Resilience → fleet-wide state](../running-in-prod/resilience.md#fleet-wide-state)).

### Gradient — `GradientBulkheadStrategy`

Drops the threshold entirely. The Gradient2 controller (Netflix `concurrency-limits`)
learns the no-load baseline RTT and tracks the gradient between it and recent latency,
finding the load/latency knee on its own — **no `latency_threshold` to tune**. Probes
up gently while latency sits near baseline, contracts as latency inflates. Only
*successful* completions feed it.

| Param | Default | Meaning |
|-------|---------|---------|
| `max_concurrency` | — | ceiling and initial limit |
| `min_concurrency` | `1` | floor |
| `max_queue` | `0` | calls allowed to wait for a slot |
| `rtt_tolerance` | `1.5` | latency-rise headroom before contracting (1.5 = tolerate a 50% rise) |
| `smoothing` | `0.2` | EWMA factor on limit *increases* (gentle up, fast down) |
| `long_window` | `600` | samples the no-load baseline RTT is averaged over |
| `headroom` | `4.0` | standing in-flight headroom probed toward while healthy |

### Queue management (every bulkhead kind)

Opt-in controls for a queued bulkhead (`max_queue >= 1`); the first two are from
Facebook's *Fail at Scale*, the last is Netflix-style prioritized shedding.

| Param | Default | Meaning |
|-------|---------|---------|
| `queue_target` | `None` | **CoDel** target sojourn — under sustained congestion, a waiter parked longer than this is shed at dequeue (`code="bulkhead_queue_shed"`), bounding queueing by *time*, not length |
| `queue_interval` | `100 ms` | CoDel congestion-detection window + the generous sojourn allowance while the queue has recently been empty (must exceed `queue_target`) |
| `queue_adaptive_lifo` | `False` | serve the *newest* waiter first while congested (its client is likeliest still listening); FIFO otherwise — pair with `queue_target` to shed the starved tail |
| `prioritized` | `False` | criticality-aware: a full queue admits a higher-criticality arrival by shedding the lowest, and lower tiers shed sooner. Tiers come from the task-scoped `Criticality` (`BEST_EFFORT < DEGRADED < NORMAL < CRITICAL`), bound with `bind_criticality(...)` |

A parked waiter whose [invocation deadline](../running-in-prod/deadlines.md) has
expired is failed at wake rather than granted a slot it can't use — no knob needed.

## Adaptive throttle

`AdaptiveThrottleStrategy` — probabilistic client-side shedding for a degraded
downstream (Google SRE book). Where the breaker is binary, this sheds
**proportionally**. Tracks `requests` and `accepts` per window and rejects locally
with probability:

```
max(0, (requests − k·accepts) / (requests + 1))
```

A healthy downstream (`accepts ≈ requests`) computes a negative number — nothing is
shed. As the accept ratio degrades, shedding rises proportionally; the steady state is
self-limiting because shed calls count as requests but not accepts, so the client
converges on ~`k ×` the downstream's current capacity, leaving a continuous probe that
detects recovery on its own (no half-open ceremony). Shed calls fail with a retryable
`throttled` (`code="adaptive_throttle"`). **Mutually exclusive with the circuit breaker**
in one policy. "Accepted" mirrors the breaker's classification inverted — a
non-retryable (domain) failure is the downstream doing its job, so it never sheds.

| Param | Default | Meaning |
|-------|---------|---------|
| `k` | `2.0` | permissiveness multiplier (`>= 1`); higher tolerates more failure before shedding |
| `window` | `2 min` | counting window; counters reset when it elapses, so shedding decays within one window of recovery |
| `min_throughput` | `10` | requests per window below which nothing is shed |

## Hedge

`HedgeStrategy` — race the tail: if the primary hasn't completed after `delay`, fire a
concurrent copy and take the first to finish (losers cancelled). Only safe on
idempotent reads; `budget` caps the extra load. Applied outside the strategy pipeline.

| Param | Default | Meaning |
|-------|---------|---------|
| `delay` | — | wait before the next concurrent copy (~p95); the fallback until the estimator warms when `adaptive_delay_quantile` is set |
| `max_attempts` | — | total concurrent attempts including the primary (`>= 2`) |
| `budget` | `None` | optional `RetryBudget` cap on extra attempts |
| `adaptive_delay_quantile` | `None` | hedge after the *observed* quantile of primary latencies (streaming P² per `(policy, route)`) instead of the fixed `delay` — *The Tail at Scale*; typical `0.95` |
| `delay_min` | `None` | floor for the adaptive delay (guards over-eager hedging when every call is fast); requires `adaptive_delay_quantile` |
| `delay_max` | `None` | cap for the adaptive delay (guards a degraded downstream dragging the trigger past usefulness); requires `adaptive_delay_quantile` |

The effective adaptive delay is the `forze.resilience.hedge.delay` gauge once
[`instrument_resilience`](../running-in-prod/observability.md#resilience-metrics) is
attached.

## Control plane

`ResilienceAdminPort` (`ctx.resilience.admin()`, `ResilienceAdminDepKey`) inspects and
retunes live policy state without a redeploy:

| Method | Notes |
|--------|-------|
| `inspect(*, policy=None)` | `ResilienceStateSnapshot`s — per `(policy, route)`: `forced_open`, the adaptive `concurrency_limit`, `in_use`, `waiting`, and the effective `hedge_delay` |
| `force_open(policy, route=None)` | manual kill-switch — trip the breaker open (`route=None` = every route) |
| `clear_forced_open(policy, route=None)` | lift a manual trip |
| `retune(policy)` | hot-swap a `ResiliencePolicy` by name; drops that policy's cached adaptive state |
