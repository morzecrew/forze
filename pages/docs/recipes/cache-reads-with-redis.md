---
title: Cache reads with Redis
icon: lucide/zap
summary: Serve repeat document reads from Redis and invalidate on writes — handlers untouched
---

Reading the same document over and over shouldn't hit the database every time.
Attach a cache to its specification and wire a Redis backend: reads serve from
Redis, writes invalidate. The handlers don't change — caching is pure wiring.

The runnable version of this recipe lives at `examples/recipes/cache_reads/` —
`just run` brings up ephemeral Postgres + Redis, runs it, and tears it down.

## Cache the aggregate

The `Product` is an ordinary document. The only caching-related line is the
`cache=` on its specification:

```python
--8<-- "recipes/cache_reads/app.py:domain"
```

```python
--8<-- "recipes/cache_reads/app.py:spec"
```

`CacheSpec(name="products")` is the whole opt-in; its TTLs default sensibly.

## Wire Postgres + Redis

Reads cache only once a cache backend is registered for the spec's
`CacheSpec.name`. Register the Redis cache next to the Postgres document module —
the `"products"` key is the same logical name on both sides:

```python
--8<-- "recipes/cache_reads/app.py:wiring"
```

Postgres stores the documents; Redis answers the repeat reads.

## What happens on read and write

```python
--8<-- "recipes/cache_reads/app.py:read-through"
```

- The first `get` misses, loads from Postgres, and populates Redis.
- Repeat `get`s are served from Redis.
- An `update` invalidates the entry; the next `get` repopulates it with the new
  value — so a cached read is never stale.

## Stampedes are handled

Two protections come with read-through caching; you wire neither.

**Concurrent misses collapse.** When many requests miss the same key at once
(a cold start, an invalidation on a hot key), one of them fetches from the
database and the rest wait for that result — per process, one gateway fetch
instead of a thundering herd. Followers share the leader's failure too, so a
broken fetch fails fast everywhere instead of retrying in a pile.

**Hot keys can refresh early.** Even with collapsed misses, a popular entry
expiring means every replica misses at the same moment. Opt in with
`CacheSpec(name="products", early_refresh_beta=1.0)`: a cache hit close to
expiry may volunteer to recompute *before* the entry dies, with probability
scaled by how expensive the recompute was observed to be — so refreshes
desynchronize across replicas and a hot key never expires for everyone at
once. Enabled entries carry a small metadata envelope in the cached value;
the default (`None`) keeps the payload format unchanged. By default the
elected reader waits for the recompute; add
`early_refresh_background=True` and it serves the still-valid cached entry
immediately while the refresh runs detached — the reader never pays the
recompute latency, and a failed refresh is logged rather than surfaced (the
entry is still valid; a later election retries).

## An in-process L1 for hot documents

When one document is read constantly, even a Redis hit costs a network
round-trip plus a JSON decode — per request, per replica. An opt-in **L1**
serves those reads from process memory instead, already decoded:

```python
from forze.application.contracts.cache import CacheSpec, L1Spec

cache = CacheSpec(
    name="products",
    l1=L1Spec(ttl=timedelta(seconds=2), capacity=1024),
)
```

!!! warning "The L1 TTL is a staleness budget"

    This changes the consistency contract you opted into above. A write
    invalidates the L1 only on the replica that performed it — **other
    replicas may serve their L1 entry for up to `ttl` after a write**. Keep
    the TTL small (it must be below the cache TTL), and enable L1 only on
    read models that tolerate reads that stale. On the writing replica,
    read-your-writes still holds: local writes refresh the local L1.

Within the budget, the behavior is what you'd hope for: repeat reads of a hot
product never leave the process, the entry pool is LRU-bounded at `capacity`,
and expired entries fall back to Redis (which keeps the early-refresh
machinery above fully functional — set the L1 TTL well below the cache TTL).

The eviction policy is pluggable. If batch jobs or listing traffic sweep many
one-off documents through the L1 and evict your hot set (the signature:
sustained evictions at full capacity with a sagging hit rate in `L1Stats`),
switch to the in-box **W-TinyLFU** store — frequency-based admission rejects
one-pass traffic outright, so scans can't displace hot documents:

```python
from forze.application.integrations.document import tiny_lfu_l1_store

L1Spec(ttl=timedelta(seconds=2), store_factory=tiny_lfu_l1_store)
```

In simulation it holds an ~18-point hit-rate lead over LRU under scan
pressure, at the cost of a couple of microseconds per access — noise next to
the round-trip each extra hit avoids.

### Push invalidation: shrink the staleness window to ~zero

On Redis 6+, the staleness budget above can become a *backstop* instead of
the contract. Opt in on the Redis side:

```python
RedisCacheConfig(namespace="app:products", invalidation_push=True)
```

This turns on Redis **client-side caching** (`CLIENT TRACKING`): the server
pushes an invalidation to every replica the moment *any* replica writes a
cached product — the L1 entry drops within a network round-trip instead of
waiting out its TTL. With push on, you can comfortably raise the L1 TTL
(e.g. to 30–60 s) for a better hit rate.

The failure posture is fail-open: if the push stream drops, every L1 flushes
(events may have been missed), reconnects with backoff, and in the meantime
the TTL bounds staleness exactly as before. Two setups stay TTL-only by
design: tenant-*routed* clients (a tracking stream bound to one tenant's
Redis would miss every other tenant's writes) and dynamic per-tenant
namespaces (no stable broadcast prefix) — both log and degrade gracefully.

## Adaptive lifetimes

A fixed TTL is a compromise: tight enough for the documents that churn,
wasteful for the ones that don't. Two opt-ins adapt it per entry — both
freshness-safe, because in-band writes invalidate regardless of TTL; these
only govern the revalidation cadence and the out-of-band safety net.

**Stable documents earn longer lifetimes** (the HTTP heuristic-freshness
rule — RFC 7234's "10% of age"):

```python
CacheSpec(
    name="products",
    age_ttl=AgeBasedTtl(alpha=0.1, min_ttl=timedelta(seconds=30), max_ttl=timedelta(hours=1)),
)
```

At warm time the entry's lifetime becomes `alpha ×` the document's age since
`last_update_at`, clamped: a product untouched for a day caches for an hour;
one edited a minute ago revalidates within seconds — and resets to cautious
the moment it's written again. No state, no tuning per document.

**Hot documents stay cached while they're hot** (sliding expiration):

```python
CacheSpec(name="products", ttl=timedelta(hours=1), sliding_ttl=timedelta(seconds=60))
```

Each cache hit extends the entry's life to the sliding window, so a
frequently-read document never expires mid-heat — and one quiet window after
its last read, it's gone. Seasonality and time-of-day patterns need no
prediction: the entry simply lives while in season. `ttl` remains the
**absolute cap** — even a perpetually-hot entry revalidates against the
source within that bound.

The two compose: the age heuristic sets each entry's initial lifetime and
cap, sliding keeps it alive under access within that cap.

## Run it

```bash
cd examples/recipes/cache_reads
just run
```
