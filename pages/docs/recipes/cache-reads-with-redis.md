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
the default (`None`) keeps the payload format unchanged.

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

## Run it

```bash
cd examples/recipes/cache_reads
just run
```
