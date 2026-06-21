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

## Going further

The cache contract layers more on top of plain read-through, each a one-line
addition to the `CacheSpec` — covered in [Caching reads](../data-events/caching.md):

- **Stampede protection** — concurrent misses collapse to one fetch, and hot keys
  can refresh early so they never expire for every replica at once. Both
  automatic (early refresh is one opt-in flag).
- **An in-process L1** — serve the hottest documents from process memory, already
  decoded, on a small staleness budget; switch to W-TinyLFU when scans evict your
  hot set, or add Redis [push invalidation](../integrations/redis.md#l1-push-invalidation)
  to shrink that budget to ~zero.
- **Adaptive lifetimes** — stable documents earn longer TTLs and hot ones stay
  cached while they're read, with no per-document tuning.

## Run it

```bash
cd examples/recipes/cache_reads
just run
```
