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

The scenario builds the same typed `DocumentFacade` used by the CRUD examples;
only the dependency wiring adds caching:

```python
--8<-- "recipes/cache_reads/app.py:read-through"
```

- The first `get` misses, loads from Postgres, and populates Redis.
- Repeat `get`s are served from Redis.
- An `update` invalidates the entry; the next `get` repopulates it with the new
  value — so a cached read is never stale.

## Going further

Read-through is the floor. The cache contract layers stampede protection with
background early refresh (serve the still-valid entry, refresh off the request
path), an in-process L1 (with W-TinyLFU admission and Redis push invalidation),
and adaptive TTLs on top — each a one-line addition to the `CacheSpec`, covered
in [Caching reads](../data-events/caching.md).

## Run it

```bash
cd examples/recipes/cache_reads
just run
```
