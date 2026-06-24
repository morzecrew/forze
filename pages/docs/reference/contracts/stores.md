---
title: Cache, counter & storage
icon: lucide/database
summary: The cache, counter, and object-storage contracts — their specs and methods
---

Three small key-addressed contracts. The **cache** is read-through key/value with TTLs
([Caching](../../data-events/caching.md)); the **counter** is an atomic monotonic
sequence; **object storage** holds blobs by key. The ports-and-adapters model behind them
is in [Contracts](../../core-concepts/contracts.md).

## Cache

`ctx.cache(spec)` returns a combined read/write `CachePort`. Keys are strings
within the spec's namespace. **Values must be JSON-serializable, or
pre-encoded `bytes`** (stored verbatim, returned for the caller to decode) —
don't rely on key ordering or non-JSON types surviving a round trip.

`CacheSpec` — most fields tune document read-through; the deep behavior is in
[Caching](../../data-events/caching.md):

| Field | Type | Default | Meaning |
|-------|------|---------|---------|
| `name` | `str \| StrEnum` | required | namespace for the keys |
| `ttl` | `timedelta` | `300s` | default entry lifetime |
| `ttl_pointer` | `timedelta` | `60s` | TTL for versioned-cache pointers |
| `early_refresh_beta` | `float \| None` | `None` | probabilistic [early refresh](../../data-events/caching.md#stampede-protection) (XFetch) |
| `early_refresh_background` | `bool` | `False` | run elected refreshes detached, off the read path |
| `l1` | `L1Spec \| None` | `None` | opt-in [in-process L1](../../data-events/caching.md) ahead of the backend |
| `sliding_ttl` | `timedelta \| None` | `None` | expire-after-access for versioned entries (capped by `ttl`) |
| `age_ttl` | `AgeBasedTtl \| None` | `None` | age-proportional [per-entry lifetime](../../data-events/caching.md#adaptive-lifetimes) |

| Method | Signature | Notes |
|--------|-----------|-------|
| `get` | `get(key)` | value or `None` on miss |
| `get_many` | `get_many(keys)` | `(found_mapping, missing_keys)` |
| `exists` | `exists(key)` | presence check without payload transfer |
| `set` | `set(key, value, *, ttl=None)` | store |
| `set_many` | `set_many(mapping, *, ttl=None)` | bulk store |
| `set_versioned` | `set_versioned(key, version, value, *, ttl=None)` | store tagged with a version |
| `set_many_versioned` | `set_many_versioned(mapping, *, ttl=None)` | keyed by `(key, version)` |
| `delete` | `delete(key, *, hard)` | `hard` is required |
| `delete_many` | `delete_many(keys, *, hard)` | `hard` is required |

TTLs default from the `CacheSpec`; the per-entry `ttl=` overrides that entry's
lifetime alone (the seam the
[adaptive lifetimes](../../data-events/caching.md#adaptive-lifetimes)
write through). See
[Cache reads with Redis](../../recipes/cache-reads-with-redis.md).

## Counter

`ctx.counter(spec)` returns a `CounterPort` — an atomic, monotonic sequence within
the spec's namespace (`CounterSpec` carries only a name). `suffix` partitions counters
under one spec.

| Method | Signature | Notes |
|--------|-----------|-------|
| `incr` | `incr(by=1, *, suffix=None)` | increment, return the new value |
| `incr_batch` | `incr_batch(size=2, *, suffix=None)` | allocate `size` sequential values at once |
| `decr` | `decr(by=1, *, suffix=None)` | decrement, return the new value |
| `reset` | `reset(value=1, *, suffix=None)` | reset, return `value` |

## Storage

Object storage splits into query and command ports —
`ctx.storage.query(spec)` / `ctx.storage.command(spec)` (`StorageSpec` carries only a
name). Objects are addressed by string `key`.

| Port | Method | Signature | Notes |
|------|--------|-----------|-------|
| query | `download` | `download(key)` | returns a `DownloadedObject` (bytes + metadata) |
| query | `list` | `list(limit, offset, *, prefix=None)` | `(objects, total_count)` |
| command | `upload` | `upload(obj)` | takes an `UploadedObject`, returns `StoredObject` metadata |
| command | `delete` | `delete(key)` | remove by key |

The core port has no presigned-URL method — that's backend-specific.

## Implemented by

| Contract | Backend | Integration |
|----------|---------|-------------|
| Cache | Redis | [Redis](../../integrations/redis.md) |
| Counter | Redis | [Redis](../../integrations/redis.md) |
| Object storage | S3, GCS | [S3](../../integrations/s3.md) · [GCS](../../integrations/gcs.md) |

The in-memory mock implements all three for tests.
