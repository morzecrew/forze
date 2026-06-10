---
title: Cache, counter & storage ports
icon: lucide/database
summary: Methods on the cache, counter, and object-storage contracts
---

## Cache

`ctx.cache(spec)` returns a combined read/write `CachePort`. Values are arbitrary;
keys are strings within the spec's namespace.

| Method | Signature | Notes |
|--------|-----------|-------|
| `get` | `get(key)` | value or `None` on miss |
| `get_many` | `get_many(keys)` | `(found_mapping, missing_keys)` |
| `set` | `set(key, value)` | store |
| `set_many` | `set_many(mapping)` | bulk store |
| `set_versioned` | `set_versioned(key, version, value)` | store tagged with a version |
| `set_many_versioned` | `set_many_versioned(mapping)` | keyed by `(key, version)` |
| `delete` | `delete(key, *, hard)` | `hard` is required |
| `delete_many` | `delete_many(keys, *, hard)` | `hard` is required |

TTLs come from the `CacheSpec`, not these calls. See
[Cache reads with Redis](../../recipes/cache-reads-with-redis.md).

## Counter

`ctx.counter(spec)` returns a `CounterPort` — an atomic, monotonic sequence within
the spec's namespace. `suffix` partitions counters under one spec.

| Method | Signature | Notes |
|--------|-----------|-------|
| `incr` | `incr(by=1, *, suffix=None)` | increment, return the new value |
| `incr_batch` | `incr_batch(size=2, *, suffix=None)` | allocate `size` sequential values at once |
| `decr` | `decr(by=1, *, suffix=None)` | decrement, return the new value |
| `reset` | `reset(value=1, *, suffix=None)` | reset, return `value` |

## Storage

Object storage splits into query and command ports —
`ctx.storage.query(spec)` / `ctx.storage.command(spec)`. Objects are addressed by
string `key`.

| Port | Method | Signature | Notes |
|------|--------|-----------|-------|
| query | `download` | `download(key)` | returns a `DownloadedObject` (bytes + metadata) |
| query | `list` | `list(limit, offset, *, prefix=None)` | `(objects, total_count)` |
| command | `upload` | `upload(obj)` | takes an `UploadedObject`, returns `StoredObject` metadata |
| command | `delete` | `delete(key)` | remove by key |

The core port has no presigned-URL method — that's backend-specific. See the
[S3](../../integrations/s3.md) / [GCS](../../integrations/gcs.md) integrations.
