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

There is **no read verb**, deliberately: a counter's value is only meaningful at the
instant it is allocated, so a handler that read one would be holding a number another
allocation has already moved past. Allocate and use the value you were given.

### Enumerating counters (admin)

`ctx.counter.admin(spec)` returns a `CounterAdminPort`. It exists because the missing read
verb, correct as it is for handlers, left counters as the one plane that is **durable state
nothing can read** — so a migration that faithfully rebuilt every other plane would restart
the counters at zero and reissue invoice numbers already in customers' hands, with no error
anywhere to say so.

| Method | Signature | Notes |
|--------|-----------|-------|
| `list_counters` | `list_counters()` | every counter under the spec, one `CounterEntry(suffix, value)` per partition |

Both properties hold at once: handlers still cannot read a counter (the admin port is a
separate acquisition, and it cannot allocate), while operators and a portable export can.
`suffix=None` is a real partition — the unsuffixed counter — not an absent one.

Carrying counters to another deployment needs no new write verb; `reset` is already it:

```python
for entry in await source.counter.admin(INVOICES).list_counters():
    await target.counter(INVOICES).reset(entry.value, suffix=entry.suffix)
```

The value is the last number **handed out**, so the target's next `incr` continues the
sequence rather than repeating its final number. Enumerate a counter you intend to carry only
when nothing is allocating from it (a stopped fleet) — a value read while the source is live
is stale the moment it is read. The whole set is returned at once, so a spec partitioned into
millions of suffixes is not yet in scope.

## Storage

Object storage splits into query and command ports —
`ctx.storage.query(spec)` / `ctx.storage.command(spec)` (`StorageSpec` carries only a
name). Objects are addressed by string `key`.

| Port | Method | Signature | Notes |
|------|--------|-----------|-------|
| query | `download` | `download(key)` | the whole object, buffered — a `DownloadedObject` (bytes + metadata) |
| query | `download_stream` | `download_stream(key)` | a `StreamedDownload` — single-use async chunk iterator, never buffers the whole object; encrypted objects decrypt chunk by chunk |
| query | `download_range` | `download_range(key, *, start, end=None)` | inclusive byte range as a `RangedDownload` (HTTP `Range` semantics); a `start` beyond the object is a precondition error (the 416 equivalent) |
| query | `download_if_changed` | `download_if_changed(key, *, if_none_match=None, if_modified_since=None)` | `None` when unchanged (the 304 equivalent); at least one condition required |
| query | `head` | `head(key, *, include_tags=False)` | an `ObjectHead` — size, content type, ETag, last-modified — without the body; works for raw presigned uploads too |
| query | `presign_download` | `presign_download(key, *, expires_in)` | time-limited direct-`GET` URL — a bearer credential, never log it |
| query | `list` | `list(limit, offset, *, prefix=None, include_tags=False)` | `(objects, total_count)` |
| command | `upload` | `upload(obj)` | takes an `UploadedObject`, returns `StoredObject` metadata |
| command | `upload_stream` | `upload_stream(chunks, *, filename, ...)` | bounded-memory multipart upload from an async chunk iterator; an encrypting route seals it chunk by chunk |
| command | `overwrite_stream` | `overwrite_stream(key, chunks, *, ...)` | replace an existing key in place, in bounded memory (the re-encryption seam) |
| command | `presign_upload` | `presign_upload(key, *, expires_in, content_type=None)` | time-limited direct-`PUT` URL — a **write grant**, so it lives on the command port |
| command | `delete` | `delete(key)` | remove by key |
| command | `copy` / `move` | `copy(src_key, dst_key)` | server-side, same-bucket; `move` is copy-then-delete (non-atomic) |
| command | `put_object_tags` | `put_object_tags(key, tags)` | full tag replacement |

A third port, `StorageUploadSessionPort`, drives resumable multipart uploads
(begin / presign part / list parts / complete / abort) — its HTTP projection is
on the [FastAPI route generators](../fastapi-routes.md#direct-resumable-uploads)
page. Ranged reads work over chunked-AEAD encrypted objects (only the covering
chunks are fetched and decrypted); a legacy whole-payload envelope cannot be
sliced and refuses with `core.storage.range_whole_payload_unsupported`.

## Implemented by

| Contract | Backend | Integration |
|----------|---------|-------------|
| Cache | Redis | [Redis](../../integrations/redis.md) |
| Counter | Redis | [Redis](../../integrations/redis.md) |
| Object storage | S3, GCS | [S3](../../integrations/s3.md) · [GCS](../../integrations/gcs.md) |

The in-memory mock implements all three for tests.
