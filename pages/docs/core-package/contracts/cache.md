# Cache contracts

Cache contracts provide a small key/value abstraction for read-through document
caches, versioned cache entries, and application-level cached values.

## `CacheSpec`

| Section | Details |
|---------|---------|
| Purpose | Names a cache namespace and its default time-to-live settings. |
| Import path | `from forze.application.contracts.cache import CacheSpec` |
| Type parameters | None. |
| Required fields | `name`; `ttl` and `ttl_pointer` have defaults. |
| Returned values | Passed to `ctx.cache(spec)` to resolve a `CachePort`. |
| Common implementations | Mock cache adapter, Redis / Valkey cache adapter. |
| Related dependency keys | `CacheDepKey`. |
| Minimal example | `cache_spec = CacheSpec(name="projects", ttl=timedelta(minutes=10))` |
| Related pages | [Contracts overview](../contracts.md), [Redis / Valkey](../../integrations/redis.md), [Document contracts](document.md). |

| Field | Type | Notes |
|-------|------|-------|
| `name` | `str | StrEnum` | Logical route for the cache backend. |
| `ttl` | `timedelta` | Default TTL for stored values. |
| `ttl_pointer` | `timedelta` | TTL for version pointer keys. |

## `CacheQueryPort`

| Section | Details |
|---------|---------|
| Purpose | Reads cached values. |
| Import path | `from forze.application.contracts.cache.ports import CacheQueryPort` |
| Type parameters | None. |
| Required methods | `get`, `get_many`. |
| Returned values | A cached value or `None`; bulk reads return `(found_mapping, missing_keys)`. |
| Common implementations | Mock, Redis / Valkey. |
| Related dependency keys | `CacheDepKey`. |
| Minimal example | `value = await ctx.cache(cache_spec).get("project:123")` |
| Related pages | [Add Caching](../../recipes/add-caching.md). |

## `CacheCommandPort`

| Section | Details |
|---------|---------|
| Purpose | Writes, version-tags, and deletes cached values. |
| Import path | `from forze.application.contracts.cache.ports import CacheCommandPort` |
| Type parameters | None. |
| Required methods | `set`, `set_many`, `set_versioned`, `set_many_versioned`, `delete`, `delete_many`. |
| Returned values | `None` for writes/deletes. |
| Common implementations | Mock, Redis / Valkey. |
| Related dependency keys | `CacheDepKey`. |
| Minimal example | `await ctx.cache(cache_spec).set("project:123", payload)` |
| Related pages | [Document contracts](document.md). |

## `CachePort`

| Section | Details |
|---------|---------|
| Purpose | Combined read/write cache contract. |
| Import path | `from forze.application.contracts.cache import CachePort` |
| Type parameters | None. |
| Required methods | All methods from `CacheQueryPort` and `CacheCommandPort`. |
| Returned values | Same as the query and command methods. |
| Common implementations | Mock, Redis / Valkey. |
| Related dependency keys | `CacheDepKey`; resolve with `ctx.cache(spec)`. |
| Minimal example | See below. |
| Related pages | [Contracts overview](../contracts.md). |

    :::python
    from datetime import timedelta

    from forze.application.contracts.cache import CacheSpec

    cache_spec = CacheSpec(name="projects", ttl=timedelta(minutes=10))
    cache = ctx.cache(cache_spec)
    await cache.set("project:123", {"name": "Docs"})
    cached = await cache.get("project:123")
