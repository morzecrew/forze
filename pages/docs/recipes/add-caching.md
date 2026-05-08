# Add caching

Use this recipe when document reads are stable enough to cache and you want adapter-level read-through behavior.

## Ingredients

- `CacheSpec` on the document spec
- A cache integration such as [Redis / Valkey](../integrations/redis.md)
- Matching logical names in specs and integration configs

## Steps

1. Add a `CacheSpec` to the `DocumentSpec`.
2. Register the same cache name in `RedisDepsModule.caches`.
3. Ensure the document adapter can resolve the cache route while building query and command ports.
4. Keep cache invalidation in the adapter/coordinator layer instead of usecases when possible.

## Minimal shape

    :::python
    from datetime import timedelta

    from forze.application.contracts.cache import CacheSpec


    cache=CacheSpec(name="projects", ttl=timedelta(minutes=5))

## Learn more

See [Specs and infrastructure wiring](../concepts/specs-and-wiring.md) for route naming and [Redis / Valkey Integration](../integrations/redis.md) for key behavior.
