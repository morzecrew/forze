---
title: Add idempotency
icon: lucide/copy-check
summary: Make a retried mutation a no-op that returns the first result, keyed by a header
---

A client retries a `POST` it isn't sure landed; an at-least-once queue delivers
the same command twice. **Idempotency** makes the duplicate a no-op that returns
the *first* result — the handler and its writes run exactly once. The concept is
covered in [Idempotency](../in-depth/idempotency.md); this is the wiring.

The runnable version lives at `examples/recipes/idempotency/` and runs on the
in-memory mock store — no infrastructure needed.

## Wrap the operation

Idempotency is a wrap on the operation registry. Bind the operation you want
deduped and add an `IdempotencyWrap` — it sits **outermost**, so a replay skips
the handler and its transaction, while `before` hooks (authn/authz) still run:

```python
--8<-- "recipes/idempotency/app.py:wrap"
```

`IdempotencySpec.name` is the adapter route and `result_type` must be a Pydantic
model (the stored result is encoded and decoded on replay).

## Bind the key and call it

Build the `DocumentFacade` over the wrapped registry. The wrap fires whenever an
idempotency key is bound — passing a key, replaying the call returns the stored
result:

```python
--8<-- "recipes/idempotency/app.py:scenario"
```

Over HTTP you don't bind by hand: the
[`InvocationMetadataMiddleware`](../integrations/fastapi.md) reads the
`Idempotency-Key` header and binds it around the request, so the route just calls
`facade.create(cmd)`.

## Register a store

The mock module auto-registers an idempotency adapter, so the example needs no
config. For production, register one — commonly Redis:

```python
RedisDepsModule(
    client=redis,
    idempotency={"orders": RedisIdempotencyConfig(namespace="orders")},
)
```

The route key (`"orders"`) matches `IdempotencySpec.name`; the TTL comes from the
spec, not the config.

## Notes

- **Same key, different payload → `conflict`.** A key can't be reused for a
  different request; the store rejects a payload-hash mismatch.
- **Stable keys, generous TTL.** The key must be the same across a client's
  retries, and the TTL must outlast them. Every worker that can handle the
  operation must share the same store namespace.
- **Not transactional with the write.** The wrap records the result *after* the
  business transaction commits — a crash in the gap leaves the operation
  un-recorded, so it re-runs rather than replays. Idempotency dedupes the common
  case; it isn't a substitute for the [outbox](../in-depth/events-sagas.md) when
  you need exactly-once *effects*.
