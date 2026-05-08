# Idempotency contracts

Idempotency contracts let HTTP-style boundaries replay a stored response when a
caller repeats the same operation, idempotency key, and normalized payload hash.

## `IdempotencySpec`

| Section | Details |
|---------|---------|
| Purpose | Names an idempotency backend route and configures snapshot TTL. |
| Import path | `from forze.application.contracts.idempotency import IdempotencySpec` |
| Type parameters | None. |
| Required fields | `name`; `ttl` defaults to 30 seconds. |
| Returned values | Passed to the idempotency dependency factory. |
| Common implementations | Mock idempotency adapter, Redis / Valkey idempotency adapter. |
| Related dependency keys | `IdempotencyDepKey`. |
| Minimal example | `spec = IdempotencySpec(name="http", ttl=timedelta(minutes=5))` |
| Related pages | [Add Idempotency](../../recipes/add-idempotency.md), [FastAPI](../../integrations/fastapi.md). |

## `IdempotencyPort`

| Section | Details |
|---------|---------|
| Purpose | Begins idempotent operations and commits serialized response snapshots. |
| Import path | `from forze.application.contracts.idempotency import IdempotencyPort` |
| Type parameters | None. |
| Required methods | `begin`, `commit`. |
| Returned values | `begin` returns `IdempotencySnapshot | None`; `commit` returns `None`. |
| Common implementations | Mock, Redis / Valkey, FastAPI idempotency feature consumers. |
| Related dependency keys | `IdempotencyDepKey`. |
| Minimal example | See below. |
| Related pages | [Contracts overview](../contracts.md), [Redis / Valkey](../../integrations/redis.md). |

Required methods:

| Method | Parameters | Returns |
|--------|------------|---------|
| `begin` | `op`, `key`, `payload_hash` | Stored `IdempotencySnapshot` or `None`. |
| `commit` | `op`, `key`, `payload_hash`, `snapshot` | `None`. |

## `IdempotencySnapshot`

| Section | Details |
|---------|---------|
| Purpose | Stores a serialized response for replay. |
| Import path | `from forze.application.contracts.idempotency import IdempotencySnapshot` |
| Type parameters | None. |
| Required fields | `code`, `content_type`, `body`; optional `headers`. |
| Returned values | Returned by `begin` and accepted by `commit`. |
| Common implementations | attrs value object stored by idempotency adapters. |
| Related dependency keys | Produced through `IdempotencyDepKey` implementations. |
| Minimal example | `IdempotencySnapshot(code=201, content_type="application/json", body=b"{}")` |
| Related pages | [FastAPI integration](../../integrations/fastapi.md). |

    :::python
    from datetime import timedelta

    from forze.application.contracts.idempotency import (
        IdempotencyDepKey,
        IdempotencySnapshot,
        IdempotencySpec,
    )

    spec = IdempotencySpec(name="http", ttl=timedelta(minutes=5))
    idem = ctx.dep(IdempotencyDepKey)(ctx, spec)
    cached = await idem.begin("create-project", key, payload_hash)
    if cached is None:
        await idem.commit(
            "create-project",
            key,
            payload_hash,
            IdempotencySnapshot(
                code=201,
                content_type="application/json",
                body=b'{"ok": true}',
            ),
        )
