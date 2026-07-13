---
title: Idempotency & locks
icon: lucide/lock-keyhole
summary: The idempotency and distributed-lock contracts — exactly-once effects and cross-process mutual exclusion
---

Two coordination primitives. **Idempotency** makes a retried operation a no-op that returns
the first result; a **distributed lock** gives mutual exclusion across processes. Both are
small, TTL-bounded specs.

## Idempotency

`ctx.idempotency(spec)` returns an `IdempotencyPort`. In practice the engine applies it as
the outermost wrap on a keyed operation — you rarely call the port directly; see
[Idempotency](../../writing-operation/idempotency.md) and the
[recipe](../../recipes/add-idempotency.md).

`IdempotencySpec`:

| Field | Type | Default | Meaning |
|-------|------|---------|---------|
| `name` | `str \| StrEnum` | required | store namespace |
| `ttl` | `timedelta` | `24h` | how long a completed result is remembered |
| `encrypt_result` | `bool` | `False` | seal the cached result at rest (AAD binds tenant + op:key; needs a keyring) |

The port lifecycle the wrap drives:

| Method | Notes |
|--------|-------|
| `begin(...)` | claim `(operation, key, payload-hash)`; returns the stored result if already complete |
| `commit(...)` | store the encoded result on success |
| `fail(...)` | release the claim so a retry can re-run |

## Distributed lock

`ctx.dlock.query(spec)` / `ctx.dlock.command(spec)` give cross-process mutual exclusion.

`DistributedLockSpec`:

| Field | Type | Default | Meaning |
|-------|------|---------|---------|
| `name` | `str \| StrEnum` | required | lock namespace |
| `ttl` | `timedelta` | `30s` | lease lifetime — a held lock auto-expires after this |
| `requires_fencing_token` | `bool` | `False` | fail closed at wiring unless the backend issues fencing tokens (`AcquiredLock.token`) |

| Port | Method | Notes |
|------|--------|-------|
| command | `acquire(key, owner)` | returns an `AcquiredLock` (with an optional fencing `token`), or `None` if already held |
| command | `release(key, owner)` | release if owned; returns whether it was |
| command | `reset(key, owner)` | renew the lease — reset the TTL of a held lock (the fencing token doesn't change); returns whether it was held |
| query | `is_locked(key)` | presence check |
| query | `get_owner(key)` | the current owner, or `None` |
| query | `get_ttl(key)` | remaining lease time, or `None` |

For a long-held critical section, use the `DistributedLockScope` kit with an `extend_interval`
to renew the lease rather than picking a large `ttl`.

## Implemented by

| Contract | Backend | Integration |
|----------|---------|-------------|
| Idempotency | Redis, Postgres | [Redis](../../integrations/redis.md) · [Postgres](../../integrations/postgres.md) |
| Distributed lock | Redis | [Redis](../../integrations/redis.md) |

A mock implements both for tests.
