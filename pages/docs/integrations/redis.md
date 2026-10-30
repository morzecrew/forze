---
title: Redis / Valkey
icon: lucide/zap
summary: Cache, counters, idempotency, search snapshots, distributed locks, streams, and pub-sub
---

`forze[redis]` implements fast, ephemeral, and coordination-oriented state on
Redis or Valkey: caching, atomic counters, idempotency records, search-result
snapshots, distributed locks, and the messaging transports (Redis Streams and
pub-sub) — all behind Forze contracts.

## Install

```bash
uv add 'forze[redis]'
```

Needs a reachable Redis or Valkey server.

## The client

```python
from forze.application.execution.lifecycle.builtin import routed_client_lifecycle_step
from forze_redis import RedisClient

redis = RedisClient()
```

Use `RoutedRedisClient` when the tenant or route selects the endpoint.

### Settings

`RedisSettings` builds the URL from parts, so its grammar — which scheme TLS selects,
percent-encoding the credentials, the ACL username and logical database index, omitting
the credentials entirely when there is none rather than emitting a bare `:@` — stays here:

```python
from forze_redis import RedisSettings

cache = RedisSettings(host="cache.internal", port=6379, ssl=True)

cache.dsn      # SecretStr("rediss://cache.internal:6379")
cache.config   # RedisConfig, from the pool fields that are set
```

A plain `BaseModel`, not a `BaseSettings`: mount it on your own root settings class, which
owns the environment prefix and delimiter. `host` has no default — an unset one raises a
configuration error naming the setting rather than connecting to whatever is on localhost.

## Wire it

Each resource takes a **namespace** (a logical key prefix); set
`tenant_aware=True` when keys must include the current tenant. Register the
resources on the deps module, open the pool from the lifecycle plan:

```python
from forze.application.execution import DepsRegistry, LifecyclePlan
from forze_redis import (
    RedisCacheConfig,
    RedisConfig,
    RedisDepsModule,
    RedisIdempotencyConfig,
    redis_lifecycle_step,
)

deps = DepsRegistry.from_modules(
    RedisDepsModule(
        client=redis,
        caches={"orders": RedisCacheConfig(namespace="app:orders", tenant_aware=True)},
        idempotency=RedisIdempotencyConfig(namespace="app:idempotency"),
    ),
)
lifecycle = LifecyclePlan.from_steps(
    redis_lifecycle_step(dsn="redis://localhost:6379/0", config=RedisConfig(max_size=20)),
)
```

A pool holds at most `max_size` connections, and a call made while all of them are busy
fails at once with a connection error. Set `RedisConfig(pool_timeout=timedelta(...))` to
make it wait up to that long for one to be released instead, so a burst queues rather
than fails. The wait applies to each acquisition, `subscribe` included, so a read
retried by `read_retry_attempts` waits again on each attempt.

## What it provides

| Contract | Keyed by |
|----------|----------|
| Cache | `CacheSpec.name` (`caches`) |
| Counter | `CounterSpec.name` (`counters`) |
| Idempotency | `IdempotencySpec.name` (`idempotency`, plain or routed) |
| Search-result snapshots | `SearchResultSnapshotSpec.name` (`search_snapshots`) |
| Distributed locks | `DistributedLockSpec.name` (`dlocks`) |
| [Fleet membership](../running-in-prod/fleet-membership.md) | `FleetSpec.name` (`fleets`) |
| Stream (read/append) | `StreamSpec.name` (`streams`) |
| Stream consumer group (read/ack/claim + provisioning) | `StreamSpec.name` (`stream_groups`) |
| Pub-sub (publish/subscribe) | `PubSubSpec.name` (`pubsub`) |

## Streams & pub-sub

Redis Streams back the `StreamSpec` transport (append/read + consumer groups, the
outbox relay's `to_stream`, and the realtime gateway); Redis Pub/Sub backs the
`PubSubSpec` transport (publish/subscribe, the relay's `to_pubsub`). Register routes
with `RedisStreamConfig`, `RedisStreamGroupConfig`, and `RedisPubSubConfig`:

```python
from forze_redis import RedisPubSubConfig, RedisStreamConfig, RedisStreamGroupConfig

RedisDepsModule(
    client=redis,
    streams={"events": RedisStreamConfig(tenant_aware=True)},
    stream_groups={"events": RedisStreamGroupConfig(tenant_aware=True)},
    pubsub={"presence": RedisPubSubConfig()},
)
```

Unlike the keyed resources above, these carry **no namespace** — the stream/topic
name is supplied per call, and `tenant_aware=True` isolates it with a
`tenant:{id}:` key prefix. Set `encryption="end_to_end"` on the `StreamSpec` /
`PubSubSpec` to seal payloads through the broker (the consumer decrypts); a wired
`CryptoDepsModule` is required, and a deployment `required_reach` floor is
enforced here. Pub-sub is at-most-once past the broker.

**Retention.** A stream without a cap grows in Redis memory until `maxmemory`
eviction takes the keyspace. `RedisStreamConfig(retention_max_entries=...)` applies an
approximate cap at every append (`XADD MAXLEN ~`, O(1) amortized). Cap any stream with
an unbounded producer — the realtime egress stream above all
(`DEFAULT_REALTIME_STREAM_MAX_ENTRIES` is the recommended starting point) — and size it
so the horizon at peak append rate comfortably exceeds the consumer group's reclaim
window: retention bounds how long an *undelivered* entry survives a total consumer
outage. Alarm on the ack admin port's `depth()` (undelivered backlog, pending count,
oldest pending age) long before a cap becomes the failure; the same surface feeds
`quiesce(ack_streams=...)`.

The precise companion is `AckStreamGroupAdminPort.trim_acknowledged(stream)` — a sweep
(`XTRIM MINID` at the floor computed from `XINFO GROUPS` + `XPENDING`) that removes only
entries **every** consumer group has been delivered *and* acked, so it can never outrun
a slow or crashed consumer, and a group-less stream is never trimmed. Run it on an
interval (`realtime_stream_trim_lifecycle_step` for the realtime stream) to keep
steady-state memory near the groups' real horizon; keep the cap as the backstop for a
group nobody drains.

**Pub-sub reconnect.** Long-running subscribers reconnect automatically after a
transport error (`pubsub_auto_reconnect` now defaults to `True` — the previous opt-in
default meant a subscriber silently stopped on the first Redis blip). Opt out per
client config to fail loudly and own the reconnect yourself.

## L1 push invalidation

`RedisCacheConfig(invalidation_push=True)` enables Redis 6+ **client-side
caching** (`CLIENT TRACKING`, RESP3 push) for caches backing the
[document L1](../data-events/caching.md#an-in-process-l1-for-hot-documents):
one pinned connection per client receives an invalidation push for every
write, expiration, or eviction under the cache's key prefix — by any replica —
and the in-process L1 drops the entry immediately, demoting the L1 TTL to a
backstop. Requires redis-py 8+ — on redis-py 7, enabling it fails closed with a
`configuration` error. Fails open at runtime (stream loss flushes the L1 and falls back to TTL
semantics while reconnecting); tenant-routed clients and dynamic namespaces
stay TTL-only by design.

## Fleet-wide resilience state

Two builders turn the process-local [resilience](../running-in-prod/resilience.md)
state into shared, fleet-wide state:

```python
from forze.application.execution import ResilienceDepsModule
from forze_redis import redis_circuit_breaker_store, redis_rate_limit_store

ResilienceDepsModule(
    breaker_store=redis_circuit_breaker_store(redis),
    rate_limit_store=redis_rate_limit_store(redis),
)
```

The breaker store makes an open circuit on one replica protect them all; the
rate-limit store keeps the token bucket in a Redis hash mutated atomically by
Lua on the **server clock**, so the declared `permits/per` is the fleet's rate
(not per-replica). Both fail open to process-local state on a Redis error —
see [Fleet-wide state](../running-in-prod/resilience.md#fleet-wide-state).

## Distributed locks and fencing

A lock alone is best-effort exclusion: a holder paused by GC or a network
partition can resume after its lease expired while a new holder runs. To close
that gap, `acquire` returns an `AcquiredLock` whose `token` is a **fencing
token** — monotonically increasing per key across lock generations. The Redis
adapter issues it atomically with the `SET NX PX` acquire (a Lua script `INCR`s
a per-key `<lock key>:fence` counter; the counter has no TTL and is never
deleted on release, so tokens stay monotonic even after expiry — at the cost of
one small permanent key per lock key).

Protect downstream writes by sending the token with the write. The store keeps
the highest fence per resource, and a holder writes in two shapes:

```sql
-- The first write under a token claims it.
UPDATE invoice SET ..., fence = :token WHERE id = 42 AND fence < :token;

-- Every later write under the same token: an equal fence is the holder's own claim.
UPDATE invoice SET ..., fence = :token WHERE id = 42 AND fence <= :token;
```

A lower token is refused by both. **Zero rows on any of these writes, the claim
or a later one, means your token is no longer current: end the term and stop
writing.** In code, the claim is the first thing the holder writes, and every
write checks its result:

```python
async with dlock_scope.scope("invoice:42") as lock:
    if not await repo.claim_invoice(invoice, fence_token=lock.token):
        return  # another holder has this token

    for change in changes:
        if not await repo.update_invoice(change, fence_token=lock.token):
            return  # a higher token has claimed the invoice since
```

Keep whether you have claimed in memory for the term; a restarted process
acquires again and gets a new token. Extending a live lease (`reset`, the
scope's heartbeat) keeps the same token — only a fresh acquisition starts a new
generation. Without the consumer-side checks the lock remains best-effort
exclusion; the checks are what upgrade it to fenced exclusion.

## Fencing after data loss

The fence counter lives in Redis, so a Redis that loses its data restarts every
counter at 1. Two things then go wrong:

- **New tokens are refused.** The store's fence is far above the restarted
  counter, so it refuses every write until the counter climbs back past it.
- **Tokens repeat.** Once the counter catches up, it hands out tokens it already
  issued. The store cannot tell you which: it only knows tokens that were
  written, and a holder may have drawn a token it has not written yet.

`advance_fence` fixes the first, and the claim makes the second harmless. Before
acquiring, read the highest fence the store holds **for that key** and move the
counter up to it. Lock through the same port you advanced: counters live under
the lock route's namespace, so a scope built from another spec can draw from
another counter.

```python
from typing import cast

from forze.application.contracts.dlock import DistributedLockSpec, FenceAdvancing
from forze_kits.scopes import DistributedLockScope

spec = DistributedLockSpec(
    name="invoices",
    requires_fencing_token=True,
    requires_fence_advance=True,
)


async def advance_then_lock(ctx, repo, worker_id):
    port = ctx.dlock.command(spec)
    stored = await repo.highest_fence("invoice:42")  # SELECT fence ... WHERE id = 42
    await cast(FenceAdvancing, port).advance_fence("invoice:42", past=stored)

    scope = DistributedLockScope(cmd=port, owner_provider=lambda: worker_id)

    async with scope.scope("invoice:42") as held:
        ...  # claim with held.token, then write, checking every write's rows
```

- **With the counter intact,** the counter is already above every fence the
  store holds, so the call changes nothing. Call it on every acquire rather than
  trying to detect a loss.
- **With the counter lost,** the next token is above the fence you read. The
  store's fence can still rise before your claim, if a holder from before the
  loss claims a higher token it drew then. Your claim then finds zero rows,
  a refusal like any other: end the term and acquire again.
- **It never lowers the counter** and needs no lock ownership; racing calls
  settle on the largest value. `past` must be a plain `int` from 0 to 2⁵³ − 1
  and is refused before anything is sent.
- **Tokens stop at 2⁵³ − 1:** an acquire past it is refused with
  `dlock.fence_exhausted` and leaves the lock free, so every token, and every
  fence a store holds, is a valid `past`.
- **Set `requires_fence_advance`** so resolving the port fails at wiring on a
  backend without it. Narrow the port with `cast`: `ctx.dlock.command` returns a
  guarded stand-in, and `isinstance(port, FenceAdvancing)` is always false on it.

The next token can still equal one a holder drew before the loss and never
wrote. Both holders then try the claim, the store serializes the two `UPDATE`s,
and exactly one finds the fence lower. The other gets zero rows and ends its term
having written nothing. That holds after any number of losses, and for any
`past`, zero included.

**Two windows remain**, and in both the store keeps one writer at a time:

- **An older holder with a lower token** that has already claimed it can keep
  writing until the new holder's claim raises the stored fence. The new holder's
  claim is what stops it, so claim as the term begins, before the first unit of
  work.
- **An older holder with a higher token** it drew before the loss and never
  wrote can claim it after the new holder has, since the counter has no memory
  of the tokens it issued. The new holder's next write then finds zero rows, and
  it stops. The older holder keeps writing until its next lease renewal fails, or
  until a later term's claim goes above it: calling `advance_fence` with the
  store's current fence before each acquire puts the next term's token there.

**AOF persistence complements this; it does not replace it.** `appendfsync
everysec` narrows a loss to about a second of `INCR`s but does not close it, and
it is a server setting forze cannot check.

## Notes

- **Namespaces are deliberate.** Give each contract its own namespace; don't
  share one across unrelated resources.
- **TTLs are logical.** `CacheSpec` / `IdempotencySpec` TTLs are expiry intents —
  Redis memory eviction can drop keys earlier, so size and policy the server
  accordingly.
- **Pre-encoded cache values carry a marker.** A `bytes` cache value is stored
  behind a fixed prefix that starts with a NUL byte, so reads return it
  verbatim; JSON values are stored as plain JSON. A replica on an older release
  misses on marked entries, and `bytes` entries it wrote come back parsed, or as
  a miss, on a newer one. While both releases run, an older replica can also keep
  serving a value it can still read after a newer one rewrote the entry. To avoid
  mixed reads, cut over without overlap or point the new release at a fresh cache
  namespace; otherwise flush the namespace once every replica runs the new
  release. Document read-through caching accepts both forms.
- **Idempotency** needs stable keys and a TTL longer than client retries; every
  worker that can handle an operation must share the same namespace.
- **Routed clients** use `routed_client_lifecycle_step` — don't mix routed and
  non-routed lifecycle steps for one client.
