# RFC 0072 — Fence advance

- **Status:** 📝 Draft
- **Scope:** An optional capability on the distributed-lock command port that moves a lock's
  fencing counter forward to at least a given value. After a lock backend loses its data, it
  lets a fenced writer draw tokens above the fence its store already holds. It covers:
  - the capability protocol and its capability flag;
  - the Redis and mock implementations;
  - a fail-closed spec flag;
  - the rule a fenced write applies.

  It does not add a fenced-write helper for any store, and it does not persist the counter
  anywhere but the lock backend.
- **Related:**
  - [`src/forze/application/contracts/dlock/`](../src/forze/application/contracts/dlock/):
    `FencingAware`, `DistributedLockCapabilities`, `DistributedLockSpec`;
  - [`src/forze_redis/adapters/dlock.py`](../src/forze_redis/adapters/dlock.py) and
    [`src/forze_redis/kernel/scripts.py`](../src/forze_redis/kernel/scripts.py);
  - RFC 0071 (lifetime leadership), whose `fence_floor` calls this.
- **Origin:** hh-backend-v2's fleet proposals (F3, 2026-10-14). The app writes forze_redis's
  private fence key directly (`fences.py`, 137 LOC), because "forze has no call that moves a
  counter".

---

## 1. Summary

```python
@runtime_checkable
class FenceAdvancing(Protocol):
    async def advance_fence(self, key: str, *, past: int) -> int: ...
```

After `advance_fence(key, past=n)`, the next token `acquire` draws for that key is greater
than `n`, and the counter has never moved backwards. The Redis implementation is one script:
`counter = max(counter, past)`, returning the counter.

A fenced writer that knows the highest fence its store holds can call it before acquiring.
After a Redis that lost its data, its next token is then above anything the store has seen,
instead of being refused until the counter catches up, or equal to a stale holder's.

## 2. Motivation

**The counter does not survive data loss.** The Redis lock draws its fencing token from an
`INCR` counter beside the lock key, which deliberately has no TTL
(`forze_redis/kernel/scripts.py:5-21`, `forze_redis/adapters/dlock.py:58-59`, `:125-130`). A
Redis that loses its data restarts the counter at 1. A store that refuses tokens lower than
the highest it has seen then refuses every write until the counter climbs back past its
stored fence.

**When the counter catches up, the token can come out equal.** The contract's rule rejects a
lower token and accepts an equal one, and must: a holder writes several times under one
token. So once the restarted counter reaches the stored fence, a new holder draws a token
equal to a token a previous holder may still be writing with.

**Nothing moves the counter today.** The command port has `acquire`, `release` and `reset`
(`contracts/dlock/ports.py:40-84`). hh-backend-v2 writes the private `dlock:…:fence` key
itself, which breaks the moment forze renames it.

## 3. Current state

All verified against the tree at the time of writing.

- **The Redis keys.** The fence key is `construct_key("dlock", key, "fence")`, for example
  `dlock:app:invoice:42:fence`, tenant-prefixed when the adapter is tenant-aware
  (`dlock.py:58-59`).
- **Acquire** is `SET NX PX` on the lock key, then `INCR` of the fence key, only on success.
  `release` and `reset` never touch the fence key.
- **Capabilities.** `FencingAware.capabilities()` returns
  `DistributedLockCapabilities(fencing_tokens: bool)` (`ports.py:90-102`,
  `value_objects.py:12-25`). `DistributedLockDeps.command` refuses a backend without fencing
  when the spec sets `requires_fencing_token` (`deps.py:46-79`). This mirrors `IsolationAware`.
- **The comparison rule.** The contract says a fenced write rejects a token lower than the
  highest observed (`ports.py:50`, `value_objects.py:41`). `DistributedLockScope`'s SQL example
  used a strict `fence < :token`, which also refused the same holder's second write
  (`scopes/dlock.py:47-48`); the bug-fix branch makes it match the contract.
- **No store-side helper exists.** No Postgres or Mongo code reads a dlock token. The
  comparison belongs to the app's write.
- **The mock dlock** keeps its counter in memory and has fencing tokens.

## 4. Goals / Non-goals

**Goals**

- A supported way to move a lock's fence counter forward, without touching backend keys.
- Monotonic: no call ever lowers the counter, and racing calls settle on the largest value.
- Fail closed: a spec that needs the capability is refused on a backend without it.

**Non-goals**

- **Persisting the counter elsewhere** (a database sequence). The token would then come from
  a second system on every acquire.
- **Knowing the stored fence.** Which value to pass is the app's: the highest fence its store
  holds for the key.
- **A fenced-write helper per store.** The rule is one comparison in the app's `UPDATE`, and
  the docs show it.

## 5. Design

### 5.1 The capability

```python
@runtime_checkable
class FenceAdvancing(Protocol):
    async def advance_fence(self, key: str, *, past: int) -> int:
        """Move the key's fence counter to at least ``past``.

        The next token ``acquire`` draws for the key is greater than ``past``. Returns the
        counter after the call. Never lowers it.
        """
```

- `DistributedLockCapabilities` gains `fence_advance: bool = False`.
- `DistributedLockSpec` gains `requires_fence_advance: bool = False`. With it set,
  `DistributedLockDeps.command` refuses a backend whose capabilities lack `fence_advance`, as
  it does for `requires_fencing_token`.
- `past` must be zero or more. A negative value is a validation error.
- The call needs no lock ownership. It only moves a counter that every holder draws from, and
  moving it forward can only refuse stale tokens, never admit them.

### 5.2 Redis

One script in `kernel/scripts.py`:

```lua
local c = tonumber(redis.call('GET', KEYS[1]) or '0')
local past = tonumber(ARGV[1])
if c < past then
  redis.call('SET', KEYS[1], past)
  c = past
end
return c
```

- The key is the same fence key `acquire` increments, built by the same helper, so tenant
  prefixes match.
- The next `INCR` then returns at least `past + 1`.

### 5.3 The mock

The mock keeps the same `max` rule over its in-memory counter, so the conformance battery
runs unchanged against both.

### 5.4 Using it

A fenced writer that has lost confidence in the counter, at startup or before a campaign,
does three things:
1. It reads the highest fence its store holds for the key.
2. It calls `advance_fence(key, past=that)`.
3. It acquires.

The new holder's first write then raises the store's fence above anything a stale holder
carries, and the stale holder's writes are refused from then on.

RFC 0071's leadership step does this before each acquire when given a `fence_floor`.

**One window remains.** Between the new holder's acquire and its first write, a stale holder
whose token equals the stored fence can still write once. The stale holder's own next renewal
finds its lease gone and ends its term, so the window is bounded by its renewal interval. The
docs state it.

### 5.5 Alternatives considered

- **Redis persistence (AOF `everysec`).** It narrows the loss to about a second of `INCR`s but
  does not close it, and it is an operator setting forze cannot check. Recommended in the docs
  as a complement, not a substitute.
- **Draw the token from a database sequence.** It is durable, but acquire would then span two
  systems, and the lock and the token could disagree. Rejected.
- **Seed the counter from the clock** (milliseconds since the epoch). It survives data loss
  without the app's help, but a clock stepped back draws lower tokens, and every existing
  stored fence would refuse the first token after the switch. Rejected.

## 6. Tests

- **Unit, mock and Redis, through the conformance battery:**
  - advance past the counter: the next acquire draws `past + 1`;
  - advance below the counter: no change, and the current value is returned;
  - two racing advances settle on the larger.
- **Redis integration.** Acquire and release until the counter is 50. Run `FLUSHALL`, then
  `advance_fence(past=50)` and acquire: the token is 51.
- **Tenant-aware adapter.** The advance moves the same key `acquire` increments.
- **Fail closed.** A spec with `requires_fence_advance` on a backend without the capability
  is refused at resolve.

## 7. Docs

The distributed-lock page gains "Fencing after data loss":
- the rule a fenced write applies (accept equal, reject lower);
- the `UPDATE … WHERE fence <= :token` shape;
- when to call `advance_fence`;
- the window in 5.4;
- AOF as a complement.

## 8. Out of scope

- **Reading the stored fence.** It is app-specific: which table, which column.
- **Fence advance on other backends.** Only Redis and the mock ship dlock today. A new
  backend implements it, or declares it does not.

## 9. Risks

- **Called with a value from the wrong key** (another lock's fence). It only moves this
  counter forward, which can refuse tokens but never admit stale ones. The docs name the key
  the value must come from.
- **Read as making fencing unnecessary.** It makes fencing work after data loss. Writes still
  have to check the token.

## 10. Unresolved questions

None that block the design. The capability's exact module placement follows `FencingAware`.

## 11. Decisions

| # | Grade | Decision |
| --- | --- | --- |
| 1 | `LOCKED` | `advance_fence(key, *, past) -> int` sets the counter to the larger of its value and `past`, so the next token is greater than `past`. It never lowers the counter and needs no lock ownership. |
| 2 | `ASSUMED` | It is a separate `@runtime_checkable` protocol, `FenceAdvancing`, flagged by `DistributedLockCapabilities.fence_advance`, following `FencingAware`. |
| 3 | `ASSUMED` | `DistributedLockSpec.requires_fence_advance` makes resolution fail closed on a backend without the capability. |
| 4 | `LOCKED` | A fenced write accepts a token equal to or greater than the stored fence and rejects a lower one. The contract and its examples state the rule identically. |
| 5 | `ASSUMED` | Redis implements it as one script over the same fence key `acquire` increments, and the mock applies the same rule. |

## 12. Phasing

One PR: the protocol, the capability flag and spec flag, Redis and mock, the conformance
cases, and the docs section.
