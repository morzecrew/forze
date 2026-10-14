# RFC 0072 — Fence advance

- **Status:** 📝 Draft
- **Scope:** Two pieces that make fencing tokens safe after a lock backend loses its data:
  - an optional capability on the distributed-lock command port that moves a lock's fencing
    counter up to the highest fence a store holds, so new tokens are not refused for ever;
  - a claim rule for the store: a token's first write must find the fence strictly lower,
    so two holders that drew the same token cannot both act on it.

  It covers the capability protocol and its flag, the Redis and mock implementations, a
  fail-closed spec flag, and the write rule the docs and examples state. It adds no
  fenced-write helper for any store, and does not persist the counter anywhere but the lock
  backend.
- **Related:**
  - [`src/forze/application/contracts/dlock/`](../src/forze/application/contracts/dlock/):
    `FencingAware`, `DistributedLockCapabilities`, `DistributedLockSpec`;
  - [`src/forze_redis/adapters/dlock.py`](../src/forze_redis/adapters/dlock.py) and
    [`src/forze_redis/kernel/scripts.py`](../src/forze_redis/kernel/scripts.py);
  - RFC 0071 (lifetime leadership), whose `fence_floor` calls the capability and whose terms
    claim their token.
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

`advance_fence(key, past=n)` sets the counter to the larger of its value and `n`, so the next
token is above every fence the store holds. The Redis implementation is one script.

That alone cannot make tokens unique after a loss. The store only knows tokens that were
written, and a holder may have drawn a token it has not written yet. So the write rule gains
a **claim**:
- a holder's first fenced write under a token must find the stored fence strictly lower, and
  sets it to the token;
- later writes under the same token accept an equal fence.

Two holders that drew the same token both try to claim it, exactly one succeeds, and the
other ends its term. That holds after any number of losses, and for any value of `past`,
zero included.

## 2. Motivation

**The counter does not survive data loss.** The Redis lock draws its fencing token from an
`INCR` counter beside the lock key, which deliberately has no TTL
(`forze_redis/kernel/scripts.py:5-21`, `forze_redis/adapters/dlock.py:58-59`, `:125-130`). A
Redis that loses its data restarts the counter at 1. A store that refuses tokens lower than
the highest it has seen then refuses every write until the counter climbs back past its
stored fence.

**A counter that catches up can repeat a token.** The contract's rule rejects a lower token
and accepts an equal one, and must: a holder writes several times under one token. Once the
restarted counter reaches the stored fence, a new holder draws a token equal to one that a
previous holder may still be writing with.

**Moving the counter past the stored fence is not enough.** The store's highest fence counts
only tokens that were written. A holder that drew token 51 and has not written yet (an idle
relay) is invisible to a store whose fence is 50. Moved to 50, the counter hands the next
holder 51 as well.

**No jump is enough either.** Jumping a fixed distance (an epoch) above the stored fence
fails near the boundary, and fails again after a second loss before the first new write. The
counter has no durable memory of what it issued, so uniqueness cannot come from the counter.
It has to come from the store, which is durable: the first write under a token claims it.

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
- **The write rule.** The contract says a fenced write rejects a token lower than the highest
  observed, and accepts an equal one (`ports.py:50`, `value_objects.py:41`). The bug-fix
  branch makes `DistributedLockScope`'s SQL example match (`fence <= :token`).
- **No store-side helper exists.** No Postgres or Mongo code reads a dlock token. The
  comparison is in the app's write.
- **Tests.** The mock dlock keeps its counter in memory and has fencing tokens. dlock has
  per-adapter tests and no shared conformance battery.

## 4. Goals / Non-goals

**Goals**

- A supported way to move a lock's fence counter up to a stored fence, without touching
  backend keys.
- After any number of backend losses, no two holders can both write under one token.
- Monotonic: no call ever lowers the counter, and racing calls settle on the largest value.
- Fail closed: a spec that needs the capability is refused on a backend without it.

**Non-goals**

- **Persisting the counter elsewhere** (a database sequence). The token would then come from
  a second system on every acquire.
- **Knowing the stored fence.** Which value to pass is the app's: the highest fence its store
  holds for the key.
- **A fenced-write helper per store.** The rule is two comparisons in the app's `UPDATE`, and
  the docs show both.

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

- **Flags.** `DistributedLockCapabilities` gains `fence_advance: bool = False`, and
  `DistributedLockSpec` gains `requires_fence_advance: bool = False`. With the spec flag set,
  `DistributedLockDeps.command` refuses a backend whose capabilities lack `fence_advance`, as
  it does for `requires_fencing_token`.
- **The bound on `past`.** It must be between 0 and 2^53 − 1, and is checked before sending.
  Redis's Lua compares numbers as doubles, which are exact only below 2^53.
- **No lock ownership needed.** The call only moves a counter that every holder draws from.
  Moving it up can make a token be refused, never accepted, and the claim rule (5.4) makes a
  repeated token harmless.

### 5.2 Redis

One script in `kernel/scripts.py`:

```lua
local raw = redis.call('GET', KEYS[1])
if tonumber(raw or '0') < tonumber(ARGV[1]) then
  redis.call('SET', KEYS[1], ARGV[1])
  return ARGV[1]
end
return raw or '0'
```

- **Numbers travel as strings.** The adapter passes `past` as a string, and the script returns
  the stored string. That keeps Lua's number formatting, which switches to an exponent above
  10^14, away from the counter.
- **The key** is the same fence key `acquire` increments, prepared by the same helper, so
  tenant and namespace prefixes match.

### 5.3 The mock

The mock applies the same rule to its in-memory counter. The PR builds a dlock conformance
battery, since none exists, and runs it against both.

### 5.4 The write rule: claim, then write

A fenced store keeps the highest fence per key. A holder's writes under a token follow two
rules:

```sql
-- the first write of a term claims the token: the stored fence must be strictly lower
UPDATE guarded SET …, fence = :token WHERE key = :key AND fence < :token;   -- 0 rows: not yours
-- every later write of the term accepts an equal fence
UPDATE guarded SET …, fence = :token WHERE key = :key AND fence <= :token;
```

- **Why a claim.** Tokens can repeat after a backend loss: a holder may have drawn a token it
  never wrote, and the counter cannot know. Two holders of one token both try the claim, the
  store serializes them, and exactly one finds the fence lower. The other gets zero rows,
  learns its token is not fresh, and ends its term. It loses nothing, since it had written
  nothing.
- **Why equal afterwards.** A holder writes many times under one token, and its second write
  finds the fence it set itself.
- **Holders track whether they have claimed** their token, in memory, for their term. A
  restarted process starts a new term with a new token.
- **A holder that skips the claim** and writes with `fence <= :token` from the start can
  collide with a duplicate holder. The docs make the claim mandatory for fenced writers that
  may outlive a backend loss.

### 5.5 Using the advance

Before acquiring, a fenced writer reads the highest fence its store holds for the key, calls
`advance_fence(key, past=that)`, and acquires.
- **With the counter intact,** the call changes nothing.
- **With the counter lost,** the next token is above the stored fence, so it is not refused.

If that token repeats one a stale holder drew and never wrote, the claim settles who acts.
RFC 0071's leadership step advances before acquiring, when given a `fence_floor` and the lease
is free, and claims its token before the body's first write.

**One window remains.** An older holder whose token is below the new one, and who has already
claimed it, can keep writing until the new holder's claim raises the stored fence.
- **A responsive older holder** finds its lease gone at its next renewal, and stops at once,
  with no grace on a lost lease (RFC 0071).
- **A paused one** is stopped by the new holder's claim, which RFC 0071 makes as the term
  begins, before the body's first unit of work.

### 5.6 Alternatives considered

- **Jump the counter to a new epoch** (a multiple of 2^32 above the stored fence). This was
  the first revision. It needs headroom it cannot guarantee near a boundary, and it repeats an
  epoch after a second loss before the first new write. Rejected for the claim rule, which
  needs no assumption about the counter.
- **Advance alone, with equal tokens accepted.** That is the first draft. A token drawn but
  not yet written can be issued twice. Rejected.
- **Redis persistence (AOF `everysec`).** It narrows the loss to about a second of `INCR`s but
  does not close it, and it is an operator setting forze cannot check. Recommended in the docs
  as a complement, not a substitute.
- **Draw the token from a database sequence.** It is durable, but acquire would then span two
  systems, and the lock and the token could disagree. Rejected.

## 6. Tests

- **Conformance battery, mock and Redis:**
  - with the counter below `past`, the next acquire draws `past + 1`;
  - with the counter at or above `past`, nothing changes, and the stored value is returned;
  - two racing advances settle on the larger;
  - a `past` outside the bound is refused before sending.
- **Redis integration.**
  - Acquire and release until the counter is 51, with a store fence of 50.
  - Run `FLUSHALL`, then `advance_fence(past=50)` and acquire. The token is 51, repeating the
    unwritten one.
  - Two holders of 51 both claim. Exactly one gets a row, and the other ends its term.
  - Large counters round-trip exactly, with no exponent formatting.
- **`past=0` after a total loss.** Two holders of token 1 claim, and one wins.
- **Tenant-aware adapter.** The advance moves the same key `acquire` increments.
- **Fail closed.** A spec with `requires_fence_advance` on a backend without the capability is
  refused at resolve.

## 7. Docs

The distributed-lock page gains "Fencing after data loss":
- the claim rule and the equal-accepting rule, with both `UPDATE` shapes;
- when to call `advance_fence`;
- why tokens can repeat, and why the claim makes that harmless;
- the window in 5.5;
- AOF as a complement.

## 8. Out of scope

- **Reading the stored fence.** It is app-specific: which table, which column.
- **Fence advance on other backends.** Only Redis and the mock ship dlock today. A new backend
  implements it, or declares it does not.

## 9. Risks

- **A writer that skips the claim.** It is protected only while tokens are unique, which a
  backend loss breaks. The docs and RFC 0071's recipe make the claim the first write.
- **Called with a value from the wrong key** (another lock's fence). It only moves this
  counter up, which can make tokens be refused but never accepted. The docs name the key the
  value must come from.
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
| 4 | `LOCKED` | A token's first write claims it: the stored fence must be strictly lower. Later writes under the same token accept an equal fence, and a lower token is always refused. Tokens can repeat after a backend loss, and the claim serializes holders of a repeated token. The contract and its examples state this rule. |
| 5 | `ASSUMED` | Redis implements the advance as one script over the same fence key `acquire` increments, with numbers passed and returned as strings and `past` bounded below 2^53. The mock applies the same rule, and the PR builds a dlock conformance battery for both. |

## 12. Phasing

One PR: the protocol, the capability and spec flags, Redis and mock, the conformance battery,
the claim rule in the contract text and examples, and the docs section.
