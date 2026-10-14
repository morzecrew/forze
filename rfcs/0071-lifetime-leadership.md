# RFC 0071 — Lifetime leadership

- **Status:** 📝 Draft
- **Scope:** A lifecycle step that elects one replica to run a body for as long as it holds a
  renewing lease. It covers:
  - campaigning, renewal and release;
  - a `Term` that tells the body the moment its lease is lost, expired, yielded or stopped;
  - a fencing token per term;
  - an opt-in handoff, where a preferred replica asks the holder to yield.

  It is built on the existing distributed-lock ports, so there is no new contract and no new
  backend. The startup-only `singleton_lifecycle_step` stays as it is. Fleet membership, fence
  advance and rebalancing are their own RFCs (0073, 0072, 0076).
- **Related:**
  - [`src/forze_kits/lifecycle/singleton.py`](../src/forze_kits/lifecycle/singleton.py): the
    startup-only guard;
  - [`src/forze_kits/scopes/dlock.py`](../src/forze_kits/scopes/dlock.py):
    `DistributedLockScope`;
  - [`src/forze/application/contracts/dlock/`](../src/forze/application/contracts/dlock/):
    the lock ports and spec;
  - [`src/forze/application/execution/background/`](../src/forze/application/execution/background/):
    `BackgroundLoopControl`, `run_supervised`;
  - RFC 0072 (fence advance), RFC 0073 (fleet membership).
- **Origin:** hh-backend-v2's fleet proposals (F2, 2026-10-14). The app hand-built this twice:
  its outbox courier (about 250 LOC with handoff) and its lobby publisher (57 LOC), pinned by
  18 unit tests and a fault test.

---

## 1. Summary

`leadership_lifecycle_step(spec, owner=..., lead=...)` campaigns for a lease on every replica,
and runs `lead(ctx, term)` on the one that holds it:
- it renews the lease while the body runs;
- it ends the term on the earliest of a refused renewal, a passed deadline, a stop or a
  yield;
- it gives the body a grace period to return before cancelling it;
- it releases the lease on the way out, so a successor leads within one campaign interval
  rather than a full TTL.

`term.token` carries the lock's fencing token for the body's writes. With
`handoff="on_request"`, a replica that prefers the unit asks for it, and the holder yields at
its next renewal.

## 2. Motivation

forze has no way to run work on exactly one replica for the life of the process, and its own
docs recommend a guard that cannot do it.
- **The singleton guard only guards startup.** `singleton_lifecycle_step` acquires once, runs
  the inner startup, and releases in a `finally` (`singleton.py:29-49`). Its docstring says a
  lifetime singleton needs "a renewing lease, which this helper does not provide — run such
  work behind your own held-lease leader election instead" (`singleton.py:80-86`).
- **A replica that skips never retries,** so if the replica that ran startup dies, nobody
  takes over.
- **The scope does not tell its body.** `DistributedLockScope` can renew on an
  `extend_interval`, but when a renewal is refused it only logs, stops renewing, and raises at
  scope exit (`scopes/dlock.py:167-182`, `:239-240`). The body keeps running unaware, and
  nothing campaigns again.
- **Docstrings recommended the wrong tool.** The durable recovery and scheduler steps and the
  realtime sweep and trim steps told readers to pair them with the singleton guard for "a
  single elected scanner" (`durable/lifecycle.py:233-236`, `:461-463`;
  `realtime/lifecycle.py:619-621`, `:690-692`). Their startups only create a task, so the
  lock is released at once and every replica that starts later runs its own loop. Those
  loops tolerate concurrency, so the advice was harmless but wrong, and the bug-fix branch
  corrects it.

hh-backend-v2 built held-lease leadership twice, with handoff, deadlines and fencing. Every
app that wants one relay, one publisher or one coordinator across a fleet will build it again.

## 3. Current state

All verified against the tree at the time of writing.

- **The dlock ports.**
  - `DistributedLockCommandPort`: `acquire(key, owner) -> AcquiredLock | None`,
    `release(key, owner) -> bool`, and `reset(key, owner) -> bool`, which sets the TTL back
    to `spec.ttl` (`contracts/dlock/ports.py:40-84`).
  - `DistributedLockQueryPort`: `is_locked`, `get_owner`, `get_ttl` (`ports.py:12-33`).
  - Both are resolved per spec through `ctx.dlock` (`contracts/dlock/deps.py:35`, `:46`).
- **The spec and fencing.**
  - `DistributedLockSpec` has `ttl` (30 s by default) and `requires_fencing_token`
    (`specs.py:15-41`). `AcquiredLock` carries `token: int | None`
    (`value_objects.py:31-58`).
  - A spec that requires fencing fails closed on a backend without it (`deps.py:46-79`).
- **The Redis backend** acquires with `SET NX PX` and draws the token from a separate `INCR`
  counter (`forze_redis/kernel/scripts.py:5-21`). `reset` re-arms the TTL
  (`forze_redis/adapters/dlock.py:150-157`).
- **The singleton guard** (`singleton.py:29-117`) ignores the token, never renews and never
  retries. It is marked `mutates_shared_state=True, singleton_guarded=True`.
- **The scope** yields `AcquiredLock`, whose token is stable across `reset`. It exposes no
  event or callback for a lost lock (`scopes/dlock.py:40-54`, `:148`).
- **Loop primitives.**
  - `BackgroundLoopControl` provides `arm`, `request_stop`, `sleep_or_stop` and `stop`.
  - `run_supervised` restarts with a jittered backoff and treats a configuration error as
    terminal.
  - Loops registered with `ctx.drainables` are stopped by the runtime before teardown
    (`runtime.py:462`).
- **Step markers.** `LifecycleStep` has `mutates_shared_state`, `singleton_guarded` and
  `requires_long_running`. The `FLEET` profile refuses a step that mutates shared state
  without `singleton_guarded`, and `SERVERLESS` refuses `requires_long_running`
  (`runtime.py:269-296`).
- **No other leader election exists** in `src/`. The closest are per-run durable leases
  (`run_store.py:234-255`) and in-process single-flight lanes, which are unrelated.

## 4. Goals / Non-goals

**Goals**

- At most one replica runs a given `lead` body at a time, as long as every holder is
  responsive and the lock backend keeps its data. Fencing covers the cases where it is not
  or does not (5.4).
- The body learns that its term ended, without polling the backend.
- A stopping leader hands over within one campaign interval, and a dead one within the lease
  TTL.
- An opt-in handoff, so a preferred replica can take a unit back without a restart.
- No new port, and the step works over any dlock backend, the mock included.

**Non-goals**

- **Consensus.** This is a lease over a lock service. The design does not claim more than the
  lock and fencing give it.
- **Changing `singleton_lifecycle_step`.** Startup-only work, such as creating a stream
  group, is a correct use of it, and its semantics stay.
- **Choosing preferences.** Which replica prefers which unit is the app's (for example, a
  courier's preferred routes).

## 5. Design

### 5.1 The spec and the step

```python
@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class LeadershipSpec:
    lock: DistributedLockSpec                 # ttl, requires_fencing_token
    key: str
    renew_every: timedelta | None = None      # default lock.ttl / 3
    campaign_every: timedelta = timedelta(seconds=2)
    margin: timedelta = timedelta(seconds=1)  # deadline = renewal sent + ttl - margin
    yield_grace: timedelta = timedelta(seconds=5)
    verify: bool = False                      # Term.held() also reads the owner


def leadership_lifecycle_step(
    spec: LeadershipSpec,
    *,
    owner: str,
    lead: Callable[[ExecutionContext, Term], Awaitable[None]],
    may_campaign: Callable[[ExecutionContext], Awaitable[bool]] | None = None,
    handoff: Literal["none", "on_request"] = "none",
    prefer: Callable[[ExecutionContext], Awaitable[bool]] | None = None,
    fence_floor: Callable[[ExecutionContext], Awaitable[int]] | None = None,
    step_id: StrKey = "leadership",
) -> LifecycleStep: ...   # requires_long_running, mutates_shared_state, singleton_guarded
```

- **Validation.** `margin < lock.ttl` and `renew_every + margin < lock.ttl` are checked when
  the spec is built. `handoff="on_request"` requires `prefer`.
- **The loop.** The step runs one campaign loop under `BackgroundLoopControl` and
  `run_supervised`, registered with `ctx.drainables`.
- **The markers.**
  - `singleton_guarded=True` is honest: the body runs on one replica.
  - `mutates_shared_state=True` describes the body.
  - `requires_long_running=True` keeps it off `SERVERLESS`.

### 5.2 The term

```python
class Term(Protocol):
    token: int | None                   # the lock's fencing token, stable for the term
    ended: asyncio.Event                # set once, by the step
    reason: TermEnd | None              # "lost" | "expired" | "stopped" | "yielded" | "returned"
    def live(self) -> bool: ...         # not ended and before the deadline; no I/O
    async def held(self) -> bool: ...   # live(), and with verify, the backend still names us
```

**The deadline** is `t_sent + lock.ttl - margin`, where `t_sent` is the monotonic time just
before the acquire or the latest successful renewal was sent. It is never measured from the
answer. A renewal that took long to come back therefore shortens the term, and never
lengthens it.

**A term ends on the first of these:**
- a renewal answered `False`, because the lease is gone (`lost`);
- the deadline passing without a successful renewal, for example while the backend is
  unreachable (`expired`);
- the step stopping (`stopped`);
- a handoff (`yielded`);
- the body returning on its own (`returned`).

**When it ends:**
1. The step sets `term.ended`.
2. The body has `yield_grace` to return at a unit boundary, and is cancelled after that.
3. The step releases the lease if it may still hold it. The release is shielded, so a
   cancelled stop still releases.
4. The step goes back to campaigning, unless it is stopping.

A body that raises ends its term the same way, and `run_supervised` backs off before the next
campaign.

### 5.3 Campaigning and renewal

- **Campaigning.** Every `campaign_every`, a replica that is not stopping, whose
  `may_campaign` allows it, and that is not deferring to a handoff (5.5) does three things:
  1. With `fence_floor` set, it advances the fence first (RFC 0072).
  2. It tries `acquire`.
  3. On success, it starts a term and runs `lead` in a task.
- **Renewal.** Every `renew_every`, it calls `reset`.
  - `True` moves the deadline.
  - `False` ends the term as `lost`.
  - An error or a timeout leaves the deadline where it was, and the term ends as `expired`
    once it passes.

  Each renewal is bounded by a timeout shorter than the time left before the deadline.
- **Stop.** The step ends the term as `stopped`, waits out the grace period, releases and
  returns. A successor that is campaigning takes over within `campaign_every`.

### 5.4 What it guarantees, and what fencing is for

- **A responsive leader stops acting within `margin`** of the moment its lease could be gone.
  The deadline is measured locally, from before the renewal was sent.
- **A leader that is not responsive is not stopped by any of this:** a paused process, a
  blocked event loop, a long GC. Neither is one whose lock backend lost its data, so the
  lease is gone while the leader cannot yet know it. Those are what the fencing token is for.
  - The body passes `term.token` with each write it makes as leader.
  - The store refuses a token lower than the highest it has seen. This is the contract's
    rule, stated the same way in its example since the bug-fix branch.
- **Requiring fencing.** A spec that needs a token sets `lock.requires_fencing_token`, and a
  backend without one fails closed, as today.
- **After Redis loses its data,** the fence counter restarts low, and the tokens it draws are
  refused until it catches up. RFC 0072's fence advance closes that gap: `fence_floor` lets
  the step move the counter past the highest stored fence before campaigning.
- **`verify=True`** makes `held()` also read the owner through the query port. A body can
  then check before an expensive or irreversible unit, at the cost of one read. This catches
  a backend that lost the key between renewals.

### 5.5 Handoff on request

With `handoff="on_request"`, a replica that does not hold the lease, but whose `prefer` returns
true, asks for it by holding a second lock on the same backend, `{key}:want`. It acquires the
want-lock and renews it on each campaign tick.
- **The holder** reads the want-lock's owner at each renewal. If another replica holds it,
  and the holder's own `prefer` is false, it ends its term as `yielded` and releases. A holder
  that prefers the unit itself never yields, so two preferring replicas cannot pass the unit
  back and forth.
- **Other replicas** skip their campaign while another replica holds the want-lock, so the
  one that asked is the only campaigner once the lease is free.
- **The asker** releases the want-lock once it holds the lease. If it dies first, the
  want-lock expires within its TTL, and campaigning goes back to normal.

The handoff costs one owner read per renewal, and only when it is enabled.

### 5.6 Observability

- A gauge `forze.leadership.held` (0 or 1), labelled by key.
- A counter `forze.leadership.terms_ended`, labelled by key and reason.
- A log line for each term start and end, with the token and the reason.

### 5.7 Alternatives considered

- **A lost-lock callback on `DistributedLockScope`.** The scope is a bounded critical section
  that a caller enters once. It has no campaign, no grace, no release-and-retry, and no
  handoff. Adding them would turn a scope into a supervisor. Rejected.
- **Making the singleton guard hold its lock for the step's lifetime.** It would silently
  change a step that startup-only work uses correctly today, such as group ensure. Rejected
  in favour of a second step.
- **A dedicated leadership port with a Raft-like backend.** The guarantee is bounded by the
  lock backend either way, and every deployment already has the dlock ports. Rejected until
  a deployment needs more than lease plus fencing.

## 6. Tests

Unit tests run against the mock dlock with a controllable clock. Integration tests run against
Redis.
- **One leader.** Three replicas campaign, and exactly one runs `lead`.
- **Takeover.**
  - The leader stops: it releases, and a successor leads within `campaign_every`.
  - The leader is killed without releasing: a successor leads within the lease TTL.
- **Losing the lease.**
  - Delete the lease key under a running term: `term.ended` is set as `lost` at the next
    renewal.
  - Make renewals hang: the term ends as `expired` at the deadline, measured from when the
    last good renewal was sent.
- **Grace.** A body that ignores `term.ended` is cancelled after `yield_grace`, and the lease
  is released.
- **Fencing.** Tokens grow strictly across terms, and `term.token` is stable within one.
- **Handoff.**
  - A preferring replica takes the unit from a non-preferring holder at the holder's next
    renewal.
  - Two preferring replicas do not pass the unit back and forth.
  - A wanting replica that dies lets the others campaign once its want-lock expires.
- **`may_campaign`.** A replica it refuses never leads.
- **Markers.** The `FLEET` profile accepts the step, and `SERVERLESS` refuses it.
- **Bounded doubles.** Every retry loop's double is a bounded script, so a lost loop bound
  fails in milliseconds.

## 7. Docs

- A "Run one replica's worth of work" section on the lifecycle page: when to use this step,
  when the singleton guard is enough, and what fencing is for.
- A recipe: an outbox relay or a publisher behind leadership, with a fenced write.
- The durable and realtime docstrings corrected on the bug-fix branch gain a pointer to this
  step.

## 8. Out of scope

- **Fence advance itself.** It is RFC 0072. This step only calls it through `fence_floor`.
- **Fleet membership and assignment** (RFC 0073). Leadership elects one replica per key, and
  membership tells replicas who else is alive.
- **Leadership across lock backends** (a lease in Redis guarding a write in Mongo without
  fencing). The fencing token is the bridge, and the store's check is the app's.

## 9. Risks

- **Read as consensus.** The docs say what the term guarantees: a responsive leader stops
  within `margin`, and an unresponsive one is stopped by fencing. Bodies that skip fencing
  are told what they are giving up.
- **Short TTLs and slow backends.** A TTL close to the backend's latency makes terms flap.
  Validation enforces `renew_every + margin < ttl`, and the docs suggest a TTL of at least ten
  times the renewal latency.
- **A body that never checks `term.ended`** is cancelled after the grace period. Cancellation
  at an arbitrary await is what the grace period exists to avoid, so the docs show the
  unit-boundary pattern.

## 10. Unresolved questions

- Whether `Term` is a Protocol with a private implementation or a concrete frozen-ish class.
  Implementation decides; the Protocol is the contract.
- Whether the gauge and the counter belong in the existing telemetry module or a new
  leadership one. Implementation decides.

## 11. Decisions

| # | Grade | Decision |
| --- | --- | --- |
| 1 | `LOCKED` | Lifetime leadership is a new step, `leadership_lifecycle_step`, built on the existing dlock ports. `singleton_lifecycle_step` keeps its startup-only semantics. |
| 2 | `LOCKED` | A term ends on the first of: a refused renewal, the deadline passing, a stop, a yield, or the body returning. The step sets `term.ended`, gives the body `yield_grace`, then cancels it and releases the lease, shielded. |
| 3 | `LOCKED` | The deadline is measured from the monotonic time before the acquire or the latest successful renewal was sent, minus `margin`, never from the answer. |
| 4 | `ASSUMED` | `renew_every` defaults to `ttl / 3`, and `renew_every + margin < ttl` is validated at construction. |
| 5 | `ASSUMED` | Handoff uses a second lock, `{key}:want`, on the same backend. A holder whose own `prefer` is true never yields, and non-askers skip campaigning while another replica holds the want-lock. |
| 6 | `ASSUMED` | `verify=True` makes `held()` read the owner through the query port. Off by default, since a read per check is a cost the body opts into. |
| 7 | `ASSUMED` | The fencing token is exposed as `term.token`. A spec that needs one sets `requires_fencing_token`, which already fails closed on a backend without fencing. |
| 8 | `ASSUMED` | With `fence_floor` set, the step advances the fence before each acquire, through RFC 0072's capability, and fails closed when the backend lacks it. |
| 9 | `ASSUMED` | The step is marked `singleton_guarded`, `mutates_shared_state` and `requires_long_running`. |
| 10 | `OPEN` | Whether `Term` is a Protocol or a concrete class, and where its metrics live. |

## 12. Phasing

1. **The step:**
   - the spec;
   - campaign, renewal, term end, grace and release;
   - fencing token exposure, `may_campaign`, `verify`;
   - the metrics;
   - the docs and the recipe.
2. **Handoff on request.**
3. **`fence_floor`,** once RFC 0072 ships.
