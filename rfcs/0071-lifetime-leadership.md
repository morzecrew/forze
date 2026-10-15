# RFC 0071 — Lifetime leadership

- **Status:** ✅ Complete
- **Scope:** A lifecycle step that elects one replica to run a body for as long as it holds a
  renewing lease. It covers:
  - campaigning, renewal and release;
  - a `Term` that tells the body when its lease is lost, about to expire, yielded or stopped;
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
and runs `lead(ctx, term)` on the one that holds it.
- **Renewal.** It renews the lease while the body runs.
- **Ending a term.** It ends the term early enough that the body's grace period finishes before
  the lease could lapse.
- **A lost lease.** It cancels the body at once when the lease is known to be lost.
- **Release.** It releases the lease only once the body has finished, so a successor leads
  within one campaign interval rather than a full TTL.

Each term uses its own owner string, so a late release or renewal from an old term can never
touch a newer holder's lease. `term.token` carries the lock's fencing token for the body's
writes. With `handoff="on_request"`, a replica that prefers the unit asks for it, and the
holder yields at its next renewal.

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
- **The Redis backend.**
  - It acquires with `SET NX PX` and draws the token from a separate `INCR` counter
    (`forze_redis/kernel/scripts.py:5-21`).
  - `reset` re-arms the TTL (`forze_redis/adapters/dlock.py:150-157`).
  - `release` and `reset` compare only the owner string (`scripts.py:24-44`). Two terms that
    share an owner string are therefore indistinguishable to Redis.
  - The expiry is stored as an absolute wall-clock time.
- **The singleton guard** (`singleton.py:29-117`) ignores the token, never renews and never
  retries.
- **The scope** yields `AcquiredLock`, whose token is stable across `reset`. It exposes no
  event or callback for a lost lock (`scopes/dlock.py:40-54`, `:148`).
- **Loop primitives.**
  - `BackgroundLoopControl.stop` gives a loop `min(stop_grace, deadline)` before cancelling
    it, and `stop_grace` defaults to 5 s (`loop.py:45`, `:152`).
  - `run_supervised` restarts with a jittered backoff, and treats a configuration error as
    terminal (`supervise.py:124`).
  - Loops registered with `ctx.drainables` are stopped by the runtime before teardown
    (`runtime.py:462`).
- **Step markers.**
  - `mutates_shared_state` means the step's *startup* writes shared infrastructure.
    `singleton_guarded` means that startup runs on one replica at a time
    (`contracts/execution/value_objects.py:208-221`).
  - The `FLEET` profile refuses a mutating step without the guard (`runtime.py:269-281`).
  - `SERVERLESS` refuses `requires_long_running`.
- **No other leader election exists** in `src/`. The closest are per-run durable leases
  (`run_store.py:234-255`) and in-process single-flight lanes, which are unrelated.
- **dlock has no shared conformance battery,** only per-adapter tests
  (`tests/unit/test_forze_mock/test_mock_dlock.py`,
  `tests/unit/test_forze_redis/test_dlock_adapter.py`).

## 4. Goals / Non-goals

**Goals**

- A responsive leader runs its body only while it holds the lease. That holds as long as
  Redis's clock does not jump forward by more than `margin` (5.4). Fencing covers leaders that
  are not responsive, and backends that lost their data.
- The body learns that its term is ending, without polling the backend.
- A stopping leader hands over within `yield_grace + release_timeout + campaign_every` of
  the stop. If its body cannot be confirmed done, it hands over within the lease TTL plus one
  campaign interval. A dead leader hands over within the lease TTL plus one campaign
  interval.
- An opt-in handoff, so a preferred replica can take a unit back without a restart, and that
  cannot leave the fleet without a leader.
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
    margin: timedelta = timedelta(seconds=1)  # allowance for Redis's clock and the network
    yield_grace: timedelta = timedelta(seconds=5)
    release_timeout: timedelta = timedelta(seconds=1)
    verify: bool = False                      # Term.held() also reads the owner


def leadership_lifecycle_step(
    spec: LeadershipSpec,
    *,
    owner: str,                               # a prefix; each term appends its own nonce
    lead: Callable[[ExecutionContext, Term], Awaitable[None]],
    may_campaign: Callable[[ExecutionContext], Awaitable[bool]] | None = None,
    handoff: Literal["none", "on_request"] = "none",
    prefer: Callable[[ExecutionContext], Awaitable[bool]] | None = None,
    fence_floor: Callable[[ExecutionContext], Awaitable[int]] | None = None,
    claim: Callable[[ExecutionContext, Term], Awaitable[bool]] | None = None,
    step_id: StrKey | None = None,            # default f"leadership:{spec.key}"
) -> LifecycleStep: ...
```

- **Validation, when the spec is built:**
  - `renew_every + yield_grace + margin < lock.ttl`, so a term's grace period ends before the
    lease could lapse;
  - `campaign_every < lock.ttl`;
  - `handoff="on_request"` requires `prefer`.
- **Startup checks.** When `fence_floor` is given, the step checks the backend for RFC 0072's
  capability at startup. A missing capability then fails assembly, rather than becoming a
  configuration error that would stop the supervised loop for good while the process stays
  up.
- **The loop.** The step runs one campaign loop under `BackgroundLoopControl` and
  `run_supervised`, registered with `ctx.drainables`. Its `stop_grace` is set to
  `yield_grace + release_timeout` plus a second, so stopping it can finish the term's grace
  and its release.
- **The markers.** Only `requires_long_running=True`, which keeps the step off `SERVERLESS`.
  - Its startup runs on every replica and writes nothing shared; it only starts the campaign
    loop.
  - `mutates_shared_state` and `singleton_guarded` describe startup, so neither applies.
  - The body's exclusivity is this step's own guarantee, not a marker's.

### 5.2 The term

```python
class Term(Protocol):
    token: int | None                   # the lock's fencing token, stable for the term
    ended: asyncio.Event                # set once, by the step
    reason: TermEnd | None              # "lost" | "expiring" | "stopped" | "yielded" | "returned" | "raised" | "unclaimed"
    def live(self) -> bool: ...         # not ended; no I/O
    async def held(self) -> bool: ...   # live(), and with verify, the backend still names this term
```

**Owners are per term.** Each acquire uses `f"{owner}:{nonce}"`, with a fresh random nonce.
Every reset, release and want-lock call of that term uses the same string.
- A late release from an old term therefore never deletes a successor's lease, even if both
  replicas were given the same `owner`, or a replacement pod reuses a name.
- A zombie's renewals fail against the new holder's lease.

**Two deadlines,** both measured from `t_sent`, the monotonic time just before the acquire or
the latest successful renewal was sent:
- the **hard deadline**, `t_sent + lock.ttl - margin`, by which the lease may be gone;
- the **soft deadline**, `hard - yield_grace`, by which the body must be told.

Neither is measured from the answer. A renewal that took long to come back therefore shortens
the term, and never lengthens it.

**How a term ends:**

| Reason | When | The body |
| --- | --- | --- |
| `lost` | a renewal answered `False`: the lease is already gone | cancelled at once; no grace |
| `expiring` | the soft deadline passed without a successful renewal | grace until the hard deadline, then cancelled |
| `stopped` | the step is stopping | `yield_grace`, then cancelled |
| `yielded` | a handoff (5.5) | `yield_grace`, then cancelled |
| `returned` | the body returned on its own | already done |
| `raised` | the body raised | already done; `run_supervised` backs off before the next campaign |

- **Renewals continue** through the grace of a `stopped` or `yielded` term. The lease stays the
  step's until the body is done, so a successor cannot overlap it.
- **Release follows the body.** The step releases only after the body's task has finished, or
  has been cancelled and awaited. If the cancelled body does not finish within
  `release_timeout`, the step does not release. The lease then lapses at its TTL, and a
  successor cannot start while the old body may still run.

### 5.3 Campaigning and renewal

- **Campaigning.** Every `campaign_every`, a replica that is not stopping, whose `may_campaign`
  allows it, and that is not deferring to a handoff (5.5) does three things:
  1. With `fence_floor` set and the lease free (`is_locked` is false), it advances the fence
     first (RFC 0072). It does not read the store's fence on every tick.
  2. It tries `acquire` with a fresh per-term owner.
  3. On success, it starts a term.
  4. With `claim` set, it calls `claim(ctx, term)` before `lead`. This is the store's claim of
     the term's fencing token (RFC 0072 §5.4). `False` means another holder already claimed
     an equal token: the term ends as `unclaimed`, the step releases, and it campaigns again.
  5. It runs `lead` in a task.
- **A late acquire.** An acquire that has not answered in time is not cancelled, because a
  cancelled request may still land. The step stops waiting for it and lets it run to
  completion in the background. If it then returns a lock the step no longer wants, the step
  releases it with that term's owner. A request that lands after the step stopped waiting can
  therefore never strand a lease for a TTL.
- **Renewal.** Every `renew_every`, it calls `reset` with the term's owner.
  - `True` moves both deadlines.
  - `False` ends the term as `lost`.
  - An error or a timeout leaves the deadlines where they were.

  Each renewal is bounded by a timeout shorter than the time left before the soft deadline.
- **Stop.** The step ends the term as `stopped`, keeps renewing through the grace period, waits
  for the body as above, releases, and returns. A campaigning successor takes over within
  `campaign_every`.

### 5.4 What it guarantees, and what fencing is for

- **A responsive leader whose body honours cancellation runs it only while it holds the
  lease.**
  - A `lost` term cancels the body at once.
  - Every other end leaves the body's grace inside the hard deadline. The validation in 5.1
    makes that possible.
  - The step releases only after the body is done.
- **A body that resists cancellation** past the hard deadline is not stopped by the lease. The
  step does not release it, but the lease expires at its TTL, and a successor can then run
  alongside it. Fencing covers that case, as it covers a paused process.

  This assumes Redis's clock does not jump forward by more than `margin` while the lease runs.
  Redis stores the expiry as an absolute wall-clock time, so an NTP step or a failover to a
  replica with a faster clock ends the lease early. `margin` is sized for the deployment's
  clock discipline, and the docs say so.
- **After a `lost` answer,** the body may still run until its cancellation lands, while a
  successor already holds the lease. Fencing covers that gap.
- **A leader that is not responsive is not stopped by any of this:** a paused process, a
  blocked event loop, a long GC. Neither is one whose lock backend lost its data, so the lease
  is gone while the leader cannot yet know it. Those are what the fencing token is for.
  - The body passes `term.token` with each write it makes as leader.
  - The store refuses a token lower than the highest it has seen. This is the contract's rule,
    stated the same way in its example since the bug-fix branch.
- **Requiring fencing.** A spec that needs a token sets `lock.requires_fencing_token`, and a
  backend without one fails closed, as today.
- **After Redis loses its data,** the fence counter restarts low. RFC 0072's fence advance, via
  `fence_floor`, moves it to a new epoch above every earlier token before the step campaigns.
- **`verify=True`** makes `held()` also read the owner through the query port, and compare it
  with the term's per-term owner. A body can then check before an expensive or irreversible
  unit, at the cost of one read.

### 5.5 Handoff on request

With `handoff="on_request"`, a replica that does not hold the lease, whose `prefer` returns
true and whose `may_campaign` allows it, asks for the lease by holding a second lock on the
same backend, `{key}:want`. It acquires the want-lock and renews it on each campaign tick.

- **The holder** reads the want-lock's owner at each renewal. If another replica holds it, and
  the holder's own `prefer` is false, it ends its term as `yielded`. It then releases once the
  body is done (5.2). A holder that prefers the unit itself never yields, so two preferring
  replicas cannot pass the unit back and forth.
- **Other replicas** skip their campaign while another replica holds the want-lock, with one
  exception. If the lease has been free for longer than three campaign intervals, they
  campaign anyway. An asker that cannot acquire therefore never leaves the fleet without a
  leader.
- **The asker** releases the want-lock:
  - once it holds the lease;
  - when it stops;
  - after three consecutive campaigns that did not get the lease.

  If it dies, the want-lock expires within its TTL.

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
- **One owner string per replica, as the caller gives it.** That is the first draft. Redis
  compares only the owner string, so a shared or reused name lets an old term's release or
  renewal act on a newer holder's lease. Rejected for per-term owners.
- **A dedicated leadership port with a Raft-like backend.** The guarantee is bounded by the
  lock backend either way, and every deployment already has the dlock ports. Rejected until
  a deployment needs more than lease plus fencing.

## 6. Tests

Unit tests run against the mock dlock with a controllable clock. Integration tests run against
Redis.
- **One leader.** Three replicas campaign, and exactly one runs `lead`.
- **Takeover.**
  - The leader stops: it releases after its body finishes, and a successor leads within
    `campaign_every`.
  - The leader is killed without releasing: a successor leads within `ttl + campaign_every`.
- **No overlap with the backend intact:**
  - Make renewals hang. The term ends as `expiring` at the soft deadline, and the body is gone
    before the hard deadline. A successor never starts before it.
  - A body that ignores `term.ended` is cancelled at the hard deadline.
  - With a cancelled body that will not finish, the step does not release, and no successor
    starts until the TTL.
- **Losing the lease.** Delete the lease key under a running term. The term ends as `lost` at
  the next renewal, and the body is cancelled at once.
- **Per-term owners.**
  - Two replicas given the same `owner` never both lead.
  - An old term's late release does not delete the new holder's lease.
- **A late acquire.** An acquire that answers after the step stopped waiting is released, and
  the fleet elects a leader within one campaign interval.
- **Claims.** Two terms holding an equal token (after a backend loss) both claim. One runs
  `lead`, and the other ends as `unclaimed`.
- **A body that resists cancellation** keeps the lease until its TTL, and no successor starts
  before then.
- **Fencing.** Tokens grow strictly across terms, and `term.token` is stable within one.
- **Handoff.**
  - A preferring replica takes the unit from a non-preferring holder at the holder's next
    renewal.
  - Two preferring replicas do not pass the unit back and forth.
  - An asker that `may_campaign` refuses never takes the want-lock.
  - An asker that cannot acquire releases the want-lock after three tries, and the fleet still
    elects a leader.
- **Configuration.** A `fence_floor` without the capability fails at assembly. Validation
  refuses `renew_every + yield_grace + margin >= ttl`.
- **Bounded doubles.** Every retry loop's double is a bounded script, so a lost loop bound
  fails in milliseconds.

## 7. Docs

- A "Run one replica's worth of work" section on the lifecycle page:
  - when to use this step, and when the singleton guard is enough;
  - what fencing is for;
  - the clock assumption behind `margin`.
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

- **Read as consensus.** The docs say what the term guarantees: a responsive leader stays
  inside its lease, under the clock assumption. Everything else is covered by fencing.
  Bodies that skip fencing are told what they are giving up.
- **Short TTLs and slow backends.** A TTL close to the backend's latency makes terms flap. The
  validation enforces the arithmetic, and the docs suggest a TTL of at least ten times the
  renewal latency.
- **A body that never checks `term.ended`** is cancelled at its deadline. Cancellation at an
  arbitrary await is what the grace period exists to avoid, so the docs show the unit-boundary
  pattern.
- **A configuration error in the body** stops the supervised loop for good, as it does for any
  supervised loop, while the process stays up. The step logs it as such, and the docs say so.

## 10. Unresolved questions

- Whether `Term` is a Protocol with a private implementation or a concrete class.
  Implementation decides; the Protocol is the contract.
- Whether the gauge and the counter belong in the existing telemetry module or a new
  leadership one. Implementation decides.

## 11. Decisions

| # | Grade | Decision |
| --- | --- | --- |
| 1 | `LOCKED` | Lifetime leadership is a new step, `leadership_lifecycle_step`, built on the existing dlock ports. `singleton_lifecycle_step` keeps its startup-only semantics. |
| 2 | `LOCKED` | A term is told it is ending by a soft deadline, `hard − yield_grace`, so its grace finishes inside the lease. A `lost` term cancels the body at once. Renewals continue through the grace of a stopped or yielded term, and the lease is released only after the body is done. If the body cannot be confirmed done, the lease is left to expire. |
| 3 | `LOCKED` | Both deadlines are measured from the monotonic time before the acquire or the latest successful renewal was sent, never from the answer. The hard deadline is `t_sent + ttl − margin`, and `margin` absorbs Redis clock jumps. |
| 4 | `ASSUMED` | `renew_every` defaults to `ttl / 3`. `renew_every + yield_grace + margin < ttl` and `campaign_every < ttl` are validated at construction. |
| 5 | `ASSUMED` | Handoff uses a second lock, `{key}:want`, taken only when `may_campaign` allows it. A holder whose own `prefer` is true never yields. Non-askers defer to the want-lock unless the lease has been free for three campaign intervals. The asker releases the want-lock on acquiring, on stop, and after three failed campaigns. |
| 6 | `ASSUMED` | `verify=True` makes `held()` read the owner and compare it with the term's own. Off by default. |
| 7 | `ASSUMED` | The fencing token is exposed as `term.token`. A spec that needs one sets `requires_fencing_token`, which already fails closed on a backend without fencing. |
| 8 | `ASSUMED` | With `fence_floor`, the step advances the fence before acquiring, only while the lease is free, through RFC 0072's capability. The capability is checked at startup, so a missing one fails assembly. |
| 9 | `ASSUMED` | The step is marked only `requires_long_running`. Its startup writes nothing shared and runs on every replica; the body's exclusivity is the step's guarantee. |
| 10 | `OPEN` | Whether `Term` is a Protocol or a concrete class, and where its metrics live. |
| 11 | `LOCKED` | Every term uses its own owner string (`owner` plus a nonce) for acquire, reset, release and the want-lock, because Redis compares only the owner string. |
| 12 | `ASSUMED` | An acquire that has not answered in time runs to completion in the background instead of being cancelled. A lock it returns after the step stopped waiting is released with that term's owner, so a late acquire cannot strand a lease. |
| 13 | `ASSUMED` | With `claim` set, the step claims the term's fencing token in the store before running `lead` (RFC 0072 §5.4), and ends the term as `unclaimed` when another holder already has it. |

## 12. Phasing

1. **The step:**
   - the spec and its validation;
   - per-term owners;
   - campaign, renewal, both deadlines, the grace rules and release-after-body;
   - fencing token exposure, `may_campaign` and `verify`;
   - the metrics;
   - the docs and the recipe.
2. **Handoff on request.**
3. **`fence_floor`,** once RFC 0072 ships.
