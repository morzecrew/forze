# RFC 0073 — Fleet membership

- **Status:** 🚧 In progress
- **Scope:** A contract for knowing which replicas of a service are alive, what state each is
  in, and what each reports. It covers:
  - the contract, with a Redis adapter and a mock;
  - a lifecycle step that keeps a replica's entry current;
  - pure policies over the view: settled, even share, rendezvous rank, and excess over the
    mean;
  - one canonical instance id.

  It does not move work between replicas. Leadership is RFC 0071, the connection rebalancing
  step is RFC 0076, and live re-sharding of `RealtimeShard` is left to a later RFC.
- **Related:**
  - [`src/forze/application/contracts/realtime/shard.py`](../src/forze/application/contracts/realtime/shard.py):
    `RealtimeShard`;
  - [`src/forze_redis/adapters/realtime_presence.py`](../src/forze_redis/adapters/realtime_presence.py):
    the nearest existing shape;
  - [`src/forze_kits/integrations/quiesce/report.py`](../src/forze_kits/integrations/quiesce/report.py);
  - [`pages/docs/recipes/tenant-sharded-realtime.md`](../pages/docs/recipes/tenant-sharded-realtime.md);
  - RFC 0071 (leadership), RFC 0074 (lifecycle state on the context), RFC 0076 (rebalancing).
- **Origin:** hh-backend-v2's fleet proposals (F1, and F6's policy, 2026-10-14). The app keeps
  three membership copies:
  - its `bg` members, a ZSET copied from presence;
  - its socket replicas' load keys, read with SCAN + MGET;
  - its courier's busy key.

  It uses them four ways: socket shedding, table claims, tournament coordinators, and courier
  routes.

---

## 1. Summary

Each replica runs `fleet_membership_lifecycle_step(spec, every=…)`. On every beat, one call:
- removes members whose entry expired, on the server clock;
- writes this replica's entry: its state (`starting`, `serving` or `draining`), when it
  entered that state, and a small report;
- returns the whole view.

Any code can read the view without beating. `FleetView` carries the pure policies apps need,
so every app computes the same answer from the same view:
- `settled(for_)`: the fleet has stopped changing;
- `share(total, me)`: an even split, remainder by name;
- `rank(key)`: rendezvous order, for owning a unit;
- `excess(me, …)`: how far above the mean this replica is.

## 2. Motivation

**forze has no idea who else is running.**
- Instance identity is ad hoc. `forze_socketio` builds `host-pid` for its consumer name
  (`gateway.py:105-113`). The singleton guard takes a required `owner` (`singleton.py:66`).
  OTel's `service.instance.id` is a fresh `uuid4()` (`telemetry/bootstrap.py:94`, `:242`).
- Nothing registers instances, and nothing lists them.

**Its own contracts point at the gap.**
- `RealtimeShard` is "a fixed snapshot, resolved once", and rebalancing "is out of scope —
  repartition by restart" (`shard.py:46-47`). Its example
  `tenants=load_assigned_shard()` names a function that does not exist (`shard.py:43`).
- The sharding recipe says "Assignment, not discovery"
  (`tenant-sharded-realtime.md:64-66`), and nothing assigns.
- The quiesce report says a fleet-wide claim "is a deployment procedure, not something a
  report can make" (`quiesce/report.py:126-130`).

**Apps build it repeatedly.** hh-backend-v2 has three copies of membership and four
consumers, each with its own key shape and its own expiry rule. The pieces under them are the
same:
- who is alive;
- what each reports;
- whether the fleet has settled;
- an even share, or a rank.

## 3. Current state

All verified against the tree at the time of writing.

- **Presence is the nearest shape.** `RedisRealtimePresence` keeps a ZSET per room, scored by
  expiry on the server clock (`realtime_presence.py:39`, `:70`, TTL 90 s). It holds members
  with no payload, and returns only a count.
- **Its scripts** (`forze_redis/kernel/scripts.py:451-493`):
  - `PRESENCE_JOIN` takes `TIME`, then `ZADD now+ttl`, and sets the key's expiry from this
    call's TTL.
  - `PRESENCE_COUNT` prunes with `ZREMRANGEBYSCORE -inf now`, then `ZCARD`.
- **Lua and the client.**
  - All Redis Lua lives in `kernel/scripts.py`, as `Final` constants run through
    `client.run_script`.
  - `run_script` returns `str`, and stringifies a table reply (`port.py:54-58`,
    `client.py:391-406`). Every existing script returns a scalar.
  - `forze_redis` has no cluster-aware client.
- **No rendezvous hashing, consistent hashing or even-split helper exists** in `src/`.
- **`RealtimeShard`** (`shard.py:26-50`) is `stream_spec`, `tenants` (a frozen tuple) and
  `group`, consumed when the steps that use it are built.
- **The runtime** creates the context before lifecycle startup, so `runtime.ready` is true
  before startup finishes (`runtime.py:359-368`, `:537-539`). RFC 0074 moves the lifecycle
  state onto the context, where steps can read it.

## 4. Goals / Non-goals

**Goals**

- One beat both writes this replica's entry and reads the fleet, in one round trip, on the
  server's clock.
- A missing member is one whose entry expired. No separate failure detector.
- A report a member publishes with its entry, small and size-capped.
- Policies that are pure functions of a view, deterministic across processes, so two
  replicas holding the same view always agree.
- One canonical instance id for every subsystem that needs one.

**Non-goals**

- **Moving work.** Membership says who should own what; moving it is the owner's protocol,
  and those differ (5.7).
- **Strong consistency.** Replicas reading at different moments can see different views. The
  policies are deterministic per view, not across views. 5.7 says what that means for
  assignment.
- **Cross-service registries.** A fleet is one service's replicas.
- **Very large fleets.** Each beat returns every entry, so fleet-wide traffic grows with the
  square of the fleet size times the report size. That suits tens to low hundreds of
  replicas.

## 5. Design

### 5.1 The contract

In `forze/application/contracts/fleet/`:

```python
MemberState = Literal["starting", "serving", "draining"]
Report = Mapping[str, int | float | str]


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class FleetSpec(BaseSpec):
    ttl: timedelta = timedelta(seconds=10)
    report_max_bytes: int = 1024


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class Member:
    id: str
    state: MemberState
    since: datetime                      # entered `state`, on the server clock
    report: Report


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class FleetView:
    members: tuple[Member, ...]          # sorted by id
    at: datetime                         # the server's clock when read

    def settled(self, for_: timedelta) -> bool: ...
    def share(self, total: int, me: str) -> int: ...
    def rank(self, key: str, *, states: Collection[MemberState] = ("serving",)) -> tuple[str, ...]: ...
    def excess(self, me: str, *, load: str, movable: str | None = None,
               tolerance: float = 0.15, settle: timedelta, smallest: int = 1) -> int: ...


@runtime_checkable
class FleetPort(Protocol):
    async def beat(self, member_id: str, state: MemberState, report: Report,
                   *, ttl: timedelta | None = None) -> FleetView: ...
    async def view(self) -> FleetView: ...
    async def leave(self, member_id: str) -> None: ...
```

- **No caller-supplied `since`.** `beat` takes the parts a member chooses (id, state, report),
  because `since` is the server's to set.
- **`FleetDepKey`** resolves the port per spec, like the other contracts.
- **Member ids** are checked: no tab, newline or brace characters, at most 256 bytes.

### 5.2 The policies

Each policy is a pure function of the view. `settled`, `share` and `excess` use its
`serving` members; `rank` uses the states it is given.
- **`settled(for_)`** is true when the view has at least one member, every member is serving,
  and each has been serving for at least `for_` by the view's own clock. It is for live
  decisions such as `excess`. Startup assignment does not use it (5.7).
- **`share(total, me)`** splits `total` evenly over the serving members sorted by id. The
  first `total % n` members get one extra. A replica that is not serving gets 0.
- **`rank(key, states=("serving",))`** orders the members in `states` by
  `blake2b(key + "\0" + member_id)`, descending. That is rendezvous hashing: removing a member
  moves only the keys it owned. Startup assignment passes `("serving", "starting")`, so a
  replica that is still joining is a candidate.
  - The hash is stable across processes and interpreters. It is never Python's `hash()`,
    which is salted per process.
  - A unit's owner is `rank(key)[0]`, and the rest are its successors in order.
- **`excess(me, …)`:**
  1. The **mean** is over the serving members that report a numeric `load`.
  2. The result is 0 unless the fleet is `settled(settle)`, `me` is serving and reports
     `load`, and its load exceeds `mean × (1 + tolerance)`. `tolerance` is relative.
  3. In that case, the result is `ceil(load − mean)`, capped by `floor` of the member's
     `movable` report when one is named. The result is always a whole number, and a
     non-numeric `movable` counts as absent.
  4. A result below `smallest` is 0.

  This is the rule hh-backend-v2's socket shedding uses, and tables or tenants can use the
  same one.

### 5.3 The Redis adapter

`forze_redis/adapters/fleet.py` keeps two keys under one hash tag, so a cluster-aware client
reads them in one slot when there is one:
- `{fleet:<ns>:<name>}:z`, a ZSET of member ids scored by expiry in milliseconds;
- `{fleet:<ns>:<name>}:h`, a HASH of member id to `state|since_ms|report_json`.

**`beat`** is one script. Its arguments are the id, the state, the encoded report and the TTL
in milliseconds.
1. Take `TIME`.
2. Read the expired ids with `ZRANGEBYSCORE -inf now`, and remove them from both keys, in
   chunks, skipping an empty `HDEL`.
3. Read this member's stored entry. Keep its `since` if its state prefix is unchanged,
   otherwise use now. This compares strings, so the report is never decoded and re-encoded.
4. Write the entry with `HSET` and the expiry with `ZADD`.
5. Set both keys' expiry with `PEXPIREAT` to the largest member expiry plus a slack. A beat
   with a short TTL, such as a linger, therefore never shortens the life of the keys below
   other members' entries.
6. Return one string: the server time on the first line, then one line per member, as
   `id \t expiry_ms \t entry`.

**`view`** is one read-only script with the same reply, over the unexpired entries. **`leave`**
removes the id from both keys.

- **The report** is JSON-encoded by the adapter, which refuses before sending:
  - a report larger than `report_max_bytes`;
  - a value that is not an int, float or string, including `bool`;
  - a float that is NaN or infinite.
- **The mock adapter** keeps the same model in memory, on an injectable monotonic clock, so
  the conformance battery runs against both.

### 5.4 The lifecycle step

```python
def fleet_membership_lifecycle_step(
    spec: FleetSpec,
    *,
    every: timedelta,
    report: Callable[[], Report] = dict,
    on_stop: Literal["leave", "linger"] = "leave",
    linger_for: timedelta = timedelta(0),
    member_id: str | None = None,                  # default instance_id()
    step_id: StrKey = "fleet_membership",
) -> LifecycleStep: ...
```

- **Beating.** At startup it beats once as `starting`, then beats every `every` from a
  supervised loop registered with `ctx.drainables`.
- **The state** comes from the hook's `ctx`, through RFC 0074's lifecycle state, so the step
  needs no handle on the runtime:
  - `starting` until `ctx.lifecycle.started`;
  - then `serving`;
  - `draining` once `ctx.lifecycle.announced`, or once the drain gate is draining.
- **The interval.** `every` must be below half of `spec.ttl`, so one missed beat does not drop
  the member. This is validated at construction.
- **On stop:**
  - `leave` removes the entry at once, so peers re-rank immediately.
  - `linger` writes one last `draining` entry with a TTL of `linger_for`, then stops beating.
    Peers see a deploy in progress for that long, without delaying this process's exit. The
    keys' own expiry is untouched (5.3, step 5).
- **The markers.**
  - The step is not marked `mutates_shared_state`. Its startup writes only this replica's own
    entry and removes expired ones, which is idempotent and safe from every replica at once.
    The step is meant to run on every replica, so the `FLEET` profile accepts it without a
    singleton guard.
  - It is marked `requires_long_running`.

### 5.5 The instance id

`forze.base.primitives.instance_id()` returns `host:pid:nonce`.
- The nonce is random per process, so a restarted process with a reused pid is a new member.
- It is cached per pid, so a forked child gets its own.

The membership step defaults to it. `forze_socketio`'s consumer default is left alone here,
because changing a consumer name strands its pending entries.

### 5.6 Choosing a member id

The default id changes on every restart, so a rolling deploy reshuffles every unit that
`rank` assigns: each restart removes one id and adds another.

A platform that gives a stable name, such as a StatefulSet pod's, can pass it as `member_id`.
Rank then survives restarts. The cost is that two processes sharing that name, such as a
zombie and its replacement, write one entry between them. The docs give both options, and the
default is the safe one.

### 5.7 What it gives forze, and what it does not

- **Startup assignment for `RealtimeShard`.** The shard is consumed when the steps are built,
  so the replica works with the port before its runtime exists:
  1. It beats once as `starting`, so replicas starting at the same time can see it.
  2. It waits one `ttl`, so their beats are visible too.
  3. It reads the view, and takes the tenants it ranks first for among serving and starting
     members:
     `[t for t in tenants if view.rank(str(t), states=("serving", "starting"))[0] == me]`.

  The first replica of a fresh fleet finds only itself, and takes every tenant. This is a
  snapshot, sound only while the fleet keeps that shape:
  - a replica that dies leaves its tenants unowned until a restart;
  - a replica whose beat lands after another's read can own overlapping or missing tenants.

  The docs say so. Live re-sharding needs RFC 0071's handoff and is a later RFC.
- **A fleet-wide quiesce check** becomes possible: every other member absent or draining. It
  is a follow-up, not part of this RFC.
- **Moving units is not shared.** Closing a socket, handing a table back between hands,
  yielding a lease and moving a tenant's consumer are four protocols. A framework rebalancer
  over them would be one interface with four special cases. Membership and the policies are
  the shared part. RFC 0076 builds the socket case once a second app needs it.

### 5.8 Alternatives considered

- **Extending presence with payloads.** Presence counts connections per room, with a 90 s TTL
  and a socket heartbeat. Membership is per process, with states and a report. Sharing the
  key shape is fine; sharing the contract would couple two lifecycles. Rejected.
- **SCAN + MGET over per-member keys** (hh-backend-v2's load keys). A SCAN is not one atomic
  read, and it walks the keyspace. Rejected for the ZSET and HASH pair.
- **Re-arming the keys' expiry from each beat's own TTL,** as presence does. A short linger
  beat would then expire the whole fleet. Rejected for the largest member expiry.
- **Consistent hashing on a ring.** Rendezvous hashing needs no ring state and moves the same
  minimal set of keys. Chosen for simplicity.

## 6. Tests

- **Conformance, mock and Redis.**
  - A beat removes expired entries, upserts this one, and returns the view in one call.
  - An expired member disappears from the next view.
  - `since` survives beats in the same state and resets on a state change.
  - `leave` removes the member at once.
  - A linger entry expires after `linger_for`, and does not shorten the other members' lives.
  - Refused before sending: an oversized report, a `bool` value, a NaN, and an id with a tab.
  - Report numbers round-trip exactly.
- **Redis integration.**
  - The expiry follows the server's clock: a test client with a skewed local clock sees the
    same view.
  - With 500 members, the reply parses and the expired-id removal works in chunks.
- **Policies.**
  - `share` sums to `total`, and differs by at most one between members.
  - `rank` is stable across processes: run in four subprocesses with different hash seeds,
    the order is the same.
  - Removing a member moves only its keys.
  - `excess` is 0 while unsettled, below tolerance, or below `smallest`, and is capped by
    `movable`.
  - `settled` is false with any `starting` or `draining` member.
  - `rank` with `("serving", "starting")` includes a joining replica, and a one-member fleet
    ranks itself first for every key.
  - `excess` returns a whole number with a fractional `movable` report.
- **The step.**
  - Placed in its own runtime's plan through `build_runtime`, it reports `starting` until
    startup finishes, then `serving`, then `draining` on `begin_draining()`.
  - `every >= ttl / 2` is refused.
  - The `FLEET` profile accepts the step, and `SERVERLESS` refuses it.

## 7. Docs

- A "Fleet membership" page:
  - what a member is, the states, and the report;
  - the policies;
  - choosing a member id;
  - the consistency caveat in 5.7;
  - the fleet size it suits.
- The tenant-sharded realtime recipe gains startup assignment by rank, with the caveat.

## 8. Out of scope

- **Live re-sharding of `RealtimeShard`.** It needs a running shard to be resized and a
  handoff for each tenant. It is a later RFC, on RFC 0071's handoff and this view.
- **The fleet-wide quiesce claim.** It is a follow-up, and needs no change here.
- **Moving units** (5.7), and the connection rebalancing step (RFC 0076).
- **Adopting `instance_id()` across forze.** It is a follow-up, subsystem by subsystem.

## 9. Risks

- **Read as consensus.** Two replicas can hold different views for one beat interval. The
  policies agree per view, not across views. The docs say what that means for anything that
  assigns.
- **A stalled replica keeps its entry** until its TTL passes, beating or not. That is the
  failure-detection latency, and the docs give the `ttl` to `every` trade-off.
- **A cluster client later.** The hash tag keeps both keys in one slot, but `forze_redis` has
  no cluster client today, so the design is forward-compatible only.

## 10. Unresolved questions

- Where `instance_id()` lives: `forze.base.primitives` is the lean.
- Whether the step should also expose the last view it read, saving a `view()` call.
  Implementation decides; the port's `view()` is the contract.

## 11. Decisions

| # | Grade | Decision |
| --- | --- | --- |
| 1 | `LOCKED` | Membership is a contract (`FleetSpec`, `Member`, `FleetView`, `FleetPort`), with a Redis adapter and a mock, and a member is alive exactly while its entry has not expired on the backend's clock. |
| 2 | `LOCKED` | `beat` removes expired entries, upserts this member, and returns the view in one atomic call on the server's clock. `view` reads without writing. |
| 3 | `ASSUMED` | Redis keeps a ZSET of expiries and a HASH of `state\|since_ms\|report_json` entries under one hash tag. Both keys expire at the largest member expiry plus a slack, never from one beat's own TTL. The reply is one line-oriented string. |
| 4 | `ASSUMED` | `since` is when the member entered its current state, kept across beats in the same state by comparing the stored state prefix. Callers never pass it. |
| 5 | `LOCKED` | `share`, `rank`, `settled` and `excess` are pure functions of a view over its `serving` members. `rank` uses a process-independent hash (blake2b), never Python's `hash()`. |
| 6 | `ASSUMED` | Reports are flat maps of int, float or string (no `bool`, no NaN or infinity), capped at `report_max_bytes` (1 KiB by default), and refused before sending. |
| 7 | `ASSUMED` | The step reads its state from the hook's `ctx` (RFC 0074): `starting` until startup finished, then `serving`, then `draining` once announced. It beats at `every < ttl / 2`, and leaves or lingers on stop. |
| 8 | `ASSUMED` | The step is not marked `mutates_shared_state`, because it writes only its own entry and removes expired ones, which is safe from every replica at once. It is marked `requires_long_running`. |
| 9 | `ASSUMED` | `instance_id()` is `host:pid:nonce`, cached per pid, and the default member id. A stable platform name is an option, with its trade-off documented. `forze_socketio`'s consumer default is not changed here. |
| 10 | `LOCKED` | No framework rebalancer: membership and the policies are shared, and moving units stays each owner's protocol. |
| 11 | `OPEN` | Where `instance_id()` lives, and whether the step exposes its last view. |
| 12 | `ASSUMED` | `rank` takes the member states to rank, so startup assignment can include joining replicas, and startup assignment does not wait for `settled`. `excess` takes its mean over serving members that report the load, with a relative tolerance, and returns a whole number. |

## 12. Phasing

1. **The contract and the policies:**
   - the contract;
   - the Redis and mock adapters with the conformance battery;
   - `settled`, `share`, `rank` and `excess`;
   - `instance_id()`;
   - the step and the docs.

   This needs RFC 0074's `ctx.lifecycle` first.
2. **The startup-assignment recipe** for `RealtimeShard`.
