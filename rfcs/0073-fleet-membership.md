# RFC 0073 — Fleet membership

- **Status:** 📝 Draft
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
  - RFC 0071 (leadership), RFC 0074 (drain announcement), RFC 0076 (rebalancing).
- **Origin:** hh-backend-v2's fleet proposals (F1, and F6's policy, 2026-10-14). The app keeps
  three membership copies:
  - its `bg` members, a ZSET copied from presence;
  - its socket replicas' load keys, read with SCAN + MGET;
  - its courier's busy key.

  It uses them four ways: socket shedding, table claims, tournament coordinators, and courier
  routes.

---

## 1. Summary

Each replica runs `fleet_membership_lifecycle_step(spec, runtime=…, every=…)`. On every beat,
one call:
- prunes members whose entry expired, on the server clock;
- writes this replica's entry: its id, its state (`starting`, `serving` or `draining`), when
  it entered that state, and a small report;
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
  expiry on the server clock (`realtime_presence.py:39`, `:70`, TTL 90 s). Its scripts:
  - `PRESENCE_JOIN` takes `TIME`, then `ZADD now+ttl`;
  - `PRESENCE_COUNT` prunes with `ZREMRANGEBYSCORE -inf now`, then `ZCARD`.

  They live in `forze_redis/kernel/scripts.py:451-491`. It holds members with no payload, and
  returns only a count.
- **Lua and the clock.** All Redis Lua lives in `kernel/scripts.py`, as `Final` constants run
  through `client.run_script`. Server `TIME` is used for presence, rate limits and breakers.
- **No rendezvous hashing, consistent hashing or even-split helper exists** in `src/`.
- **`RealtimeShard`** (`shard.py:26-50`) is `stream_spec`, `tenants` (a frozen tuple) and
  `group`. It is consumed by the socket.io sharded source, the realtime group-ensure and relay
  steps, and the SSE sharded tail. The outbox relay and the durable steps take a `tenants`
  provider frozen at startup.
- **The runtime** exposes `ready` and `draining` (`runtime.py:340-368`). Before RFC 0074, both
  change only inside `shutdown()`.
- **Step markers.** The `FLEET` profile refuses a step that mutates shared state without
  `singleton_guarded` (`runtime.py:269-296`).

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
  and those differ (5.6).
- **Strong consistency.** Replicas reading at different moments can see different views. The
  policies are deterministic per view, not across views. 5.5 says what that means for
  assignment.
- **Cross-service registries.** A fleet is one service's replicas.

## 5. Design

### 5.1 The contract

In `forze/application/contracts/fleet/`:

```python
@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class FleetSpec(BaseSpec):
    ttl: timedelta = timedelta(seconds=10)
    report_max_bytes: int = 1024


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class Member:
    id: str
    state: Literal["starting", "serving", "draining"]
    since: datetime                      # entered `state`, on the server clock
    report: Mapping[str, int | float | str] = attrs.field(factory=dict)


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class FleetView:
    members: tuple[Member, ...]          # sorted by id
    at: datetime                         # the server's clock when read

    def settled(self, for_: timedelta) -> bool: ...
    def share(self, total: int, me: str) -> int: ...
    def rank(self, key: str) -> tuple[str, ...]: ...
    def excess(self, me: str, *, load: str, movable: str | None = None,
               tolerance: float = 0.15, settle: timedelta, smallest: int = 1) -> int: ...


@runtime_checkable
class FleetPort(Protocol):
    async def beat(self, member: Member, *, ttl: timedelta | None = None) -> FleetView: ...
    async def view(self) -> FleetView: ...
    async def leave(self, member_id: str) -> None: ...
```

A `FleetDepKey` resolves the port per spec, like the other contracts.

### 5.2 The policies

Each policy is a pure function of the view, using only `serving` members.
- **`settled(for_)`** is true when the view has at least one member, every member is
  `serving`, and each has been serving for at least `for_` by the view's own clock (`at`).
  Nobody acts on a fleet that is still starting or draining.
- **`share(total, me)`** splits `total` evenly over the serving members sorted by id. The
  first `total % n` members get one extra. A replica that is not serving gets 0.
- **`rank(key)`** orders the serving members by `blake2b(key + "\0" + member_id)`,
  descending. That is rendezvous hashing: removing a member moves only the keys it owned.
  - The hash is stable across processes and interpreters. It is never Python's `hash()`,
    which is salted per process.
  - A unit's owner is `rank(key)[0]`, and the rest are its successors in order.
- **`excess(me, …)`** is 0 unless all of these hold:
  - the fleet is `settled(settle)`;
  - `me` is serving and reports `load`;
  - its load exceeds the mean by more than `tolerance`.

  Otherwise it is the load above the mean, rounded up, capped by the member's `movable`
  report when given. It is 0 when the result is below `smallest`. This is the rule
  hh-backend-v2's socket shedding uses, and tables or tenants can use the same one.

### 5.3 The Redis adapter

`forze_redis/adapters/fleet.py` keeps two keys under one hash tag, so a cluster-aware client
reads them in one slot:
- `{ns}:fleet:{name}`: a ZSET of member ids, scored by expiry in milliseconds;
- `{ns}:fleet:{name}:m`: a HASH of member id to its encoded entry.

**`beat`** is one script:
1. Take `TIME`.
2. Remove expired ids from both keys.
3. Keep `since` from the stored entry if the state is unchanged, otherwise use now.
4. Write the entry with `HSET`, and its expiry with `ZADD`.
5. Re-arm both keys' expiry, so an abandoned fleet disappears.
6. Return now and every entry.

**`view`** is one read-only script that takes `TIME` and returns the unexpired entries.
**`leave`** removes the id from both keys.

- **The report** is encoded as JSON. The adapter refuses one larger than `report_max_bytes`,
  or with a value that is not an int, float or string, before sending.
- **The mock adapter** keeps the same model in memory, on the runtime's clock, so the
  conformance battery runs against both.

### 5.4 The lifecycle step

```python
def fleet_membership_lifecycle_step(
    spec: FleetSpec,
    *,
    runtime: ExecutionRuntime,
    every: timedelta,
    report: Callable[[], Mapping[str, int | float | str]] = dict,
    on_stop: Literal["leave", "linger"] = "leave",
    linger_for: timedelta = timedelta(0),
    member_id: str | None = None,                  # default instance_id()
    step_id: StrKey = "fleet_membership",
) -> LifecycleStep: ...
```

- **Beating.** At startup it beats once as `starting`, then beats every `every` from a
  supervised loop registered with `ctx.drainables`.
- **The state** comes from the runtime:
  - `starting` until `runtime.ready` is first true;
  - then `serving`;
  - `draining` once `runtime.draining` is true. With RFC 0074, that is from
    `begin_draining()`, before teardown starts.
- **The interval.** `every` must be below half of `spec.ttl`, so one missed beat does not drop
  the member. This is validated at construction.
- **On stop:**
  - `leave` removes the entry at once, so peers re-rank immediately.
  - `linger` writes one last `draining` entry that expires after `linger_for`, then stops
    beating. Peers see a deploy in progress for that long, without delaying this process's
    exit.
- **The markers.** The step is not marked `mutates_shared_state`. Each replica writes only its
  own entry, and the step is meant to run on every replica, so the `FLEET` profile accepts it
  without a singleton guard. It is marked `requires_long_running`.

### 5.5 The instance id

`forze.base.primitives.instance_id()` returns `host:pid:nonce`.
- The nonce is random per process, so a restarted process with a reused pid is a new member.
- It is cached per pid, so a forked child gets its own.

The membership step defaults to it. Other subsystems may adopt it later. `forze_socketio`'s
consumer default is left alone here, because changing a consumer name strands its pending
entries.

### 5.6 What it gives forze, and what it does not

- **Startup assignment for `RealtimeShard`.** A replica can take
  `[t for t in tenants if view.rank(str(t))[0] == me]` once the fleet is settled. That is
  sound only while the fleet keeps that shape. A replica that dies leaves its tenants unowned
  until a restart. Replicas that start against different views can briefly own overlapping
  or missing shards. The docs say so, and live re-sharding, which needs RFC 0071's handoff,
  is a later RFC.
- **A fleet-wide quiesce check** becomes possible: every other member absent or draining. It
  is a follow-up, not part of this RFC.
- **Moving units is not shared.** Closing a socket, handing a table back between hands,
  yielding a lease and moving a tenant's consumer are four protocols. A framework rebalancer
  over them would be one interface with four special cases. Membership and the policies are
  the shared part. RFC 0076 builds the socket case once a second app needs it.

### 5.7 Alternatives considered

- **Extending presence with payloads.** Presence counts connections per room, with a 90 s
  TTL and a socket heartbeat. Membership is per process, with states and a report. Sharing
  the key shape is fine; sharing the contract would couple two lifecycles. Rejected.
- **SCAN + MGET over per-member keys** (hh-backend-v2's load keys). A SCAN is not one atomic
  read, and it walks the keyspace. Rejected for the ZSET and HASH pair.
- **Consistent hashing on a ring.** Rendezvous hashing needs no ring state and moves the same
  minimal set of keys. Chosen for simplicity.

## 6. Tests

- **Conformance, mock and Redis.**
  - A beat prunes, upserts and returns the view in one call.
  - An expired member disappears from the next view.
  - `since` survives beats in the same state and resets on a state change.
  - `leave` removes the member at once, and a linger entry expires after `linger_for`.
  - An oversized or ill-typed report is refused.
- **Redis integration.** The expiry follows the server's clock: a test client with a skewed
  local clock sees the same view.
- **Policies.**
  - `share` sums to `total`, and differs by at most one between members.
  - `rank` is stable across processes: run in four subprocesses with different hash seeds,
    the order is the same.
  - Removing a member moves only its keys.
  - `excess` is 0 while unsettled, below tolerance, or below `smallest`, and is capped by
    `movable`.
  - `settled` is false with any `starting` or `draining` member.
- **The step.**
  - The state moves from `starting` to `serving` to `draining` with the runtime.
  - `every >= ttl / 2` is refused.
  - The `FLEET` profile accepts the step, and `SERVERLESS` refuses it.

## 7. Docs

- A "Fleet membership" page: what a member is, the states, the report, the policies, and the
  consistency caveat in 5.6.
- The tenant-sharded realtime recipe gains startup assignment by rank, with the caveat.

## 8. Out of scope

- **Live re-sharding of `RealtimeShard`.** It needs a running shard to be resized and a
  handoff for each tenant. It is a later RFC, on RFC 0071's handoff and this view.
- **The fleet-wide quiesce claim.** It is a follow-up, and needs no change here.
- **Moving units** (5.6), and the connection rebalancing step (RFC 0076).
- **Adopting `instance_id()` across forze.** It is a follow-up, subsystem by subsystem.

## 9. Risks

- **Read as consensus.** Two replicas can hold different views for one beat interval. The
  policies agree per view, not across views. The docs say what that means for anything that
  assigns.
- **A large fleet in one HASH.** A view is one read of every member. Hundreds of members with
  1 KiB reports is a few hundred KiB per beat. The docs bound it, and fleets far larger are
  out of scope.
- **A stalled replica keeps its entry** until its TTL passes, beating or not. That is the
  failure-detection latency, and the docs give the `ttl` to `every` trade-off.

## 10. Unresolved questions

- Where `instance_id()` lives: `forze.base.primitives` is the lean.
- Whether the step should also expose the last view it read, saving a `view()` call.
  Implementation decides; the port's `view()` is the contract.

## 11. Decisions

| # | Grade | Decision |
| --- | --- | --- |
| 1 | `LOCKED` | Membership is a contract (`FleetSpec`, `Member`, `FleetView`, `FleetPort`), with a Redis adapter and a mock, and a member is alive exactly while its entry has not expired on the backend's clock. |
| 2 | `LOCKED` | `beat` prunes, upserts and returns the view in one atomic call on the server's clock. `view` reads without writing. |
| 3 | `ASSUMED` | Redis keeps a ZSET of expiries and a HASH of entries under one hash tag, and re-arms both keys' expiry on every beat. |
| 4 | `ASSUMED` | `since` is when the member entered its current state, kept across beats in the same state. |
| 5 | `LOCKED` | `share`, `rank`, `settled` and `excess` are pure functions of a view over its `serving` members. `rank` uses a process-independent hash (blake2b), never Python's `hash()`. |
| 6 | `ASSUMED` | Reports are flat maps of int, float or string, capped at `report_max_bytes` (1 KiB by default) and refused before sending. |
| 7 | `ASSUMED` | The step takes its state from the runtime (`starting`, then `serving` once ready, then `draining` once draining), beats at `every < ttl / 2`, and leaves or lingers on stop. |
| 8 | `ASSUMED` | The step is not marked `mutates_shared_state`, because each replica writes only its own entry and the step is meant for every replica. It is marked `requires_long_running`. |
| 9 | `ASSUMED` | `instance_id()` is `host:pid:nonce`, cached per pid. `forze_socketio`'s consumer default is not changed here. |
| 10 | `LOCKED` | No framework rebalancer: membership and the policies are shared, and moving units stays each owner's protocol. |
| 11 | `OPEN` | Where `instance_id()` lives, and whether the step exposes its last view. |

## 12. Phasing

1. **The contract and the policies:**
   - the contract;
   - the Redis and mock adapters with the conformance battery;
   - `settled`, `share`, `rank` and `excess`;
   - `instance_id()`;
   - the step and the docs.
2. **The startup-assignment recipe** for `RealtimeShard`.
