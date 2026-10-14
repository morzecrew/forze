# RFC 0076 — Connection rebalancing step

- **Status:** 📝 Draft — demand-gated: built when a second realtime app needs it.
- **Scope:** A socket.io lifecycle step that sheds a replica's movable connections while it is
  well above the fleet's mean, so a fleet that drifted out of balance recovers without a
  restart. It composes three earlier RFCs:
  - fleet membership and its `excess` policy (RFC 0073);
  - spread closes (RFC 0075);
  - the draining state (RFC 0074).

  It is socket-only. A framework-wide rebalancer across sockets, tables, routes and shards is
  rejected.
- **Related:**
  - RFC 0073 (fleet membership, `FleetView.excess`);
  - RFC 0075 (`close_spread`);
  - RFC 0074 (`runtime.draining`);
  - [`src/forze/application/contracts/realtime/shard.py`](../src/forze/application/contracts/realtime/shard.py),
    whose docstring puts rebalancing out of scope.
- **Origin:** hh-backend-v2's fleet proposals (F6, 2026-10-14). The app's rebalancer
  (`rebalance.py`, 212 LOC) is four layers:
  - a fleet view;
  - a settle gate;
  - a policy that sheds down to the mean when more than 15% above it;
  - a spread close.

  The first three are RFC 0073's, and the last is RFC 0075's.

---

## 1. Summary

```python
connection_rebalance_lifecycle_step(
    sio, *, fleet: FleetSpec, runtime, every, load="connections", movable_report="movable",
    movable: Callable[[str, str], bool], per_minute: int, tolerance=0.15,
    settle=timedelta(seconds=120),
)
```

Every `every`, while this replica is serving and the fleet has settled, the step does three
things:
1. It reads the view.
2. It asks `view.excess(me, load=…, movable=…, tolerance=…, settle=…)` how many connections to
   shed, capped by `per_minute`.
3. It closes that many of its movable connections with `close_spread` over the interval.

The clients reconnect through the balancer, which places them on a less-loaded replica.

## 2. Motivation

**A realtime fleet drifts out of balance.**
- After a deploy, the replica that restarted last holds few connections, and the others hold
  the rest until clients reconnect on their own.
- A long-lived socket never moves by itself, so the imbalance can last hours.

**What hh-backend-v2 does.** It sheds lobby sockets from a replica that is more than 15% above
the mean, at a bounded rate, only once every replica has been serving for 120 s. It never
sheds game sockets, whose clients would lose a hand.

**Why this is not built yet.** forze has one realtime app that needs it. A least-connections
balancer makes most of it unnecessary, because new connections go to the emptiest replica.
The layers under it (membership, the policy, spread closes) are useful on their own, so they
ship first, and this step waits for a second consumer.

## 3. Current state

- `RealtimeShard` says "rebalancing a running fleet is out of scope — repartition by restart"
  (`shard.py:46-47`).
- Nothing in forze closes connections to move them. The only `disconnect` call is the expiry
  sweep (`forze_socketio/connection.py:716`).
- Membership, `excess` and `close_spread` do not exist until RFCs 0073 and 0075 ship.

## 4. Goals / Non-goals

**Goals**

- Shed only connections the app marks movable, at a bounded rate, only from a replica well
  above the mean, and only once the fleet has settled.
- Use the same `excess` rule an app would use for tables or tenants.

**Non-goals**

- **A framework rebalancer.** Moving a socket, a table, a lease or a tenant's consumer are
  four protocols. One interface over them would be four special cases.
- **Choosing where clients go.** That is the balancer's.
- **Live re-sharding of `RealtimeShard`.** It needs a handoff per tenant (RFC 0071) and its own
  RFC.

## 5. Design

### 5.1 The step

- **Every `every`:**
  1. If `runtime.draining` is true, do nothing. A draining replica closes everything through
     RFC 0075 instead.
  2. Read the fleet view (RFC 0073).
  3. `n = view.excess(me, load=load, movable=movable_report, tolerance=tolerance,
     settle=settle)`. That is 0 unless the fleet has settled, this replica is serving, and its
     load is more than `tolerance` above the mean.
  4. Cap `n` at `per_minute * every / 60`.
  5. Pick `n` of this replica's connections for which `movable(namespace, sid)` is true, and
     close them with `close_spread` over `every`.
- **The report.** The load and the movable counts come from this replica's own membership
  report, which the app fills: its connection count, and how many are movable. A replica that
  does not report them is never shed from.
- **The settle gate keeps the fleet stable.** Nobody sheds while any replica is starting,
  draining, or has been serving for less than `settle`, and that includes a replica that has
  just shed. So two replicas cannot shed into each other in a loop, and a deploy never
  triggers shedding.

### 5.2 Alternatives considered

- **A generic `Rebalancer` interface** taking an actuator per unit kind. Rejected in RFC 0073
  (§5.6), for the four-protocols reason.
- **Moving clients by telling them where to go** (a redirect event). It needs client
  cooperation and a way to choose the target, which is the balancer's job. Rejected.

## 6. Tests

- **Shedding.** With a fake fleet view, a replica 30% above the mean sheds down toward it,
  bounded by `per_minute`, and only movable connections.
- **No shedding:**
  - while the fleet is unsettled;
  - while this replica is draining;
  - below the tolerance;
  - without a report.
- **Integration.** Three socket.io replicas behind a test balancer start unbalanced, and
  converge within a bounded number of intervals without shedding game connections.

## 7. Docs

A section on the socket.io page:
- when the step helps, and when a least-connections balancer is enough;
- the report it needs;
- the settle gate.

## 8. Out of scope

- **Tables, routes and tenants.** They use `excess` directly, with their own way of moving a
  unit.
- **Live re-sharding.**

## 9. Risks

- **Moved load, not removed load.** A balancer that is not least-connections can send the shed
  clients back. The docs make least-connections a prerequisite.
- **Building it without a second consumer** would freeze one app's choices into the framework.
  That is why it is demand-gated.

## 10. Unresolved questions

- The trigger for building it: a second socket.io or WebSocket app with long-lived
  connections behind a balancer that is not least-connections. Until then the design stays a
  draft.

## 11. Decisions

| # | Grade | Decision |
| --- | --- | --- |
| 1 | `LOCKED` | Rebalancing ships as a socket.io step only, composed from fleet membership, `excess` and `close_spread`. No framework-wide rebalancer. |
| 2 | `LOCKED` | Demand-gated: built once a second realtime app needs it. The layers under it ship first. |
| 3 | `ASSUMED` | The step sheds only connections the app marks movable, at most `per_minute`, only while the fleet is settled and this replica is serving, and never while it is draining. |
| 4 | `ASSUMED` | The load and movable counts come from the replica's own membership report, so a replica that does not report is never shed from. |

## 12. Phasing

One PR, after RFCs 0073 and 0075, once the trigger in §10 holds.
