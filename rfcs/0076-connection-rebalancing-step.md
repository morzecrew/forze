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
  - RFC 0074 (the draining state on the context);
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
    sio, *, fleet: FleetSpec, every, member_id: str | None = None,
    namespaces=("/",), load="connections", movable_report="movable",
    movable: Callable[[str, str], bool], per_minute: int, tolerance=0.15,
    settle=timedelta(seconds=120), cooldown: timedelta | None = None,
)
```

Every `every`, while this replica is serving, the fleet has settled, no earlier shed is still
closing, and the cooldown since the last shed has passed, the step does three things:
1. It reads the view, with this replica's own entry replaced by its live local counts.
2. It asks `view.excess(me, …)` how many connections to shed, capped by `per_minute`.
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
  1. **Skip the tick** if any of these holds:
     - this replica is draining (`ctx.lifecycle.announced`, RFC 0074), since a draining
       replica closes everything through RFC 0075 instead;
     - an earlier shed's `close_spread` is still running;
     - less than `cooldown` has passed since the last shed. The default cooldown is `settle`.
  2. **Read the view** (RFC 0073), and replace this replica's own entry with its live local
     counts:
     - the load is its connections in `namespaces`, minus those already scheduled to close;
     - the movable count is how many Engine.IO transports are movable: every namespace the
       transport carries accepted by `movable(namespace, sid)`. Closing a transport ends all
       its namespaces (RFC 0075), so one non-movable namespace, such as a game, keeps the
       whole transport.

     The peers' figures are their last reports. A member survives one missed beat, so a
     report can be up to `ttl` old, and the tolerance has to absorb that drift. This replica's
     own report could be a beat behind its last shed, and shedding on it would shed twice.
  3. `n = view.excess(me, load=load, movable=movable_report, tolerance=tolerance,
     settle=settle)`. That is 0 unless the fleet has settled, this replica is serving, and its
     load is more than `tolerance` above the mean, relative to it.
  4. Cap `n` with a token bucket: each tick adds `per_minute * every / 60` to an allowance,
     carried across ticks with its fraction, capped at `per_minute`. A shed spends whole
     units. `per_minute` is therefore a true rate however short `every` is: a low rate with
     a short interval sheds one every few ticks, rather than none or one every tick.
  5. Pick `n` movable transports not already scheduled, and close them with `close_spread`
     over `every`.
- **The report.** The peers' load and movable counts come from their membership reports,
  which the app fills. A peer that does not report them is left out of the mean, and is never
  shed from.
- **What keeps the fleet stable** is the tolerance, the cooldown and the settle gate, not
  shedding itself. Shedding does not change a replica's state, so it does not reset the settle
  gate. The cooldown stops a replica shedding again before its last shed's clients have landed
  elsewhere and been reported. The settle gate stops anyone shedding during a deploy.
- **Ids.** `member_id` must match the membership step's id. It defaults to the same
  `instance_id()`.

### 5.2 Alternatives considered

- **A generic `Rebalancer` interface** taking an actuator per unit kind. Rejected in RFC 0073
  (§5.6), for the four-protocols reason.
- **Moving clients by telling them where to go** (a redirect event). It needs client
  cooperation and a way to choose the target, which is the balancer's job. Rejected.

## 6. Tests

- **Shedding.** With a fake fleet view, a replica 30% above the mean sheds down toward it,
  bounded by `per_minute`, and only movable connections.
- **No double shedding.**
  - A tick during a running shed does nothing.
  - A tick inside the cooldown does nothing.
  - The live count, not the stale report, sets the next shed.
- **Rate.** `per_minute=10` sheds about ten a minute with `every=1 s` and with `every=30 s`.
- **Shared transports.** A transport carrying a movable and a non-movable namespace is never
  shed.
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
  connections that stay unbalanced even behind a least-connections balancer. A
  least-connections balancer places new connections well, but long-lived ones that never
  reconnect keep a deploy's imbalance. The balancer is still a prerequisite, because shed
  clients have to land on the emptiest replica. Until such an app exists, the design stays a
  draft.

## 11. Decisions

| # | Grade | Decision |
| --- | --- | --- |
| 1 | `LOCKED` | Rebalancing ships as a socket.io step only, composed from fleet membership, `excess` and `close_spread`. No framework-wide rebalancer. |
| 2 | `LOCKED` | Demand-gated: built once a second realtime app needs it. The layers under it ship first. |
| 3 | `ASSUMED` | The step sheds only transports whose every namespace the app marks movable, at a rate of `per_minute`, kept by a token bucket that carries fractions across ticks. It sheds only while the fleet is settled and this replica is serving, never while it is draining, never while an earlier shed is closing, and never inside the cooldown after a shed. |
| 4 | `ASSUMED` | Peers' load and movable counts come from their membership reports. This replica uses its live local counts, minus connections already scheduled to close, so it never sheds twice on a stale report. |

## 12. Phasing

One PR, after RFCs 0073 and 0075, once the trigger in §10 holds.
