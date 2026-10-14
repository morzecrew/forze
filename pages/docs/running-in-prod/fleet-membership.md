---
title: Fleet membership
icon: lucide/users
summary: Which replicas of a service are alive, what state each is in and what each reports — and the policies that split work over them
---

A replica that wants to split work with its peers first has to know who its peers are.
Fleet membership answers that from one call: each replica **beats**, which writes its own
entry and returns everyone's. A replica whose entry expires is gone — there is no separate
failure detector to configure. The view a beat returns carries the policies apps need to
divide work, so every replica holding the same view computes the same answer.

## Members, states and reports

A **member** is one replica's entry in a fleet. It carries:

| Part | Meaning |
|------|---------|
| `id` | the member id the replica beats with — see [choosing a member id](#choosing-a-member-id) |
| `state` | `starting` (joining), `serving` (taking work) or `draining` (on its way out) |
| `since` | when the member entered its state, on the backend's clock; it survives beats in the same state and resets on a change |
| `report` | a small flat map the member publishes, such as its load |

A report maps strings to `int`, `float` or `str` values — no `bool`, no NaN or infinity, no
nesting — and its encoding is capped at `report_max_bytes`. A beat that breaks either rule,
or carries a member id with a tab, a newline or a brace, is refused before anything is
sent. Numbers read back exactly as they were reported.

## Beat and read

`ctx.fleet(spec)` resolves the port for a `FleetSpec`:

```python
from datetime import timedelta

from forze.application.contracts.fleet import FleetSpec
from forze.base.primitives import instance_id

GATEWAYS = FleetSpec(name="gateways", ttl=timedelta(seconds=10))


async def heartbeat(ctx, load: int) -> int:
    fleet = ctx.fleet(GATEWAYS)
    view = await fleet.beat(instance_id(), "serving", {"load": load})

    return view.share(1000, instance_id())
```

| Method | What it does |
|--------|--------------|
| `beat(member_id, state, report, *, ttl=None)` | removes expired entries, writes this member's, and returns the view — one atomic call on the backend's clock |
| `view()` | reads the live members without writing anything |
| `leave(member_id)` | removes this member's entry at once, so peers stop counting it immediately |

`FleetSpec` fields:

| Field | Type | Default | Meaning |
|-------|------|---------|---------|
| `name` | `str \| StrEnum` | required | the fleet; every replica of the service beats into the same one |
| `ttl` | `timedelta` | `10s` | how long an entry lives without another beat |
| `report_max_bytes` | `int` | `1024` | the largest encoded report a beat may carry |

Beat at under half the `ttl`, so one missed beat does not drop the member. A beat's own
`ttl` sets only that member's expiry: a short final beat — a replica announcing `draining`
for a few seconds before it exits — expires on its own and never shortens anyone else's.

## The policies

A `FleetView` holds the live `members`, sorted by id, and `at`, the backend's clock when it
was read. Its policies are pure functions of the view:

| Policy | Answers |
|--------|---------|
| `settled(for_)` | has the fleet stopped changing? True when every member is serving, and has been for at least `for_` |
| `share(total, me)` | how many of `total` units are mine, split evenly over the serving members by id; a member not serving gets 0 |
| `rank(key, *, states=("serving",))` | who owns the unit `key`, and who succeeds them — rendezvous order over the members in `states` |
| `excess(me, *, load, movable=None, tolerance=0.15, settle, smallest=1)` | how many units above the mean `load` I hold, once the fleet is settled |

`rank(key)[0]` owns the unit, and removing a member moves only the units it owned. The order
comes from a stable hash, so it is the same in every process. Pass
`states=("serving", "starting")` to make a joining replica a candidate.

`excess` takes its mean over the serving members that report a numeric `load`, returns 0
until the fleet is `settled(settle)` and the load is above `mean × (1 + tolerance)`, then
returns `ceil(load − mean)`, capped by the floor of the member's `movable` report when one
is named. A result below `smallest` is 0. Use it to decide how many connections, tables or
tenants to hand back; moving them is your protocol, not the fleet's.

Compare a member's `since` with the view's `at`, never with your own clock: the two come
from the same backend clock, so skew between replicas does not enter.

## Wiring

Redis implements the port. Register each fleet by its spec name:

```python
from forze_redis import RedisDepsModule, RedisFleetConfig

RedisDepsModule(client=redis, fleets={"gateways": RedisFleetConfig(namespace="app")})
```

A fleet is two keys under one hash tag, `{fleet:<namespace>:<name>}:z` and `:h`: a sorted
set of member ids scored by their expiry, and a hash of their entries. Each call is one
script, and expiry, `since` and `at` all come from the server's clock, so replicas whose
own clocks disagree still see the same fleet. Both keys expire a second after the last
member's entry. The mock implements the same model in memory, on a clock a test can move.

## Choosing a member id

`instance_id()` returns `host:pid:nonce`. The nonce is random per process, so a restarted
process is a new member even when it reuses a pid. That is the safe default, with one cost:
every restart removes one id and adds another, so a rolling deploy reshuffles every unit
`rank` assigns.

A platform that gives each replica a stable name, such as a StatefulSet pod's, can beat with
that name instead, and `rank` then survives restarts. The cost moves: two processes sharing
the name — a stuck process and its replacement — write one entry between them, and the
fleet sees one member where there are two.

## What a view promises

Two replicas that hold the same view always agree. Two replicas reading at different moments
can hold different views for up to one beat interval, and then their answers can differ. A
membership view is not consensus.

Anything that **assigns** work from a view inherits that: a replica that dies leaves its
units unowned until its entry expires, and a replica whose beat lands after another's read
can make two replicas claim overlapping, or missing, units for a while. Make the work
itself safe to claim twice — a lease, an idempotent consumer — and use the view to keep the
overlap rare. [Tenant-sharded realtime](../recipes/tenant-sharded-realtime.md#assign-tenants-at-startup)
assigns tenants at startup by rank, with the same caveat.

## Sizing

Every beat returns every entry, so the traffic across the fleet grows with the square of the
fleet size times the report size. That suits tens to low hundreds of replicas, with small
reports.

A stalled replica stays a member until its entry expires, beating or not, so `ttl` is your
failure-detection latency. A shorter `ttl` notices a dead replica sooner and costs more
frequent beats; keep the beat interval below half of it either way.
