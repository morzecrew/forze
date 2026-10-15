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

## Join the fleet

Every replica runs `fleet_membership_lifecycle_step`, which keeps its entry current for as
long as it runs:

```python
from datetime import timedelta

from forze.application.contracts.fleet import FleetSpec
from forze_kits.integrations.fleet import fleet_membership_lifecycle_step

GATEWAYS = FleetSpec(name="gateways", ttl=timedelta(seconds=10))

membership = fleet_membership_lifecycle_step(
    GATEWAYS,
    every=timedelta(seconds=3),
    report=lambda: {"load": open_connections()},
    depends_on=("redis_lifecycle",),
)
```

At startup it beats once as `starting`, then beats every `every` from a background loop. Its
state follows the runtime it runs in:

| State | While |
|-------|-------|
| `starting` | lifecycle startup has not finished |
| `serving` | startup finished, and no drain has begun |
| `draining` | a drain was announced — `runtime.begin_draining()` or `runtime.drain()` — or the drain gate closed |

Peers see a change at the next beat. `every` must be below half the `ttl`, so one missed beat
does not drop the member; a step built with more is refused. A beat may take at most
`(ttl − every) / 3`: the loop waits `every` after a beat's reply, while an entry's `ttl` runs
from its write, so a slow reply and a slow next write together must fit inside the `ttl`,
with room left for the loop's own time. A beat that fails, or takes longer, is logged and the
next one tries again. The first one fails startup instead, so an unwired fleet or a report
the codec refuses stops the boot rather than logging on every beat.

On stop the step does one of two things:

- `on_stop="leave"`, the default, removes the entry at once, so peers re-rank immediately.
- `on_stop="linger"` writes one last `draining` entry that lives for `linger_for`, so peers
  see the deploy in progress for that long without the process waiting for it. That entry's
  own `ttl` shortens no one else's life.

Both write to the fleet's backend, so name the step that closes its client in
`depends_on` — the Redis client's step above — and the membership step stops first. The
step writes only its own entry and removes expired ones, which is safe from every replica at
once: a `FLEET` runtime accepts it without a singleton guard. It needs a long-running
process, so a `SERVERLESS` runtime refuses it.

## Read the fleet

`ctx.fleet(spec)` resolves the port, so any code can read the fleet without beating:

```python
async def my_share(ctx, me: str) -> int:
    view = await ctx.fleet(GATEWAYS).view()

    return view.share(1000, me)
```

| Method | What it does |
|--------|--------------|
| `beat(member_id, state, report, *, ttl=None)` | removes expired entries, writes this member's, and returns the view — one atomic call on the backend's clock |
| `view()` | reads the live members without writing anything |
| `leave(member_id)` | removes this member's entry at once, so peers stop counting it immediately |

The step calls `beat` and `leave` for you; call them yourself only for an entry the step
does not keep.

`FleetSpec` fields:

| Field | Type | Default | Meaning |
|-------|------|---------|---------|
| `name` | `str \| StrEnum` | required | the fleet; every replica of the service beats into the same one |
| `ttl` | `timedelta` | `10s` | how long an entry lives without another beat |
| `report_max_bytes` | `int` | `1024` | the largest encoded report a beat may carry |

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

A fleet is never tenant-scoped. One service's replicas form one fleet whatever tenants they
serve, and the step beats from lifecycle hooks, where no tenant is bound. A fleet route
therefore takes neither `tenant_aware` nor a per-tenant namespace — either is refused when
the route is configured — and stays outside `required_tenant_isolation`, so a floor on the
rest of the module does not touch it. For the same reason a `RedisDepsModule` whose client is
routed per tenant refuses `fleets`: register `FleetDepKey` yourself, over a
`RedisFleetAdapter` on a plain `RedisClient`.

## Choosing a member id

The step beats as `instance_id()`, which returns `host:pid:nonce`. The nonce is random per
process, so a restarted process is a new member even when it reuses a pid. That is the safe
default, with one cost:
every restart removes one id and adds another, so a rolling deploy reshuffles every unit
`rank` assigns.

A platform that gives each replica a stable name, such as a StatefulSet pod's, can pass it
as the step's `member_id`, and `rank` then survives restarts. The cost moves: two processes sharing
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
frequent beats; the step's `every` stays below half of it either way.
