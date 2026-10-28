---
title: Socket.IO
icon: lucide/radio-tower
summary: Expose operations as real-time Socket.IO commands
---

`forze[socketio]` is an inbound transport — like [FastAPI](fastapi.md), but for
real-time events. It routes Socket.IO commands to Forze operations, validates
payloads, and resolves the execution context per connection.

## Install

```bash
uv add 'forze[socketio]'
```

No service for a single worker; an optional **Redis** backplane enables delivery
across multiple processes.

## Build the server

```python
from forze_socketio import build_socketio_server, build_socketio_asgi_app

sio = build_socketio_server()            # pass redis_url=... for multi-process
asgi = build_socketio_asgi_app(sio)      # or mount alongside a FastAPI app
```

An emit to a room starts each socket's send as an eager task, which costs the event loop
nothing when the send does not wait. python-socketio's own manager schedules a task per
socket, and at a few thousand sockets in a room scheduling them costs more than sending. A
send that does wait carries on beside the others, as the library's would, and the same
packets reach the same sockets. Pass `client_manager=AsyncManager()` to keep the library's
manager.

## Route commands to operations

Declare commands on a namespace router — each maps an event to an operation, with
typed payload and optional ack — then bind them through the adapter:

```python
from forze_socketio import ForzeSocketIOAdapter, SocketIONamespaceRouter

chat = SocketIONamespaceRouter(namespace="/chat")
chat.command(event="message.send", operation="messages.create", payload_type=SendMessage, ack_type=ReadMessage)

adapter = ForzeSocketIOAdapter(
    sio=sio,
    context_factory=lambda request: runtime.get_context(),  # tenant/deps wiring here
    operation_resolver=registry.resolve,                     # the frozen registry
)
adapter.include_router(chat)
```

On each event the adapter builds the context, validates the payload, runs the
operation, and returns the (validated) result as the Socket.IO ack.

## Errors and identity

Every handler runs inside an error boundary: a `CoreException` is acked as
`{"error": {"detail", "code", "kind", ...}}` honoring the same egress policy as
the HTTP boundary, so internals never leak to clients. An optional `identity_resolver`
on the adapter authenticates connections at connect time (refusing them via
`ConnectionRefusedError`, whose data is that same error payload) and binds the resolved `AuthnIdentity` onto each event;
without one, handlers run unauthenticated. Tenant resolution stays in the
`context_factory`.

## Push to clients (realtime egress)

Server push is an **egress plane**: a handler publishes a signal *as data* onto
messaging and a **gateway** consumes it and bridges to live connections — the handler
never touches a socket. The concept (audiences, ephemeral vs durable) is
[Realtime](../data-events/realtime.md); this is the Socket.IO gateway that hosts it.

A handler publishes through a `RealtimePublisher`:

```python
from forze_kits.integrations.realtime import build_realtime_transport, build_realtime_publisher
from forze.application.contracts.realtime import Audience, RealtimeEvent

MESSAGE_NEW = RealtimeEvent(name="message.new", payload_type=MessageView)
rt_transport = build_realtime_transport()          # one source of truth for the channel specs

rt = build_realtime_publisher(
    ctx, stream_spec=rt_transport.stream_spec, outbox_spec=rt_transport.outbox_spec
)
await rt.publish(Audience.topic("chat:42"), MESSAGE_NEW, view)   # ephemeral, at-most-once
await rt.stage(Audience.principal(user_id), ORDER_SHIPPED, dto)  # durable, at-least-once
```

The **gateway** runs as a supervised background lifecycle step, consuming the stream via
a consumer group and emitting to a tenant-scoped room (fanned out cluster-wide by the
Redis manager). Supervision is the same machinery every other background loop uses: a
crash restarts the loop after a jittered backoff (a configuration error is terminal —
wiring doesn't fix itself), and the loop registers as a **drainable**, so
`runtime.shutdown()` asks it to finish its in-flight batch before teardown instead of
cancelling it mid-emit. Durable signals also need the **relay** to move staged rows from
the outbox to the stream after commit, plus the gateway's `dedup` for exactly-once
delivery:

```python
from datetime import timedelta

from forze_socketio import (
    RealtimeGateway, StreamGroupSignalSource, GatewayDedup,
    realtime_gateway_lifecycle_step, attach_realtime_connection,
)
from forze_kits.integrations.realtime import (
    realtime_group_ensure_lifecycle_step, realtime_relay_lifecycle_step,
)

gateway = RealtimeGateway(
    sio=sio,
    source=StreamGroupSignalSource(stream_spec=rt_transport.stream_spec),
    dedup=GatewayDedup(inbox_spec=rt_transport.inbox_spec, tx_route="..."),  # exactly-once for durable
)
lifecycle = [
    realtime_group_ensure_lifecycle_step(stream_spec=rt_transport.stream_spec),  # before serving
    realtime_relay_lifecycle_step(
        outbox_spec=rt_transport.outbox_spec, stream_spec=rt_transport.stream_spec,
    ),
    realtime_gateway_lifecycle_step(gateway),
]
attach_realtime_connection(  # the single connect path
    sio, resolve=resolve_connection, presence=presence, ctx_dep=runtime.get_context,
)
```

`attach_realtime_connection` is the **single** connect path — it authenticates *and*
auto-joins the principal room, so do not also give `ForzeSocketIOAdapter` an
`identity_resolver` on the same namespace. It refuses a delegated identity; a custom
resolver must return the identity with its `actor` chain intact, not normalized to its
subject. The publish-side `Audience.principal(id)` must
use the same id the gateway joins with (`str(authn.principal_id)`).

Pass `ctx_dep` (or the serving `runtime`) so the connect can ask whether the deployment is
tenanted. Once it registers a tenant resolver, an anonymous connect and a connection
without a tenant are refused (`auth_required`, `tenant_required`), at connect and at
`realtime.reauth`, whatever the resolver returned. Anonymous connects are accepted only
where no tenancy is wired. `build_socketio_connection_resolver` applies both rules itself,
so they hold without `ctx_dep` too; a hand-written resolver relies on it. `tenant_required`
asks the client to select a tenant; a token refresh does not clear it.

A client that opens several namespaces over one connection is authenticated once per
namespace. Pass `share_identity=True` to each of them to authenticate once per Engine.IO
connection instead. Later namespaces with the same credential and resolver reuse the
first one's identity until it expires or the connection closes, and a different
credential is checked again. It is opt-in because a namespace connecting just after the
first no longer re-checks a revoked session.

`build_socketio_connection_resolver` builds `resolve_connection` for you: a fixed
credential ladder (connect/reauth `auth` payload, then the cookie, then
`Authorization: Bearer`, then an opt-in query parameter) verified through the wired
authn plane, filling the principal, tenant, device and credential `expires_at` the
expiry sweep enforces. It is the same ladder
[`build_ws_connection_resolver`](fastapi.md#authenticate-the-connection) uses, so both
transports authenticate by one set of rules — including its rule that cookie mode
requires a restricted `cors_allowed_origins`, attested with
`origin_allowlist_attested=True`. Socket.IO can also authenticate from the connect
`auth` payload alone (`header_name=None`), which the raw-WebSocket route cannot: its
upgrade carries no payload, and only a `realtime.reauth` frame does.

### The delivery envelope (client contract)

Every frame is a uniform envelope — `{ "id": <id|null>, "data": <payload> }`. Durable
frames carry the stable event id (dedup on it); ephemeral frames carry `null`. The
contract is versioned: send `protocol: 1` in the connect `auth` (missing means 1; an
unsupported version is refused with `realtime_protocol_unsupported`), and ignore
unknown envelope fields — additive changes never bump the version:

```js
const socket = io(url, { auth: { token, device_id, protocol: 1 } });

socket.on("order.shipped", ({ id, data }) => {
  if (id && seen.has(id)) return;
  if (id) { seen.add(id); socket.emit("realtime.ack", { up_to: id }); }
  render(data);
});
```

The full normative contract (handshake, envelope, ack, replay, errors — for this
transport and the SSE route) is the
[realtime wire protocol](../reference/realtime-protocol.md); its machine-readable twin
is `asyncapi_document(catalog, router)` — an AsyncAPI 3 document generated from the
typed event catalog and command router, ready for review or client-type generation.

### Tenancy and addressing

`Audience` is `principal(id)` or `topic(name)` — **no tenant**. The publish layer puts
the ambient tenant in the message headers and the gateway scopes the room
(`t:{tenant}:{kind}:{name}`); a connection only joins its own tenant's rooms, so the
stream can stay tenant-global.

For **trusted** per-tenant isolation without trusting a header, put the stream on the
[tenancy ladder](../identity-tenancy-enc/multi-tenancy.md): wire the stream route
`tenant_aware` and consume with `TenantShardedSignalSource(shard=shard)` instead of
`StreamGroupSignalSource` — one consume loop per assigned tenant, each bound to the
tenant of the stream it reads (no header trust). `StreamGroupSignalSource` and
`PubSubSignalSource` read with no tenant bound, so over a `tenant_aware` route the gateway
step refuses to start (`tenant_global_required`). The shard is a fixed snapshot resolved
at startup, so onboarding a new tenant needs a restart; hand the same `RealtimeShard` to
the source, the group-ensure step (`realtime_tenant_group_ensure_lifecycle_step`), and —
for a partitioned outbox — `realtime_tenant_relay_lifecycle_step`. The read-side rules
for every messaging resource are in the
[tenancy matrix](../reference/tenancy-matrix.md#messaging-you-consume-the-read-side-catch).

### Offline delivery, presence, and hardening

A durable, principal-addressed signal is also stored in a per-recipient **mailbox**, so
a device offline at emit time receives it on reconnect: give the gateway a
`mailbox_factory`, and pass `attach_realtime_connection` a `mailbox_factory`, a
`cursors_factory`, **and** the serving `runtime`. All three are required — the two
factories alone won't enable it (each device has its own cursor, so it never re-receives
what it acked). Replay and `realtime.ack` run on that runtime's live context, under the
connection's identity and tenant, so its scope must be open while the server serves, as
the app's lifespan holds it. With it closed, each connect's replay logs a configuration
error coded `realtime_replay_scope_closed` and delivers nothing, so that connection's acks
are ignored. For multi-node, use
`RedisRealtimePresence` (TTL-backed, so a crashed node's rooms lapse) with
`realtime_presence_heartbeat_lifecycle_step` — on a client not routed per tenant, since
rooms carry their tenant and connects, the gateway and the heartbeat use it with none
bound — and drop connections whose credential has lapsed with
`realtime_identity_expiry_lifecycle_step`. Both sweeps skip a socket already on its way out
(its namespace's disconnect begun, or its Engine.IO session gone) and leave it to its
disconnect handlers, without logging it. The full store-and-forward flow —
per-device cursors, trimming, and opting an event out (`offline_delivery=False`) — is the
[offline-delivery recipe](../recipes/realtime-offline-delivery.md).

The gateway's store half is shared code. A deployment with no Socket.IO server runs it
alone as `RealtimeMailboxWriter`, filling the mailbox the SSE and raw-WebSocket routes
replay from (see [offline delivery without Socket.IO](fastapi.md#offline-delivery-without-socketio)).
Run one or the other against a dedup inbox, never both: whichever marks a signal first
owns it, and a gateway that loses the race skips its live emit.

Operational hardening around the loop:

- **Token rotation without a reconnect** — a client sends `realtime.reauth` with its
  fresh auth payload; the same connection resolver re-verifies it in place (same
  principal only) and swaps the stored identity + `expires_at`. No reconnect, no replay.
- **Poison ceiling** — a durable signal whose bridge fails on every delivery is dropped
  (acked, logged critical, counted) after `max_deliveries` (default 5) instead of
  reclaim-looping forever. Mailboxed principal signals are unaffected: their store
  commits with the first successful mark, so the client recovers them on reconnect.
- **A single emit is bounded by default** — `emit_timeout` defaults to 5 s, so a dead
  backplane can't freeze the consume loop; the failure falls into the normal per-lane
  ack policy.
- **Delivery counters** — hand one `RealtimeGatewayStats` to the gateway *and* its
  source, then call `instrument_realtime_gateway(stats)` once at assembly; alarm on
  `forze.realtime.gateway.poisoned` and on `emit_failed` climbing while `emitted` is
  flat. Backlog is a property of the consumer group, not the process: poll
  `AckStreamGroupAdminPort.depth(group, stream)` for undelivered/pending/oldest-idle —
  the same surface `quiesce(ack_streams=[(stream_spec, group)])` attests at standstill.
- **Backplane heartbeat** — `realtime_backplane_heartbeat_lifecycle_step(sio, health)`
  pushes a probe frame through the Redis manager on an interval and
  `instrument_realtime_backplane(health)` exports its freshness; a dead manager listener
  otherwise stops every cross-node emit silently.
- **The realtime stream route must stay plaintext** — the gateway has no decrypt seam,
  so a stream route declaring an encryption tier is refused at run start
  (`realtime_stream_encryption_unsupported`) rather than emitting ciphertext to clients.
- **Cap the stream** — the realtime stream has an unbounded producer; set
  `RedisStreamConfig(retention_max_entries=...)` on its route
  (`DEFAULT_REALTIME_STREAM_MAX_ENTRIES` is the recommended starting point) so it cannot
  grow into a Redis-memory incident. Size the cap so its horizon at peak emit rate far
  exceeds the reclaim window; alarm on `depth()`'s pending age long before the cap matters.
  Add `realtime_stream_trim_lifecycle_step` to keep steady-state memory near the gateway
  group's **acknowledged** horizon instead of the cap — the sweep only removes entries
  every group has delivered and acked, so it can never outrun a slow or crashed gateway
  (pass `tenants=lambda: shard.tenants` on a tenant-sharded stream).

### Deployment

- **In-process** — run the gateway lifecycle step inside the socket-holding workers
  (fine for single node / dev).
- **Emit worker** — at scale, run the gateway and relay as a dedicated `redis_write_only`
  process holding no client sockets; the Redis manager fans emits to the socket nodes.
- **Consumer group** — `realtime_group_ensure_lifecycle_step` creates it idempotently
  before serving (the gateway reads but doesn't create it) and reclaims stranded pending
  entries so a durable signal whose consumer died is recovered, not lost.

## Drain a replica

When a replica leaves — a deploy, a scale-down — its sockets would otherwise close all at
once when the process stops, and every client would reconnect to the replicas that are left
in the same second. Instead, the replica closes its connections gently while it still
serves: spread over a window, after readiness has failed, so the clients reconnect through
the balancer to replicas that are still ready, a few at a time.

```python
import asyncio
from datetime import timedelta

import uvicorn

from forze.application.execution import build_runtime
from forze_fastapi import serve_uvicorn
from forze_socketio import (
    RealtimeConnectionStats,
    attach_realtime_connection,
    build_socketio_asgi_app,
    build_socketio_server,
    connection_drain_lifecycle_step,
    instrument_realtime_connections,
)

sio = build_socketio_server()
stats = RealtimeConnectionStats()
runtime = build_runtime(
    lifecycle_steps=connection_drain_lifecycle_step(
        sio,
        window=timedelta(seconds=20),  # spread the closes over this
        tail=timedelta(seconds=5),     # then wait this long for the slow ones
        stats=stats,
    ),
    drain_budget=timedelta(seconds=30),
)
attach_realtime_connection(
    sio,
    resolve=resolve_connection,
    draining=lambda: runtime.draining,  # turn connects away while leaving
    stats=stats,
    ctx_dep=runtime.get_context,
)
instrument_realtime_connections(stats)

asyncio.run(
    serve_uvicorn(
        uvicorn.Config(build_socketio_asgi_app(sio, other_asgi_app=api)),
        runtime,
        drain_budget=timedelta(seconds=30),
        announce_delay=timedelta(seconds=5),  # at least the balancer's notice time
    )
)
```

**Set `announce_delay`.** Readiness fails at once, but a balancer takes a while to notice —
for Kubernetes, the readiness probe's `periodSeconds × failureThreshold`, plus the time to
drop the endpoint. Until it does, it keeps sending reconnects here. With no delay — the
default — the drain closes connections at once and every client is turned away again on
its first reconnect; with `announce_delay` at least that long, the closes start once the
balancer sends reconnects elsewhere.

The step registers a [drain phase](../running-in-prod/shutdown-and-fleets.md#drain-phases).
When the runtime drains — on `SIGTERM` under
[`serve_uvicorn`](fastapi.md#graceful-shutdown-under-uvicorn), before uvicorn's own
shutdown closes every socket at once — the phase lists this node's connections and closes
them at the Engine.IO level, close *i* of *n* starting at `i × window / n`. Each close runs
on its own, so a slow one never holds up the next; after the last start the phase waits up
to `tail`, then stops waiting. A close still running carries on in the background — it
may be inside your disconnect handler — and is counted `timed_out`. The phase never takes
more than `window + tail`, but teardown waits for those closes, up to the runtime's
`shutdown_step_timeout`, before it closes any client: a disconnect handler writing to your
presence store finds it open. Past that the closes are no longer waited for, and process
exit can still cut one short.

**Fit the window to the budget.** The phase measures what is left of its allowance after
listing, gives the tail at most half of it and the window at most the rest. A budget
smaller than `window + tail` shrinks both; a larger one never lengthens the window, so a
generous budget does not slow the drain down. Under 100 ms the phase is skipped and the
connections close when the server stops. The budget itself has to fit the orchestrator's
grace period — see [Fit the budget to the grace period](../running-in-prod/shutdown-and-fleets.md#fit-the-budget-to-the-grace-period).

**Choose what closes.** `namespaces` (default `("/",)`) names the namespaces the drain may
close, and `select(namespace, sid) -> bool` can keep individual connections open. One
Engine.IO connection carries every namespace a client opened, and closing it ends them
all, so it closes only when every namespace on it is eligible — checked again right
before its close, so one that has opened a namespace the drain keeps since it was listed
stays open. `close_spread(sio, targets,
window, tail=...)` is the scheduler on its own, for closing connections at other times.

### What clients see

- **socket.io's JavaScript client** reports a transport close (`"transport close"`) and
  reconnects with its usual backoff. A namespace-level `disconnect` would read as
  `"io server disconnect"`, after which it does not reconnect on its own — which is why the
  drain closes the transport instead.
- **The Python client** (python-socketio) stops after either kind of close. Reconnect it
  yourself.
- **A connect that arrives while the replica drains** is turned away the same way: its
  Engine.IO connection is closed, and the JavaScript client reconnects — through the
  balancer, which by then sends it elsewhere. A connect on a connection that already
  carries another accepted namespace — one the drain kept open — is admitted instead:
  closing the connection would end that namespace too, and the connect goes with the
  connection when the drain or the process closes it. The client's backoff starts over
  whenever a connection opens, so a turned-away connect is held for a random 1–5 seconds
  before its connection closes: the hold is the backoff, and spreads the retries. A
  refused connect would not do:
  socket.io's JavaScript client treats a `connect_error` from the server as final and
  stops. Over long-polling the client sees its next poll fail rather than a close, and
  reconnects the same way. The check runs before authentication, again once admission
  and authentication have run, and as the connect's last step. After the hold the
  connection is looked at again: one the client already closed is left alone, and one that
  meanwhile carries another accepted namespace admits the connect after all. The close runs
  a late connect's own disconnect handlers, which undo its presence and room. Only if the
  close fails is the connect undone directly and does the client see a refusal: `Server is
  draining`, with `{"code": "realtime_draining"}` as its data.

`draining=` takes the serving runtime's state, `lambda: runtime.draining`, and nothing
defaults it, not even the `runtime` offline replay is wired with.

Spreading the closes spreads the reconnects, but a balancer that sends them all to one
replica still overloads it. Route WebSocket traffic **least-connections**, not round-robin
or by hash, so the reconnects fill the replicas with room.

### Admit connects

`attach_realtime_connection(..., admission=...)` takes an async callable that receives the
`SocketIOConnect` (sid, namespace, environ, auth) before the protocol is negotiated or the
connection authenticated. Return to admit it; awaiting delays the connect, so an app can
pace admissions by its own rules — priority lanes, a per-tenant budget. Raise a
`CoreException` to refuse it: the refusal renders as an error ack does, its message the
detail and its data `{"detail", "code", "kind", ...}`; a server-side kind shows the generic
detail and is logged.

### Count connections

`instrument_realtime_connections(stats)` exports `forze.realtime.connections` (open
connections per namespace), `forze.realtime.connect_refused` (by reason: `protocol`,
`auth`, `draining`, `admission`, `internal`) and `forze.realtime.drain_closed` (closes the
drain started, by outcome). See the [metric catalog](../reference/metrics.md#realtime-connections).

## Rebalance a fleet

A least-connections balancer places every new connection on the emptiest replica, but a
socket that stays open never moves. After a deploy the replica that restarted last holds few
connections and the others hold the rest, and when clients stay connected for hours, so does
the imbalance. If your clients reconnect often anyway — page loads, short sessions — the
balancer evens the fleet out on its own, and you do not need this.

The rebalance step lets a replica well above the fleet's mean close a few of its connections
at a time, only those you mark movable. Their clients reconnect through the balancer to
replicas with room. Each replica learns the others' counts from
[fleet membership](../running-in-prod/fleet-membership.md), so every replica runs both steps:

```python
from datetime import timedelta

from forze.application.contracts.fleet import FleetSpec
from forze.application.execution import build_runtime
from forze_kits.integrations.fleet import fleet_membership_lifecycle_step
from forze_socketio import (
    build_socketio_server,
    connection_rebalance_lifecycle_step,
    connection_report,
)

GATEWAYS = FleetSpec(name="gateways", ttl=timedelta(seconds=10))
NAMESPACES = ("/lobby", "/game")


def movable(namespace: str, sid: str) -> bool:
    return namespace == "/lobby"  # a game connection never moves


sio = build_socketio_server()
runtime = build_runtime(
    deps_module,  # wires the fleet, e.g. RedisDepsModule(fleets=...)
    lifecycle_steps=[
        fleet_membership_lifecycle_step(
            GATEWAYS,
            every=timedelta(seconds=3),
            report=connection_report(sio, namespaces=NAMESPACES, movable=movable),
            on_stop="linger",  # hold the settle gate shut until a replacement joins
            linger_for=timedelta(seconds=90),  # longer than a replacement takes to boot
            depends_on=("redis_lifecycle",),
        ),
        connection_rebalance_lifecycle_step(
            sio,
            fleet=GATEWAYS,
            every=timedelta(seconds=10),
            namespaces=NAMESPACES,
            movable=movable,
            per_minute=60,
        ),
    ],
)
```

Every `every`, the step reads the fleet's view and puts its own counts in its own entry. Its
load there is the lower of its live count and its last report, so connections that arrived
since that report are not counted; its movable count is live. It
then asks [`FleetView.excess`](../running-in-prod/fleet-membership.md#the-policies) how far
above the mean it is, and closes that many movable connections, no more than its rate allows,
the way the drain does: at the Engine.IO level, spread over `every`, each given up to another
`every` to finish. Each is checked again right before its close starts, so a client that
joined a game since the tick picked it stays.

**Route least-connections.** It is a prerequisite, not a tuning choice: a balancer that
round-robins, hashes, or pins a client to its replica with a sticky cookie can send the shed
clients straight back, and the step then moves load in circles. Long-polling across several
replicas needs every request of one Engine.IO session on one replica: pin by the session
(its `sid` query parameter, which a reconnect replaces), not by a cookie that outlives it,
or let clients use the WebSocket transport only.

**Shed only clients that reconnect.** socket.io's JavaScript client reconnects after the
close, as it does after the drain's (see [What clients see](#what-clients-see)). The Python
client stops: give it reconnect logic of its own, or leave its connections out of `movable`.

**The report counts transports.** One Engine.IO connection carries every namespace its
client opened, and closing it ends them all, so every count is of Engine.IO connections, not
of namespace connections. A connection is movable when every namespace on it is in
`namespaces` and passes `movable(namespace, sid)`; one game namespace keeps the whole
connection. The step reads its peers' loads from their membership reports, under `load`
(`"connections"`); only its own movable count limits its shed, so a peer's `movable_report`
(`"movable"`) is there for you to watch. `connection_report` returns the `report=` callable
that fills both with the counting the step uses for its own: give the two the same
`namespaces` and `movable`. A replica that reports no load is left out of the mean,
and sheds nothing itself. List every namespace your clients open in `namespaces`, so the
counts match what the balancer counts, and let `movable` decide what may move: a namespace
left out is not counted, and the step and the balancer then disagree about which replica is
full.

**When it sheds.** Only while all of these hold:

- Every replica in the view has been serving for `settle` (120 s by default), so nobody sheds
  while replicas start. A replica that leaves with the membership step's default
  `on_stop="leave"` drops out of the view at once, though, so midway through a rolling deploy
  the replicas left can count as settled and shed. Give the membership step
  `on_stop="linger"` with a `linger_for` longer than a replacement takes to boot, as above:
  the leaving replica's `draining` entry then holds the gate shut until its replacement beats
  as `starting`. Shedding does not change a replica's state, so it does not restart this
  clock.
- This replica has an entry in the view, is serving, and is not draining. A draining replica
  closes everything through its [drain](#drain-a-replica) instead. A shed already running
  when the drain begins goes on; one still running at shutdown starts no more closes, and
  teardown waits for the closes it started, as it does for the drain's.
- Its count is more than `tolerance` (15%) above the mean. Peers' reports can be a beat old;
  the tolerance absorbs that. Replicas beat at different moments, so in a connect storm
  their reports differ by up to one beat's growth: keep the membership step's `every` short
  enough that this stays within the tolerance.
- No earlier shed is still closing, and `cooldown` (default: `settle`) has passed since the
  last one ended, long enough for its clients to land elsewhere and be reported.

**The rate.** `per_minute` is a long-run rate. Each tick adds `per_minute` for each minute
that passed to an allowance, keeping the fraction, and a shed spends whole connections, so the
rate holds however short `every` is: a low rate sheds one connection every few ticks, not one
every tick. The allowance banks what refills over one shed cycle — the longer of a minute and
`cooldown` plus three `every` — so the cooldown does not lower the rate, and one shed can
spend all of it at once, spread over `every`. With the default 120 s cooldown and
`every=10s`, a shed every two minutes or so closes about two minutes' worth. The step's
`member_id` must be the membership step's; both default to `instance_id()`. It needs a
long-running process, so a `SERVERLESS` runtime refuses it.

## Settings

`SocketIOSettings` carries the Redis backplane: the URL (as a `SecretStr` — it holds the
password), the channel, and whether this process publishes without subscribing. Build the
URL from a `RedisSettings.dsn`; this package cannot import `forze_redis` itself. See
[connection settings](index.md#connection-settings).

## What it provides

| Surface | What it does |
|---------|--------------|
| `SocketIONamespaceRouter.command(...)` | inbound: event → operation, with typed payload/ack |
| `RealtimePublisher.publish` / `.stage` | egress: publish a signal to messaging (ephemeral / durable) |
| `RealtimeGateway` + `realtime_gateway_lifecycle_step` | egress: consume the stream, bridge to rooms — supervised (restart + backoff), drainable, bounded `emit_timeout`, poison ceiling |
| `TenantShardedSignalSource` + `realtime_tenant_group_ensure_lifecycle_step` | egress: tenant-aware per-tenant streams; binds tenant from the stream (trusted), no header trust; per-tenant fault isolation |
| `PubSubSignalSource` + `realtime_pubsub_spec` + `RealtimePubSubPublisher` | egress: broadcast pubsub live lane (at-most-once, every node sees every signal); durables keep riding outbox → stream → mailbox |
| `realtime_tenant_relay_lifecycle_step` | egress: per-tenant durable relay for a partitioned (tenant-aware) outbox |
| `attach_realtime_connection` | auto-join principal rooms + presence on connect; offline replay + ack |
| `build_socketio_connection_resolver` | the shipped connection resolver: connect/reauth payload, then cookie, header and opt-in query — the WebSocket route's ladder |
| `DocumentRealtimeMailbox` + `DocumentMailboxCursors` | offline store-and-forward: per-principal mailbox + per-device cursor |
| `RedisRealtimePresence` + `realtime_presence_heartbeat_lifecycle_step` | crash-safe multi-node presence (TTL + heartbeat) |
| `realtime_identity_expiry_lifecycle_step` | drop connections whose credential (`expires_at`) has lapsed |
| `connection_drain_lifecycle_step` + `close_spread` | close a leaving replica's connections spread over a window, before the server stops |
| `connection_rebalance_lifecycle_step` + `connection_report` | shed an overloaded replica's movable connections toward the fleet's mean, at a bounded rate |
| `RealtimeConnectionStats` + `instrument_realtime_connections` | open and refused connects, and the drain's closes |
| `realtime.reauth` (built-in event) | refresh a rotating token in place — same principal, no reconnect |
| `RealtimeGatewayStats` + `instrument_realtime_gateway` | delivery counters for the live path (emitted / failed / skipped / poisoned) |
| `BackplaneHealth` + `realtime_backplane_heartbeat_lifecycle_step` + `instrument_realtime_backplane` | probe the Redis fan-out path; alarm on staleness |

## Notes

- The registry must be **frozen** (`OperationRegistry(...).freeze()`), same as
  any transport.
- `operation_resolver` is the registry's own `resolve` — its signature is
  `(operation_key, context)`.
- Payloads validate through a per-route Pydantic `TypeAdapter`; authenticate
  connections with `identity_resolver`, bind tenant in the `context_factory`.
- Multi-process delivery needs the Redis backplane (`redis_url=`); without it the
  server is single-worker.
