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
the HTTP boundary — server-side kinds (internal, infrastructure, configuration,
concurrency) and unexpected exceptions are logged and acked with a generic
detail, so internals never leak to clients. An optional `identity_resolver`
on the adapter authenticates connections at connect time (refusing them via
`ConnectionRefusedError` when it raises, e.g. `exc.authentication`) and binds
the resolved `AuthnIdentity` onto the invocation context around each event;
without one, handlers run unauthenticated and governance hooks that require
identity will deny. Tenant resolution stays in the `context_factory`.

## Push to clients (realtime egress)

Server-initiated push is an **egress plane**, not a method you call to reach a
connection. The application publishes a signal *as data* onto messaging; a
**gateway** consumes it and bridges to live connections. So a handler stays on
the messaging side and never touches a socket, and any transport can host the
gateway.

```python
from forze_kits.integrations.realtime import build_realtime_transport, build_realtime_publisher
from forze.application.contracts.realtime import Audience, RealtimeEvent

MESSAGE_NEW = RealtimeEvent(name="message.new", payload_type=MessageView)
rt_transport = build_realtime_transport()          # one source of truth for all specs

# build from a handler factory — ports are resolved once, so a bad route fails at
# wiring, not on first emit (and it refuses to build in a read-only operation):
rt = build_realtime_publisher(
    ctx, stream_spec=rt_transport.stream_spec, outbox_spec=rt_transport.outbox_spec
)

# from any handler/saga — addressed by a tenant-agnostic Audience:
await rt.publish(Audience.topic("chat:42"), MESSAGE_NEW, view)   # ephemeral, at-most-once
await rt.stage(Audience.principal(user_id), ORDER_SHIPPED, dto)  # durable, at-least-once
```

The **gateway** runs as a background lifecycle step, consuming the stream via a
consumer group and emitting to a tenant-scoped room (fanned out cluster-wide by
the Redis manager):

```python
from datetime import timedelta

from forze_socketio import (
    RealtimeGateway, StreamGroupSignalSource, GatewayDedup,
    realtime_gateway_lifecycle_step, attach_realtime_connection,
    realtime_identity_expiry_lifecycle_step, realtime_presence_heartbeat_lifecycle_step,
)
from forze_kits.integrations.realtime import (
    realtime_group_ensure_lifecycle_step, realtime_relay_lifecycle_step,
)
from forze_redis.adapters import RedisRealtimePresence  # crash-safe, multi-node

gateway = RealtimeGateway(
    sio=sio,
    source=StreamGroupSignalSource(stream_spec=rt_transport.stream_spec),
    dedup=GatewayDedup(inbox_spec=rt_transport.inbox_spec, tx_route="..."),  # exactly-once for durable
    emit_timeout=timedelta(seconds=5),  # a stuck delivery can't wedge the loop
)
presence = RedisRealtimePresence(client=redis_client, ttl=timedelta(seconds=90))
lifecycle = [
    # order matters: create the consumer group before the relay/serving starts
    realtime_group_ensure_lifecycle_step(stream_spec=rt_transport.stream_spec),
    realtime_relay_lifecycle_step(
        outbox_spec=rt_transport.outbox_spec, stream_spec=rt_transport.stream_spec,
    ),
    realtime_gateway_lifecycle_step(gateway),
    # multi-node hygiene (each node runs its own):
    realtime_presence_heartbeat_lifecycle_step(sio, presence, interval=timedelta(seconds=30)),
    realtime_identity_expiry_lifecycle_step(sio, interval=timedelta(seconds=30)),
]

# auto-join each connection to its principal room on connect (set expires_at on the
# resolved RealtimeConnection for the expiry sweep to act on):
attach_realtime_connection(sio, resolve=resolve_connection, presence=presence)
```

`attach_realtime_connection` is the **single** connect path — it authenticates
*and* auto-joins. Socket.IO keeps one connect handler per namespace, so do not
also give `ForzeSocketIOAdapter` an `identity_resolver` on the same namespace
(it would silently overwrite this one). The publish-side `Audience.principal(id)`
must use the same id form the gateway joins with (`str(authn.principal_id)`).

Durable signals also need the **relay** (`realtime_relay_lifecycle_step`) to move
staged rows from the outbox to the stream after commit, plus the gateway's
`dedup` for exactly-once delivery to online recipients.

### Tenancy and addressing

`Audience` is `principal(id)` or `topic(name)` — **no tenant**. The publish layer
puts the ambient tenant in the message headers; the gateway scopes the room
(`t:{tenant}:{kind}:{name}`). Isolation is enforced at **room membership**: a
connection only ever joins its own tenant's rooms, so the realtime stream can be
tenant-global.

### The delivery envelope (client contract)

Every frame the gateway emits is a uniform envelope — `{ "id": <id|null>, "data":
<payload> }`. Durable frames carry the stable event id; ephemeral frames carry
`null`. The client unwraps `data` and **dedups by `id`**:

```js
socket.on("order.shipped", ({ id, data }) => {
  if (id && seen.has(id)) return;   // skip a frame seen live then replayed
  if (id) { seen.add(id); socket.emit("realtime.ack", { up_to: id }); }
  render(data);
});
```

### Offline delivery (store-and-forward)

A durable, **principal**-addressed signal is also stored in a per-recipient
**mailbox** so a device offline at emit time receives it on reconnect. The mailbox
is the source of truth; the live emit is an optimization. Wire it by giving the
gateway a mailbox and the connect layer a mailbox + cursors:

The mailbox + cursors are **factories**, not built objects — they materialize their
document ports against each unit-of-work ctx (`build_realtime_mailbox`,
`build_realtime_cursors`), the same pattern as the publisher:

```python
from forze_kits.integrations.realtime import (
    build_realtime_mailbox, build_realtime_cursors,
)

gateway = RealtimeGateway(
    sio=sio, source=..., dedup=...,
    mailbox_factory=build_realtime_mailbox,  # store durable principal signals before emit
    presence=presence,                        # skip the live emit when the room is empty
    bind_tenant_from_headers=True,            # multi-tenant: trust the broker's tenant header
)

# replay-on-connect + ack-advances-cursor (needs a runtime to open a scope):
attach_realtime_connection(
    sio, resolve=resolve_connection, presence=presence,
    mailbox_factory=build_realtime_mailbox, cursors_factory=build_realtime_cursors,
    runtime=runtime,
)
```

**Tenancy is the document store's job, not the kit's.** Wire the mailbox + cursor
collections `tenant_aware` (`realtime_mailbox_spec` / `realtime_cursor_spec`) and the
adapter scopes every row by the ambient tenant — these helpers carry no tenant code.
The connect layer binds the connection's **authenticated** tenant; the gateway only
binds the per-signal `forze_tenant_id` header when `bind_tenant_from_headers=True`,
which is **off by default** because that header is untrusted/forgeable — enable it only
on a broker where every producer is trusted to assert tenancy.

!!! warning "A tenant-aware mailbox at the gateway requires `bind_tenant_from_headers`"
    The realtime stream is **tenant-global** (RFC 0002): one stream carries every
    tenant's signals so the cross-tenant gateway can drain it from one consumer group.
    The gateway therefore has **no ambient tenant of its own** — its only tenant source
    is the stream's `forze_tenant_id` header. So a `tenant_aware` *mailbox* at the
    gateway has nothing to scope by unless you set `bind_tenant_from_headers=True`;
    leave it off and the store **fails closed** with `realtime_mailbox_tenant_unbound`
    naming this contract. (The connect layer is unaffected — it scopes by the
    connection's authenticated tenant.) To scope by a **trusted** tenant *without* trusting
    the header, use the tenant-aware gateway below.

#### Tenant-aware (namespace-tier) gateway

The tenant-global default carries tenant in the (untrusted) header. For **trusted**
per-tenant isolation, put the realtime stream on the tenancy tier ladder like every
other port (RFC 0007): wire the stream route `tenant_aware` so the adapter gives each
tenant its own key/partition (`tenant:{id}:stream:realtime`), and consume with
`TenantShardedSignalSource` instead of `StreamGroupSignalSource`:

```python
from forze_socketio import RealtimeGateway, TenantShardedSignalSource
from forze_kits.integrations.realtime import (
    realtime_stream_spec, realtime_tenant_group_ensure_lifecycle_step,
)

stream = realtime_stream_spec()
my_tenants = lambda: load_assigned_shard()  # this instance's disjoint tenant set

gateway = RealtimeGateway(
    sio=sio,
    source=TenantShardedSignalSource(stream_spec=stream, tenants=my_tenants),
    dedup=..., mailbox_factory=build_realtime_mailbox,
    # bind_tenant_from_headers stays off — the tenant is the stream's, not a header
)
ensure = realtime_tenant_group_ensure_lifecycle_step(stream_spec=stream, tenants=my_tenants)
```

The source runs one consume loop per assigned tenant, each **bound** to that tenant, so
the tenant a signal belongs to is the stream it was read from — set by the publisher's
ambient tenant at write time. A tenant-aware mailbox and the room then scope by that
**trusted** tenant; the `forze_tenant_id` header is never trusted (or needed). The
publisher needs no change — wiring the stream route `tenant_aware` makes it append to the
per-tenant key automatically.

**Durable signals route per-tenant too.** A staged (durable) signal travels through the
outbox and the relay before reaching the stream, and the relay runs as a tenant-less
background process. It forwards each row under the tenant it was staged with (carried on
the outbox row), so the append lands on that tenant's stream key — and the sharded gateway,
then the offline mailbox, see it. The simplest wiring keeps the realtime **outbox**
tenant-global (one route, rows tagged with their tenant) while only the **stream** route is
`tenant_aware`; `realtime_relay_lifecycle_step` drains it with per-row routing.

If you need the outbox itself **partitioned** per tenant (namespace-tier *storage* of the
staging buffer), wire the outbox route `tenant_aware` too and use
`realtime_tenant_relay_lifecycle_step(outbox_spec=…, stream_spec=…, tenants=…)` instead: it
drains each assigned tenant's partition under a bound tenant (sequentially per tick). Pass
the **same** `tenants` shard the gateway and the group-ensure step use — one instance owns a
tenant shard end to end. A tenant-aware outbox drained by the plain (non-sharded) relay fails
closed with `outbox_relay_tenant_unbound`.

!!! note "Assignment, not discovery"
    Each gateway instance consumes the **disjoint** tenant shard `tenants` returns,
    evaluated **once at startup** — shard your tenants across instances (the same way the
    "emit worker" deployment already shards). Two consequences follow from "once at
    startup": onboarding a **new tenant** (and rebalancing a running fleet) requires a
    **restart** — a tenant created after boot has no group ensured and no consume loop, so
    it is unserved until you restart with the updated shard. Pass the **same** `tenants`
    provider and group to both `TenantShardedSignalSource` and
    `realtime_tenant_group_ensure_lifecycle_step` so the groups ensured match the streams
    consumed. Broker-level enforcement (so a rogue producer can't write another tenant's
    key) is Redis ACLs, the operator's job — as with the dedicated tier.

Each device has its own **cursor**, so it never re-receives what it acked. The
device is keyed by `ClientIdentity` — a client-supplied `device_id` (stable across
logins, passed in the connect handshake `auth`), else the authenticated session
`sid`, else the per-connection sid. Set it when you resolve the connection:

```python
async def resolve_connection(connect) -> RealtimeConnection:
    claims = await verify(connect.auth["token"])
    return RealtimeConnection(
        authn=AuthnIdentity(principal_id=claims.sub),
        tenant=claims.tenant,
        client=ClientIdentity(device_id=connect.auth.get("device_id"), session_id=claims.sid),
    )
```

Opt an event out of mailboxing with `RealtimeEvent(name=..., offline_delivery=False)`
(emit-only, best-effort). Topic signals are never mailboxed. The mailbox is bounded
recent history, not a forever queue: an ack trims what every known device has acked
(`MailboxCursors.min_cursor`), with TTL/cap (`mailbox.trim`) as the backstop. Share a
`MailboxStats` across the mailbox and cursors and pass it to `instrument_realtime_mailbox`
to export stored/replayed/trimmed/acked as OpenTelemetry counters.

!!! warning "Breaking change — the delivery envelope"
    Frames are now the uniform `{ id, data }` envelope (previously the bare
    payload). Clients must read `data` (and dedup by `id`). There is no
    transitional dual-emit; update clients in step with the server.

### Deployment

- **In-process** — run the gateway lifecycle step inside the socket-holding
  workers (fine for single node / dev with the mock or in-process stream).
- **Emit worker** — at scale, run the gateway (and relay) as a dedicated
  `redis_write_only` process holding no client sockets; the Redis manager fans
  emits to the nodes that do.
- **Consumer group** — `realtime_group_ensure_lifecycle_step` creates it idempotently
  at startup (the gateway reads but does not create it); order it before the relay so
  a fresh group's `"$"` start misses nothing. The gateway also reclaims stranded
  pending entries (`reclaim_idle`) so a durable signal whose consumer died before ack
  is recovered (and deduped) rather than lost.
- **Hardening** — for multi-node, use `RedisRealtimePresence` (TTL-backed, so a
  crashed node's rows lapse) instead of the single-node in-memory tracker, and run
  `realtime_presence_heartbeat_lifecycle_step` so live connections re-assert within
  the TTL. A long-lived socket can outlive a short-lived credential: set
  `RealtimeConnection.expires_at` at connect and run
  `realtime_identity_expiry_lifecycle_step` to drop expired connections. Give the
  gateway an `emit_timeout` so one stuck delivery can't wedge the consume loop (the
  signal is then redelivered/acked by the normal per-signal policy). Transport-level
  backpressure to slow clients remains engine.io's.

## What it provides

| Surface | What it does |
|---------|--------------|
| `SocketIONamespaceRouter.command(...)` | inbound: event → operation, with typed payload/ack |
| `RealtimePublisher.publish` / `.stage` | egress: publish a signal to messaging (ephemeral / durable) |
| `RealtimeGateway` + `realtime_gateway_lifecycle_step` | egress: consume the stream, bridge to rooms (optional `emit_timeout`) |
| `TenantShardedSignalSource` + `realtime_tenant_group_ensure_lifecycle_step` | egress: namespace-tier per-tenant streams; binds tenant from the stream (trusted), no header trust (RFC 0007) |
| `realtime_tenant_relay_lifecycle_step` | egress: per-tenant durable relay for a partitioned (tenant-aware) outbox; drains each assigned tenant's partition bound (RFC 0007) |
| `attach_realtime_connection` | auto-join principal rooms + presence on connect; offline replay + ack |
| `DocumentRealtimeMailbox` + `DocumentMailboxCursors` | offline store-and-forward: per-principal mailbox + per-device cursor |
| `RedisRealtimePresence` + `realtime_presence_heartbeat_lifecycle_step` | crash-safe multi-node presence (TTL + heartbeat) |
| `realtime_identity_expiry_lifecycle_step` | drop connections whose credential (`expires_at`) has lapsed |

## Notes

- The registry must be **frozen** (`OperationRegistry(...).freeze()`), same as
  any transport.
- `operation_resolver` is the registry's own `resolve` — its signature is
  `(operation_key, context)`.
- Payloads validate through a per-route Pydantic `TypeAdapter`; authenticate
  connections with `identity_resolver`, bind tenant in the `context_factory`.
- Multi-process delivery needs the Redis backplane (`redis_url=`); without it the
  server is single-worker.
