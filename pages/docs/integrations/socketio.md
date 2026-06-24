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
the HTTP boundary, so internals never leak to clients. An optional `identity_resolver`
on the adapter authenticates connections at connect time (refusing them via
`ConnectionRefusedError`) and binds the resolved `AuthnIdentity` onto each event;
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

The **gateway** runs as a background lifecycle step, consuming the stream via a consumer
group and emitting to a tenant-scoped room (fanned out cluster-wide by the Redis
manager). Durable signals also need the **relay** to move staged rows from the outbox to
the stream after commit, plus the gateway's `dedup` for exactly-once delivery:

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
    emit_timeout=timedelta(seconds=5),  # a stuck delivery can't wedge the loop
)
lifecycle = [
    realtime_group_ensure_lifecycle_step(stream_spec=rt_transport.stream_spec),  # before serving
    realtime_relay_lifecycle_step(
        outbox_spec=rt_transport.outbox_spec, stream_spec=rt_transport.stream_spec,
    ),
    realtime_gateway_lifecycle_step(gateway),
]
attach_realtime_connection(sio, resolve=resolve_connection, presence=presence)  # the single connect path
```

`attach_realtime_connection` is the **single** connect path — it authenticates *and*
auto-joins the principal room, so do not also give `ForzeSocketIOAdapter` an
`identity_resolver` on the same namespace. The publish-side `Audience.principal(id)` must
use the same id the gateway joins with (`str(authn.principal_id)`).

### The delivery envelope (client contract)

Every frame is a uniform envelope — `{ "id": <id|null>, "data": <payload> }`. Durable
frames carry the stable event id (dedup on it); ephemeral frames carry `null`:

```js
socket.on("order.shipped", ({ id, data }) => {
  if (id && seen.has(id)) return;
  if (id) { seen.add(id); socket.emit("realtime.ack", { up_to: id }); }
  render(data);
});
```

### Tenancy and addressing

`Audience` is `principal(id)` or `topic(name)` — **no tenant**. The publish layer puts
the ambient tenant in the message headers and the gateway scopes the room
(`t:{tenant}:{kind}:{name}`); a connection only joins its own tenant's rooms, so the
stream can stay tenant-global.

For **trusted** per-tenant isolation without trusting a header, put the stream on the
[tenancy ladder](../identity-tenancy-enc/multi-tenancy.md): wire the stream route
`tenant_aware` and consume with `TenantShardedSignalSource(shard=shard)` instead of
`StreamGroupSignalSource` — one consume loop per assigned tenant, each bound to the
tenant of the stream it reads (no header trust). The shard is a fixed snapshot resolved
at startup, so onboarding a new tenant needs a restart; hand the same `RealtimeShard` to
the source, the group-ensure step (`realtime_tenant_group_ensure_lifecycle_step`), and —
for a partitioned outbox — `realtime_tenant_relay_lifecycle_step`. The read-side rules
for every messaging resource are in the
[tenancy matrix](../reference/tenancy-matrix.md#messaging-you-consume-the-read-side-catch).

### Offline delivery, presence, and hardening

A durable, principal-addressed signal is also stored in a per-recipient **mailbox**, so
a device offline at emit time receives it on reconnect: give the gateway a
`mailbox_factory` and the connect layer `mailbox_factory` + `cursors_factory` (each
device has its own cursor, so it never re-receives what it acked). For multi-node, use
`RedisRealtimePresence` (TTL-backed, so a crashed node's rooms lapse) with
`realtime_presence_heartbeat_lifecycle_step`, and drop connections whose credential has
lapsed with `realtime_identity_expiry_lifecycle_step`. The full store-and-forward flow —
per-device cursors, trimming, and opting an event out (`offline_delivery=False`) — is the
[offline-delivery recipe](../recipes/realtime-offline-delivery.md).

### Deployment

- **In-process** — run the gateway lifecycle step inside the socket-holding workers
  (fine for single node / dev).
- **Emit worker** — at scale, run the gateway and relay as a dedicated `redis_write_only`
  process holding no client sockets; the Redis manager fans emits to the socket nodes.
- **Consumer group** — `realtime_group_ensure_lifecycle_step` creates it idempotently
  before serving (the gateway reads but doesn't create it) and reclaims stranded pending
  entries so a durable signal whose consumer died is recovered, not lost.

## What it provides

| Surface | What it does |
|---------|--------------|
| `SocketIONamespaceRouter.command(...)` | inbound: event → operation, with typed payload/ack |
| `RealtimePublisher.publish` / `.stage` | egress: publish a signal to messaging (ephemeral / durable) |
| `RealtimeGateway` + `realtime_gateway_lifecycle_step` | egress: consume the stream, bridge to rooms (optional `emit_timeout`) |
| `TenantShardedSignalSource` + `realtime_tenant_group_ensure_lifecycle_step` | egress: namespace-tier per-tenant streams; binds tenant from the stream (trusted), no header trust |
| `realtime_tenant_relay_lifecycle_step` | egress: per-tenant durable relay for a partitioned (tenant-aware) outbox |
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
