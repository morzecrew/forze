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
| `attach_realtime_connection` | auto-join principal rooms + presence on connect |
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
