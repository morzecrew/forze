---
name: forze-realtime
description: >-
  Builds Forze realtime egress — RealtimeEvent catalogs, Audience, RealtimeSignal,
  RealtimePublisher (ephemeral vs durable), the offline mailbox and per-device
  cursors, and the three transports (Socket.IO gateway, SSE route, raw WebSocket
  route) behind one versioned wire protocol. Use when pushing server-side events
  to connected clients, adding live updates, offline replay, or presence.
---

# Forze realtime

Use when your service pushes events to connected clients. Realtime in Forze is an
**egress plane**: a handler publishes a `RealtimeSignal` through a port and never
touches a socket. A transport (Socket.IO, SSE, raw WebSocket) consumes the stream
and delivers. Handlers stay transport-agnostic; which transport a client speaks is
a wiring fact.

## Declare the event catalog

Events are declared once and shared by the publisher and every transport. The
catalog is what validates payloads and what the AsyncAPI export is generated from.

```python
from pydantic import BaseModel

from forze.application.contracts.realtime import (
    Audience,
    AudienceKind,
    RealtimeEvent,
    RealtimeEventCatalog,
)


class OrderShipped(BaseModel):
    order_id: str
    eta_minutes: int


order_shipped = RealtimeEvent(
    name="order.shipped",
    payload_type=OrderShipped,
    audience_kinds=frozenset({AudienceKind.PRINCIPAL}),   # None = any kind
    offline_delivery=True,                                # False = live-only, never mailboxed
)

catalog = RealtimeEventCatalog.of(order_shipped)
```

`Audience` is a `(kind, name)` selector with no tenant and no connection in it —
both are ambient. `Audience.principal(str(principal_id))` addresses one identity;
`Audience.topic("orders")` addresses an app-defined group.

## Publish from a handler

Build the publisher in the handler factory so a missing route fails at construction
rather than on first emit. Two disciplines, and the choice is a durability decision:

```python
from forze_kits.integrations.realtime import (
    build_realtime_publisher,
    realtime_outbox_spec,
    realtime_stream_spec,
)

publisher = build_realtime_publisher(
    ctx,
    stream_spec=realtime_stream_spec(),
    outbox_spec=realtime_outbox_spec(),   # omit to disable .stage
)

# Ephemeral, at-most-once — fire-and-forget onto the stream.
await publisher.publish(Audience.principal(str(user_id)), order_shipped, payload)

# Durable, at-least-once — staged into the outbox inside the current transaction;
# the relay appends it to the stream only after the commit is durable.
await publisher.stage(Audience.principal(str(user_id)), order_shipped, payload)
```

Use `stage` whenever the signal claims something the transaction made true —
`publish` is for signals that are worthless a second later (typing indicators,
cursor positions). A publisher refuses to build inside a read-only (`QUERY`)
operation, because publishing is a side effect.

## Offline delivery: mailbox and cursors

A durable principal-addressed signal is written to a per-recipient **mailbox**; the
mailbox is the source of truth and the live emit is a latency optimization. Each
device has a **cursor**, advanced by its ack, so a reconnecting device is replayed
exactly what it has not seen.

```python
from forze_kits.integrations.realtime import (
    build_realtime_cursors,
    build_realtime_mailbox,
)

mailbox = build_realtime_mailbox(ctx)
cursors = build_realtime_cursors(ctx)
```

Both are document-backed and tenant-aware — the document store scopes every row by
the ambient tenant, so your code carries no tenant logic. The mailbox is **bounded
recent history**, not a queue: register
`realtime_mailbox_retention_lifecycle_step` so an age sweep bounds growth and prunes
idle device cursors. Delivery you must guarantee forever belongs in domain state.

Topic broadcasts are never mailboxed — there is no fixed membership to store them
for.

## Three transports, one protocol

All three deliver the same `{id, data}` envelope, negotiate the protocol version at
connect, and take the same cumulative ack. Pick by client, not by feature:

| Transport | Wire | Reach for it when |
|---|---|---|
| Socket.IO (`forze_socketio`) | Socket.IO events + ack callbacks | you want rooms, presence, and the Socket.IO client ecosystem |
| SSE (`attach_realtime_sse_route`) | `text/event-stream` + `POST …/ack` | a browser only needs server push over plain HTTP |
| Raw WebSocket (`attach_realtime_ws_route`) | JSON text frames, duplex | the client also sends governed commands, or cannot run Socket.IO |

```python
from forze_fastapi.realtime import (
    RealtimeSseHub,
    attach_realtime_sse_route,
    realtime_sse_tail_lifecycle_step,
)

hub = RealtimeSseHub()
attach_realtime_sse_route(
    router,
    ctx_dep=runtime.get_context,
    mailbox_factory=build_realtime_mailbox,
    cursors_factory=build_realtime_cursors,
    hub=hub,
    authorize_topics=grant_topics,   # required for ?topics=; fail-closed
)
step = realtime_sse_tail_lifecycle_step(hub, stream_spec=realtime_stream_spec())
```

Client-side rules that hold on every transport:

- **Dedup by `id`.** Delivery is at-least-once; reconnect replay and the
  replay/live overlap legitimately produce duplicates.
- **Ignore unknown envelope fields.** Additive changes do not bump the protocol
  version — that rule is what makes minor evolution safe.
- **Ack cumulatively.** Acking an id acks everything up to it for that device.
- **Send `device_id`.** It is the cursor key. On SSE the ack endpoint *requires*
  `?device_id=`; device-less streams still work via `Last-Event-ID` resume.

## Wiring notes that bite

- Raw WebSocket scopes are **refused** by `SecurityContextMiddleware` and
  `InvocationMetadataMiddleware` unless the exact mounted path is allowlisted:
  `allowed_websocket_paths={"/realtime/ws"}`. Router prefixes count toward the
  path, and the boot check fails if an allowlisted path does not serve exactly one
  governed route.
- Topic subscriptions are **server-granted**. SSE requires an `authorize_topics`
  resolver and refuses the connection (`realtime_topics_unauthorized`) unless every
  requested topic is granted; on Socket.IO the app joins the room after its own
  checks.
- On the namespace tenancy tier (a `tenant_aware` realtime stream route), use the
  sharded lifecycle steps (`realtime_sse_sharded_tail_lifecycle_step`,
  `realtime_tenant_relay_lifecycle_step`) so signals fan out under the stream's
  trusted identity rather than an untrusted header.
- Realtime loops are supervised: they restart on crash with jittered backoff and
  register as drainable. A custom signal source must accept a stop signal.
- A realtime stream route that declares an encryption tier is **refused at start** —
  seal the mailbox at rest instead (sealing the replay index is refused).
- Generate clients from the AsyncAPI document (`asyncapi_document(catalog, router)`
  served via `attach_asyncapi_route`) rather than hand-writing types.

## Testing

Drive the public components in-process with `forze_mock` — mailbox, cursors and
publisher need no sockets. For the HTTP transports, FastAPI's `TestClient` reads
the SSE body and the WebSocket frames directly; assert on envelopes and cursor
advance, not on transport internals.

## Anti-patterns

1. **Emitting from inside a handler's transaction with `publish`** — an ephemeral
   emit before commit announces something that may roll back. Use `stage`.
2. **Treating the live emit as the delivery guarantee** — the mailbox is the source
   of truth; a signal emitted into an empty room is gone.
3. **Importing a transport in a handler** — handlers publish signals; Socket.IO,
   SSE and WebSocket live at the edge.
4. **Client-asserted topics** — never subscribe a connection to a topic it asked
   for without an authorization decision.
5. **Skipping `device_id`** — a shared per-principal cursor makes two tabs fight
   over the same ack position.
6. **Building a client against one transport's quirks** — write against the wire
   protocol so the transport stays a deployment choice.
7. **Putting must-never-be-lost delivery in the mailbox** — it is bounded recent
   history, not durable domain state.

## Reference

> Docs are versioned. These links use `latest` (the newest release). If your app pins an older `forze` minor, replace `latest` in the URL with that version (e.g. `.../forze/0.3/...`) or use the version selector on the site.

- [Realtime egress](https://morzecrew.github.io/forze/latest/data-events/realtime/)
- [Realtime wire protocol](https://morzecrew.github.io/forze/latest/reference/realtime-protocol/)
- [Socket.IO integration](https://morzecrew.github.io/forze/latest/integrations/socketio/)
- [SSE and WebSocket routes](https://morzecrew.github.io/forze/latest/integrations/fastapi/)
- [Offline delivery recipe](https://morzecrew.github.io/forze/latest/recipes/realtime-offline-delivery/)
- [Tenant-sharded realtime recipe](https://morzecrew.github.io/forze/latest/recipes/tenant-sharded-realtime/)
- Sibling skills: [`forze-messaging-streaming`](../forze-messaging-streaming/SKILL.md) (the stream and outbox underneath), [`forze-fastapi-interface`](../forze-fastapi-interface/SKILL.md) (middleware and route wiring)
