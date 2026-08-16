# Realtime event catalog

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

## Anti-patterns

- **Emitting from inside a handler's transaction with `publish`** — an ephemeral emit before commit announces something that may roll back. Use `stage`.
- **Importing a transport in a handler** — handlers publish signals; Socket.IO, SSE and WebSocket live at the edge.

## Reference

- [Realtime egress](https://morzecrew.github.io/forze/latest/data-events/realtime/)
