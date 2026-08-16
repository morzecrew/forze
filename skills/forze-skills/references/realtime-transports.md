# Realtime transports

Socket.IO, SSE and WebSocket behind one wire protocol, plus the offline mailbox and per-device cursors that make delivery survive a disconnect. What gets published is [realtime catalog](realtime-catalog.md).

## Three transports, one protocol

All three deliver the same `{id, data}` envelope, negotiate the protocol version at
connect, and take the same cumulative ack. Pick by client, not by feature:

| Transport | Wire | Reach for it when |
|---|---|---|
| Socket.IO (`forze_socketio`) | Socket.IO events + ack callbacks | you want rooms, presence, and the Socket.IO client ecosystem |
| SSE (`attach_realtime_sse_route`) | `text/event-stream` + `POST …/ack` | a browser only needs server push over plain HTTP |
| Raw WebSocket (`attach_realtime_ws_route`) | JSON text frames, duplex | the client also sends governed commands, or cannot run Socket.IO |

The SSE and WebSocket routes live in `forze_fastapi.realtime` and attach to your app. Socket.IO is a **server of its own**, mounted beside FastAPI rather than routed by it, which is why its wiring looks different from the other two:

```python
from forze_socketio import (
    build_socketio_asgi_app,
    build_socketio_server,
    realtime_gateway_lifecycle_step,
)

sio = build_socketio_server(redis_url=REDIS_URL)   # omit redis_url for a single process
asgi = build_socketio_asgi_app(sio)                # mount alongside the FastAPI app

steps = [realtime_gateway_lifecycle_step(gateway, max_consecutive_crashes=5)]
```

`redis_url` is the multi-process switch: without it a Socket.IO room exists only inside one worker, so a second replica silently delivers to half your clients.

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
    # the factory names the window; build_realtime_mailbox cannot be passed bare
    mailbox_factory=lambda ctx: build_realtime_mailbox(
        ctx, retention=MailboxRetention(max_age=timedelta(days=7)),
    ),
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

## Offline delivery: mailbox and cursors

A durable principal-addressed signal is written to a per-recipient **mailbox**; the
mailbox is the source of truth and the live emit is a latency optimization. Each
device has a **cursor**, advanced by its ack, so a reconnecting device is replayed
exactly what it has not seen.

```python
from datetime import timedelta

from forze_kits.integrations.realtime import (
    MailboxRetention,
    build_realtime_cursors,
    build_realtime_mailbox,
    realtime_mailbox_retention_lifecycle_step,
)

mailbox = build_realtime_mailbox(
    ctx, retention=MailboxRetention(max_age=timedelta(days=7)),
)
cursors = build_realtime_cursors(ctx)

# ...and the step that actually enforces the window, in your lifecycle steps:
retention = realtime_mailbox_retention_lifecycle_step(max_age=timedelta(days=7))
```

Both are document-backed and tenant-aware — the document store scopes every row by
the ambient tenant, so your code carries no tenant logic.

`retention` is **required and has no default**, because the mailbox has no delete path
of its own: the ack-driven trim only follows the slowest device's cursor and the replay
cap bounds *reads*, so a principal whose devices stop acking accumulates entries
forever. Declaring a window without registering
`realtime_mailbox_retention_lifecycle_step` is refused at build
(`realtime_mailbox_retention_unwired`) — wiring that *looks* bounded and is not is worse
than the honest unbounded case. If unbounded is genuinely what you want, say so:
`MailboxRetention.unbounded(reason=...)`. Delivery you must guarantee forever belongs in
domain state, not in a mailbox.

Topic broadcasts are never mailboxed — there is no fixed membership to store them
for.

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

- **Treating the live emit as the delivery guarantee** — the mailbox is the source of truth; a signal emitted into an empty room is gone.
- **Client-asserted topics** — never subscribe a connection to a topic it asked for without an authorization decision.
- **Skipping `device_id`** — a shared per-principal cursor makes two tabs fight over the same ack position.
- **Building a client against one transport's quirks** — write against the wire protocol so the transport stays a deployment choice.
- **Putting must-never-be-lost delivery in the mailbox** — it is bounded recent history, not durable domain state.

## Reference

- [Realtime wire protocol](https://morzecrew.github.io/forze/latest/reference/realtime-protocol/)
- [Socket.IO integration](https://morzecrew.github.io/forze/latest/integrations/socketio/)
- [SSE and WebSocket routes](https://morzecrew.github.io/forze/latest/integrations/fastapi/)
- [Offline delivery recipe](https://morzecrew.github.io/forze/latest/recipes/realtime-offline-delivery/)
- [Tenant-sharded realtime recipe](https://morzecrew.github.io/forze/latest/recipes/tenant-sharded-realtime/)
