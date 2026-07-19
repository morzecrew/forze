# Realtime wire protocol

The client contract for the realtime egress plane, normative for every transport.
One protocol, currently **version 1**, served over two transports: the Socket.IO
gateway and the SSE route. A client written against this page works against either.

The machine-readable twin of this page is the [AsyncAPI export](#asyncapi-export) —
generated from the same typed catalog the server validates against, so it cannot
drift from the code.

## Connect handshake

A connection negotiates its protocol version **once, at connect** — frames never
carry a version, and a connection speaks exactly one version for its lifetime.

| Field | Socket.IO (`auth` payload) | SSE (query parameter) |
|---|---|---|
| `protocol` | `io(url, { auth: { protocol: 1 } })` | `GET /realtime/sse?protocol=1` |
| `device_id` | `auth.device_id` (optional) | `?device_id=` (optional) |
| credentials | `auth.token` (resolver-defined) | HTTP credentials (middleware-resolved) |

Rules:

- A missing `protocol` means **1** (pre-versioning clients keep working).
- An unsupported version is **refused at connect** with the error code
  `realtime_protocol_unsupported`, naming the supported range. It is never silently
  downgraded.
- Additive envelope changes (new optional fields) do **not** bump the version.
  Consequently, clients **must ignore unknown envelope fields** — that rule is what
  makes minor evolution safe.
- A `device_id` is the stable cursor key for offline replay; without one the
  framework falls back to the authenticated session id (Socket.IO) or a shared
  per-principal cursor (SSE).

## The delivery envelope

Every delivered frame is the same envelope on every transport:

```json
{ "id": "<durable event id | null>", "data": { /* the event's payload model */ } }
```

- `id` is the **durable event id** — stable across redeliveries and transports.
  Ephemeral signals carry `null`.
- `data` is exactly the catalog-declared payload model's JSON — nothing added,
  nothing renamed.
- Durable consumers **must dedup by `id`**: at-least-once delivery, reconnect
  replay, and the SSE replay/live overlap all legitimately produce duplicates.

Transport binding:

- **Socket.IO**: emitted as the event named by the catalog
  (`socket.on("order.shipped", ({id, data}) => …)`).
- **SSE**: one `text/event-stream` frame per signal; the SSE `event:` field is the
  catalog event name, the SSE `id:` field is set for durable signals (so the
  browser's automatic `Last-Event-ID` names a durable event), and `data:` is the
  JSON envelope above. Comment lines (`: keepalive`) are heartbeats — ignore them.

## Acknowledgement

The ack is **cumulative**: acknowledging an event id acknowledges everything up to
and including it for this device, advancing the device's replay cursor.

- **Socket.IO**: `socket.emit("realtime.ack", { up_to: "<event id>" })`.
- **SSE**: `POST <sse path>/ack` with body `{ "up_to": "<event id>" }` (same
  `device_id` query parameter as the stream, so both derive the same cursor).

A client that never acks still converges: replay is idempotent client-side (dedup
by `id`) and bounded server-side by mailbox retention.

## Reconnect and replay

On connect, everything past the device's cursor is redelivered, oldest first:

- **Socket.IO** replays automatically after the connect handshake.
- **SSE** replays at the start of the response body. A browser-supplied
  `Last-Event-ID` header takes precedence over the stored cursor — the browser's
  native resume beats a stale server cursor. Without a live tail configured the
  stream ends after replay and the browser's auto-reconnect (with `Last-Event-ID`)
  gives long-poll-style delivery.

Only **principal-addressed durable** signals are replayed (they are mailboxed);
topic signals are live-only, at-most-once.

## Errors

- **Connect refusal** (bad credentials, unsupported protocol): Socket.IO refuses
  the connection with a client-safe message; SSE responds with the standard error
  envelope and the `X-Error-Code` header.
- **Command errors** (Socket.IO inbound): the ack callback receives
  `{ "error": { "detail", "code", "kind", "context" } }` — the same error envelope
  shape the HTTP routes render.

## Re-authentication (Socket.IO)

A rotating token is refreshed in place with
`socket.emit("realtime.reauth", { token, … })` — same principal and tenant only;
anything else is a re-login, which reconnects. The protocol version is **not**
renegotiated (it is fixed per connection).

## AsyncAPI export

`asyncapi_document(catalog, router)` (from `forze_socketio`) generates an AsyncAPI
3.0 document from the typed event catalog and the inbound command router: one
channel and `send` operation per egress event (payload schema from the pydantic
model, wrapped in the envelope above), one channel and `receive` operation per
command plus the built-in `realtime.ack`, with audience/offline metadata as
`x-forze-audience-kinds` / `x-forze-offline-delivery` extensions and the protocol
version in `info.x-forze-realtime-protocol`. The document is generated and
parity-tested against the catalog — never hand-edited.

Serve it like the OpenAPI schema with `attach_asyncapi_route` (from
`forze_fastapi.routes`) — the app composes the two, since the integration
packages never import each other:

```python
from forze_fastapi.routes import attach_asyncapi_route
from forze_socketio import asyncapi_document

attach_asyncapi_route(router, document=asyncapi_document(catalog, sio_router))
```

Client types then come from standard AsyncAPI tooling pointed at the endpoint —
the same workflow as OpenAPI codegen, which is why no maintained client SDK
ships:

```sh
npx @asyncapi/cli generate models typescript http://localhost:8000/asyncapi.json \
  --output src/generated/realtime
```

## Version history

| Protocol | Changes |
|---|---|
| 1 | The `{id, data}` envelope, cumulative `realtime.ack`, `realtime.reauth`, connect-time negotiation. |
