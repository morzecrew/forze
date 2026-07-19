# Realtime wire protocol

The client contract for the realtime egress plane, normative for every transport.
One protocol, currently **version 1**, served over three transports: the Socket.IO
gateway, the SSE route, and the raw-WebSocket route. A client written against this
page works against any of them.

The machine-readable twin of this page is the [AsyncAPI export](#asyncapi-export) —
generated from the same typed catalog the server validates against, so it cannot
drift from the code.

## Connect handshake

A connection negotiates its protocol version **once, at connect** — frames never
carry a version, and a connection speaks exactly one version for its lifetime.

| Field | Socket.IO (`auth` payload) | SSE (query parameter) | WebSocket (query parameter) |
|---|---|---|---|
| `protocol` | `io(url, { auth: { protocol: 1 } })` | `GET /realtime/sse?protocol=1` | `wss://…/realtime/ws?protocol=1` |
| `device_id` | `auth.device_id` (optional) | `?device_id=` (optional) | `?device_id=` (optional) |
| credentials | `auth.token` (resolver-defined) | HTTP credentials (middleware-resolved) | upgrade-request headers/query (resolver-defined) |

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
- Topic subscriptions are **server-granted**, never client-asserted: on Socket.IO
  the app joins the room after its own checks; on SSE the `?topics=a,b` parameter
  is authorized by the app's resolver and the connection is refused with
  `realtime_topics_unauthorized` if any requested topic is not granted.

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
- **WebSocket**: one JSON text frame per signal with the event name in-band —
  `{"event": <name>, "id": <id|null>, "data": <payload>}`. Keepalive is WS
  protocol-level ping/pong (the server's; no application frames).

## Acknowledgement

The ack is **cumulative**: acknowledging an event id acknowledges everything up to
and including it for this device, advancing the device's replay cursor.

- **Socket.IO**: `socket.emit("realtime.ack", { up_to: "<event id>" })`.
- **SSE**: `POST <sse path>/ack` with body `{ "up_to": "<event id>" }` (same
  `device_id` query parameter as the stream, so both derive the same cursor).
- **WebSocket**: send `{"type": "realtime.ack", "up_to": "<event id>"}` inline.

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
- **WebSocket** replays immediately after accept; a `?last_event_id=` query
  parameter resumes precisely, beating the stored cursor. Without a live hub the
  socket stays open for acks and commands after the replay.

Only **principal-addressed durable** signals are replayed (they are mailboxed);
topic signals are live-only, at-most-once.

## Duplex commands (WebSocket framing)

The raw-WebSocket transport frames its ingress as JSON objects with a `type`:

```json
{ "type": "cmd", "event": "note.create", "cid": "<correlation id>", "payload": { … } }
```

- `event` names a server-declared command route; `payload` is exactly the route's
  declared model. `cid` is client-chosen and echoed verbatim on the ack.
- Optional per-frame governance: `idempotency_key` (string) and `deadline_budget`
  (positive number of seconds) bind onto the invocation exactly as the HTTP
  headers do — but typed strictly: a wrong-typed value is refused
  (`realtime_invalid_frame`), never coerced or silently dropped. The same goes
  for `realtime.ack`: `up_to` must be a non-empty string.
- Every command is acknowledged: `{ "type": "ack", "cid", "data": <result> }` on
  success, `{ "type": "ack", "cid", "error": <error envelope> }` on failure.
- Limits are fail-loud: an unparseable or unknown-`type` frame gets a
  `{ "type": "error", "error": … }` frame (`realtime_invalid_frame`); a command
  past the server's in-flight bound is error-acked (`realtime_commands_limit`)
  without running; an oversized frame closes the socket (code 1009).

Commands are unordered with respect to each other (they dispatch concurrently up
to the in-flight bound) — sequence-dependent commands must wait for their acks.

## Errors

- **Connect refusal** (bad credentials, unsupported protocol, ungranted topics):
  Socket.IO refuses the connection with a client-safe message; SSE responds with
  the standard error envelope and the `X-Error-Code` header; WebSocket accepts and
  immediately closes with code 1008, the client-safe summary in the close reason.
- **Command errors** (Socket.IO ack callbacks, WebSocket `ack` frames): the error
  is `{ "detail", "code", "kind", "context" }` — the same envelope shape the HTTP
  routes render.

## Re-authentication

A rotating token is refreshed in place — same principal and tenant only; anything
else is a re-login, which reconnects. The protocol version is **not** renegotiated
(it is fixed per connection).

- **Socket.IO**: `socket.emit("realtime.reauth", { token, … })`.
- **WebSocket**: send `{ "type": "realtime.reauth", "cid", "auth": { token, … } }`;
  acknowledged like a command (`{"ok": true}` or an error ack).

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
| 1 | The `{id, data}` envelope, cumulative `realtime.ack`, `realtime.reauth`, connect-time negotiation. The WebSocket binding (in-band event name, typed ingress frames) is an additive transport binding within protocol 1. |
