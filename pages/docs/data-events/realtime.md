---
title: Realtime push
icon: lucide/radio-tower
summary: Server-initiated push as an egress plane — publish a signal, a gateway delivers it, even to someone who's offline
---

A shipment is confirmed in a background saga, and the customer staring at their
order page should see it *now* — no refresh, no poll. That push originates on the
server, far from the request that opened the socket. Forze models it the same way
it models every other cross-system effect: not as a method you call to reach a
connection, but as a **signal you publish as data**, which a gateway delivers.

This page is the conceptual tour. The [Socket.IO integration](../integrations/socketio.md)
has the wiring; the [`realtime_offline` example](https://github.com/morzecrew/forze/tree/main/examples/recipes/realtime_offline)
is a complete, runnable program.

## A connection is not a port

Forze's ports are *driven* dependencies — a database, a broker, an email vendor —
infrastructure with an existence of its own. A live WebSocket is not that. It is
the **inbound adapter's own state**: ephemeral, pinned to one node, tied to a
session that a request opened. A saga reaching sideways into that connection
would be coupling the application to the interface edge — the egress twin of
letting a use case render an HTTP route.

So the application never touches a connection. It publishes a **`RealtimeSignal`** —
a logical `Audience` (a `principal` or a `topic`), a typed event, and a payload —
onto the messaging it already uses. Everything realtime delivery needs comes from
that choice for free: tenancy, the read-only guard, encryption, transactional
safety, and a clean simulation boundary all come from the messaging port, not a
bespoke realtime stack.

## Two ways to publish

The application picks a delivery discipline by *how* it publishes, and the choice
is visible at the call site:

<div class="grid cards" markdown>

-   **`publish` — ephemeral**

    Appended straight to the realtime stream, fire-and-forget. At-most-once:
    perfect for typing indicators, presence, live cursors — signals that are
    worthless a second later, so a missed one costs nothing.

-   **`stage` — durable**

    Staged in the outbox with the business write and relayed after commit.
    At-least-once, deduplicated at delivery: for things that *must* arrive while
    the recipient is online — order shipped, payment received.

</div>

Durable publishing dissolves the usual "did the emit race the commit?" problem:
the signal rides the same transactional outbox as every other integration event,
relayed only once the transaction is durable.

## A gateway bridges to connections

On the other side, a **gateway** consumes the realtime stream and emits each
signal to the connections it owns. It is an edge adapter — the egress twin of the
inbound Socket.IO adapter — and it is where Socket.IO is confined. It resolves a
signal's `Audience` to a tenant-scoped *room* and emits; the Redis manager fans
that emit to whichever node actually holds the recipient's socket, so the gateway
need not be co-located with connections.

Tenancy lives at **room membership**: a connection only ever joins its own
tenant's rooms, so the stream itself can be tenant-global.

**A transport is never a new application contract.** Three ship — the Socket.IO
gateway, an SSE route and a raw-WebSocket route — and each is just another
consumer of the same stream, delivering the same envelope under the same
[wire protocol](../reference/realtime-protocol.md). Which one a client speaks is
a deployment fact: SSE where a browser only needs server push over plain HTTP,
raw WebSocket where it also sends governed commands back, Socket.IO where you
want rooms, presence and its client ecosystem. The signal your handler published
does not know the difference, and a fourth transport (a push vendor, a mobile
gateway) is written the same way.

## Reaching someone who's offline

Online delivery is only half the promise. If the recipient's phone is asleep when
the shipment confirms, emitting into an empty room loses the signal. So a durable,
principal-addressed signal is also written to a per-recipient **mailbox** — the
mailbox is the source of truth, and the live emit is just a latency optimization
on top. In a live app the gateway does this store as it processes the signal:

```python
--8<-- "recipes/realtime_offline/app.py:emit"
```

When the device reconnects, it is replayed everything past its own **cursor**, and
its ack advances that cursor — so a device never re-receives what it already saw,
and once every device has acked an entry it's trimmed away:

```python
--8<-- "recipes/realtime_offline/app.py:reconnect"
```

The mailbox is **bounded recent history**, not an unbounded queue: a recipient
offline longer than the retention window loses the oldest signals (delivery you
need to guarantee forever belongs in domain state, not in realtime). Topic
broadcasts are never mailboxed — there's no fixed membership to store them for —
and an event can opt out of offline delivery when it isn't worth persisting.

That bound is something you wire, not something you get. The mailbox has **no
delete path of its own**: the ack-driven trim only follows the slowest device's
cursor and the replay cap bounds *reads*, so a principal whose devices stop
acking accumulates entries forever. So `build_realtime_mailbox` takes a required
`retention=` with no default, and a declared window is checked against the
sweeper that enforces it:

```python
# a window, plus the step that actually deletes
mailbox = build_realtime_mailbox(
    ctx, retention=MailboxRetention(max_age=timedelta(days=7)),
)

lifecycle_steps = [
    realtime_mailbox_retention_lifecycle_step(max_age=timedelta(days=7)),
]
```

Declaring the window and forgetting the step is refused at build
(`realtime_mailbox_retention_unwired`) — that wiring *looks* bounded and is not,
which is worse than the honest unbounded case. If unbounded is genuinely what you
want, say so and it is allowed: `MailboxRetention.unbounded(reason=…)`. Nothing
about this failure mode is loud otherwise — storage grows, replays stay correct,
and the first symptom is a disk.

## Where to go next

- [Socket.IO integration](../integrations/socketio.md) — the full wiring: the
  publisher, the gateway and its lifecycle, presence, the device-identity
  handshake, the delivery envelope, and the production hardening knobs.
- [Realtime wire protocol](../reference/realtime-protocol.md) — the client
  contract every transport implements: handshake, envelope, cumulative ack,
  replay, and the AsyncAPI export you generate clients from.
- [SSE and raw WebSocket routes](../integrations/fastapi.md#realtime-egress-over-sse) —
  the two HTTP-native transports.
- [Events & sagas](events-sagas.md) — the outbox and relay the durable path rides on.
- Recipes: [offline delivery](../recipes/realtime-offline-delivery.md) (store-and-forward
  per device) and [tenant-sharded realtime](../recipes/tenant-sharded-realtime.md)
  (trusted per-tenant isolation).
