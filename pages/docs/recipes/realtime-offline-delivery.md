---
title: Realtime delivery to offline users
icon: lucide/inbox
summary: Store a server-sent signal for a recipient who's offline, replay it on reconnect, and ack so it's never re-sent
---

Online realtime push is the easy half: emit to the room, the socket gets it. The
hard half is the user whose phone was asleep when the shipment confirmed. Forze
handles it with a per-recipient **mailbox** and a per-device **cursor** — store
the durable signal, replay what a device hasn't seen when it reconnects, and trim
once it's acked. See [Realtime push](../data-events/realtime.md) for the concepts;
this is the runnable shape.

The runnable version lives at `examples/recipes/realtime_offline/` (mock — no
sockets or broker needed). In a live app the [Socket.IO gateway](../integrations/socketio.md)
calls `store` for you and `attach_realtime_connection` does the reconnect replay
and `realtime.ack`; here we drive the same public components directly so the
mechanism is visible.

## Set the scene

A durable signal is addressed to a **principal** — a single recipient. The mailbox
scopes to the **ambient tenant** (read from the bound context, never passed as a
parameter), so a worker binds it before calling — the gateway from the signal
header, the connection from the live connection. Here `main()` binds it once around
the flow:

```python
--8<-- "recipes/realtime_offline/app.py:setup"
```

## Store what an offline user can't receive yet

When the gateway processes a durable principal signal it writes it to that
recipient's mailbox. Online, it would also `sio.emit`; offline — no room members —
the mailbox is the only delivery, drained later:

```python
--8<-- "recipes/realtime_offline/app.py:emit"
```

## Replay on reconnect, then ack

On connect, a device is replayed everything past **its own** cursor — so two
devices catch up independently. The client acks what it processed, which advances
that device's cursor and trims whatever every known device has now seen:

```python
--8<-- "recipes/realtime_offline/app.py:reconnect"
```

Run end to end, two signals stored while the phone is offline arrive in order on
reconnect; after the client acks the last one, a later reconnect of that same
device replays nothing — it's caught up:

```text
phone reconnect: ['shipped', 'delivered']
phone reconnect again: []
```

## Notes

- Only **durable, principal-addressed** signals are mailboxed. Ephemeral signals
  (`publish`) are emit-only, and topic broadcasts have no per-recipient mailbox.
- An event opts out with `RealtimeEvent(..., offline_delivery=False)` — emit-only,
  best-effort.
- The mailbox is **bounded recent history**: trimmed once all known devices ack,
  with a TTL/cap backstop. A device offline longer than the window loses the
  oldest signals — guarantee-forever delivery belongs in domain state.
- The cursor key is a `ClientIdentity` — a client-supplied `device_id` (stable
  across logins) or the authenticated session `sid`. With neither, it falls back
  to the per-connection socket id.
- **Tenant is ambient**, never a mailbox argument: the methods take only
  `principal`, and the implementation reads the tenant from the bound context (the
  worker binds it). The same discipline as the publisher reading the ambient tenant
  for the message header.
