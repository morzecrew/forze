---
title: Streaming & pub/sub
icon: lucide/radio
summary: The stream and pub/sub contracts, and the realtime egress pattern built on them
---

Two append/fan-out messaging contracts, plus the realtime pattern built on them. A
**stream** is an ordered, replayable log read by offset or consumer group; **pub/sub** is
fire-and-forget fan-out to live subscribers. Both are [resolved by dep key](../contracts.md)
and declared with a `MessageCodecSpec` — a `name`, a payload `codec`, and an `encryption`
tier (`none` / `end_to_end`). Realtime push rides on the stream; see
[Realtime](../../data-events/realtime.md).

## Streams

`StreamSpec[M]` — an ordered, replayable log. Four ports by dep key:

| Dep key | Port | Methods |
|---------|------|---------|
| `StreamCommandDepKey` | producer | `append` |
| `StreamQueryDepKey` | reader | `read` (by offset), `tail` (follow) |
| `StreamGroupQueryDepKey` | consumer group | `read`, `tail`, `ack` — at-least-once per group |
| `StreamGroupAdminDepKey` | group admin | `ensure_group` |

A consumer group gives competing consumers and acked, resumable delivery; a plain reader
replays from any offset.

## Pub/sub

`PubSubSpec[M]` — fire-and-forget fan-out. Two ports:

| Dep key | Port | Methods |
|---------|------|---------|
| `PubSubCommandDepKey` | publisher | `publish` |
| `PubSubQueryDepKey` | subscriber | `subscribe` (an async stream of messages) |

Pub/sub is **at-most-once** past the broker — a subscriber offline at publish time misses
the message. For guaranteed delivery, use a stream consumer group or the
[outbox](messaging.md).

## Realtime egress

Server-initiated push is an **egress plane**, not a port: a handler publishes a
`RealtimeSignal` to a principal or topic through these messaging ports, and a gateway
consumes the stream and bridges it to live connections. The concept is
[Realtime](../../data-events/realtime.md); the
[Socket.IO gateway](../../integrations/socketio.md) hosts it.

## Implemented by

| Contract | Backend | Integration |
|----------|---------|-------------|
| Stream (+ consumer groups) | Redis | [Redis](../../integrations/redis.md) |
| Pub/sub | Redis | [Redis](../../integrations/redis.md) |

A mock implements both for tests.
