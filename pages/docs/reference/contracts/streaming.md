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

`StreamSpec[M]` — an ordered, replayable log. It adds one field over the codec spec:
`requires_transactions` (default `False`). It is a **capability gate, not a delivery-semantics
switch**: setting it changes nothing about how messages are delivered, it only refuses at
resolve to wire a `CommitStreamGroupQueryPort` onto a backend that doesn't report
`supports_transactions` — so a consumer written against native transport-level exactly-once
can never be silently attached to a backend that only offers at-least-once. No shipped
backend reports `supports_transactions` today, so enabling it currently fails closed
everywhere; it is a forward guard for a transactional backend, not a way to obtain one.
Leave it `False` and pair the consumer with an [inbox](messaging.md) for the portable
exactly-once-*effect* path. Ignored by the ack sub-model. The producer and plain reader are shared;
consumption comes in two disciplines, named for how they acknowledge — per-message **ack**
(Redis-class) or per-partition offset **commit** (Kafka-class):

| Dep key | Port | Methods |
|---------|------|---------|
| `StreamCommandDepKey` | producer | `append` |
| `StreamQueryDepKey` | reader | `read` (by offset), `tail` (follow) |
| `AckStreamGroupQueryDepKey` | ack consumer group | `read`, `tail`, `ack`, `claim`, `pending` |
| `AckStreamGroupAdminDepKey` | ack group admin | `ensure_group` |
| `CommitStreamGroupQueryDepKey` | commit consumer group | `read`, `tail`, `commit`, `seek_to_committed` |
| `CommitStreamGroupAdminDepKey` | commit group admin | `ensure_topic`, `ensure_group`, `reset_offsets`, `lag` |

The **ack** group gives competing consumers, per-message acks, and explicit `claim` recovery
of stranded entries. The **commit** group is a partitioned, offset-committed log: a single
committed `StreamPosition` acknowledges every message up to it on that partition, recovery is
broker-coordinated (no per-message claim), and `reset_offsets` replays. Both are
at-least-once — including with `requires_transactions` set, which gates *which backend may be
wired*, not what the wired backend delivers — so pair either with the [inbox](messaging.md)
for exactly-once *effect*. A plain reader replays from any offset. The commit sub-model has context shortcuts —
`ctx.stream.commit_query(spec)` / `ctx.stream.commit_admin(spec)`; the other ports resolve
by dep key. For picking a model, see
[Messaging delivery models](../../data-events/messaging-delivery-models.md).

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
| Stream (+ ack consumer groups) | Redis | [Redis](../../integrations/redis.md) |
| Stream produce + commit consumer groups | Kafka-protocol brokers | [Kafka](../../integrations/kafka.md) |
| Pub/sub | Redis | [Redis](../../integrations/redis.md) |

A mock implements both for tests.
