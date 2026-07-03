---
title: Messaging delivery models
icon: lucide/split
summary: The four delivery models — queue, ack-stream, commit-stream, pub/sub — and how to pick one
---

Forze separates its messaging ports by **delivery model**, not by backend. The model decides
what a consumer is promised — whether a message survives a crash, whether it can be replayed,
how a stalled consumer recovers — so choosing the model is the real decision; the backend is
an implementation detail behind it. There are four.

## The four models

| Model | Guarantee | Recovery unit | Replayable | Fits |
|-------|-----------|---------------|------------|------|
| **Queue** | at-least-once | per-message `ack` / `nack(requeue)` | no | work items, jobs, commands |
| **Ack-stream** | at-least-once, per group | per-message `ack` + explicit `claim` | from group creation | ordered events, competing consumers |
| **Commit-stream** | at-least-once, per group | per-partition offset `commit`; broker rebalance | any offset / timestamp | high-throughput logs, event sourcing |
| **Pub/sub** | at-most-once | none | no | lossy broadcast (presence, cache-bust) |

Three of the four — queue, ack-stream, commit-stream — are at-least-once at the transport, and
those three reach **exactly-once *effect*** when paired with the
[inbox](../reference/contracts/messaging.md): the consumer dedups on a stable message id inside
the same transaction as its writes, so a redelivery is a no-op. The inbox only turns *at-least*
-once into exactly-once — it cannot recover a message the transport never delivered, so it does
**not** apply to pub/sub, which is at-most-once and can drop a message before the inbox ever
sees it. Choose an at-least-once model whenever a consumer must eventually see every message.

## Queue

`QueueSpec[M]` — a competing-consumers work queue. Each message is delivered to one consumer,
which `ack`s on success or `nack`s (with or without requeue) on failure; the broker redelivers
after a visibility timeout. No offsets, no replay — once acked, a message is gone. Reach for it
for **jobs and commands** where order across items does not matter and each item is handled
once.

## Ack-stream (Redis-class)

`AckStreamGroupQueryPort` — an ordered, replayable log consumed by a **consumer group** with a
Pending Entries List. Each entry is delivered to exactly one consumer in the group and stays
*pending* until that consumer `ack`s it; a consumer that crashes strands its pending entries,
and any worker recovers them with `claim` (transferring ownership of entries idle past a
threshold). Use it for **ordered events with competing consumers** on a Redis-class backend,
where you want per-message acks and explicit stranded-work recovery.

## Commit-stream (Kafka-class)

`CommitStreamGroupQueryPort` — a **partitioned, offset-committed log**. Instead of acking each
message, a consumer `commit`s a `StreamPosition`, and that single offset acknowledges every
message up to it on that partition (a high-water mark). Partitions are assigned across live
group members by the broker, so recovery from a crash is a **rebalance**, not a per-message
claim. The control plane (`CommitStreamGroupAdminPort`) provisions topics, positions a group,
inspects `lag`, and `reset_offsets` to **replay** from any offset or timestamp.

Consume it with the `CommitStreamGroupConsumer` runner, which commits the offset only **after**
`process_with_inbox` succeeds — never auto-commit, which would decouple the commit from
processing and silently drop the guarantee. On a poison message it either produces to a
dead-letter stream and commits past it (freeing the partition) or, with no dead-letter route,
pauses and alerts — never a silent skip. Reach for it for **high-throughput logs and event
sourcing** where partitioned ordering and replay matter.

!!! note "Ack vs commit"
    Both are consumer groups over an ordered log; the difference is the acknowledgment unit —
    per-message id (`ack`) versus per-partition offset (`commit`). That one axis is why the
    ports carry the `Ack` / `Commit` prefix.

## Pub/sub

`PubSubSpec[M]` — fire-and-forget fan-out to whoever is listening **right now**. A subscriber
offline at publish time misses the message; there is no ack and no replay. It is **at-most-once
past the broker** — legitimate for lossy broadcast (cache invalidation, presence, live
notifications), wrong for anything that must eventually be seen. When in doubt, pick a queue or
a stream.

## Choosing

- **Must every item be handled, order-independent?** → **Queue**.
- **Ordered events, competing consumers, per-message acks?** → **Ack-stream**.
- **Partitioned high-throughput log, replay, offset commits?** → **Commit-stream**.
- **Lossy broadcast to live listeners only?** → **Pub/sub**.

The contract surface for each lives in
[Streaming & pub/sub](../reference/contracts/streaming.md) and
[Messaging & the outbox](../reference/contracts/messaging.md).
