---
title: Kafka
icon: lucide/list-ordered
summary: Produce and consume a partitioned, offset-committed log — the commit-stream delivery model on Kafka or any Kafka-protocol broker
---

`forze[kafka]` implements the **commit-stream** (offset-log) contracts on Apache
Kafka or any Kafka-protocol broker — an ordered, partitioned, replayable log
produced and consumed behind the stream ports, with consumer groups that
acknowledge by committed offset. It is the production backend for the
[commit-stream delivery model](../data-events/messaging-delivery-models.md#commit-stream-kafka-class).

## Install

```bash
uv add 'forze[kafka]'
```

Needs a reachable Kafka-protocol broker (the client is `aiokafka`). Protocol
compatibility is what matters, not the vendor — Apache Kafka and Redpanda both
work.

## The client

```python
from forze_kafka import KafkaClient

kafka = KafkaClient()
```

The client owns one shared producer and admin client, and pools data-plane
consumers per `(group, member, topics)`. `RoutedKafkaClient` resolves a
per-tenant cluster instead (with `routed_kafka_lifecycle_step`).

## Wire it

Register produce routes (`streams`) and consumer-group routes (`commit_groups`),
keyed by `StreamSpec.name`; connect from the lifecycle plan:

```python
from forze.application.execution import DepsRegistry, LifecyclePlan
from forze_kafka import (
    KafkaClient,
    KafkaCommitStreamGroupConfig,
    KafkaConfig,
    KafkaDepsModule,
    KafkaStreamConfig,
    kafka_lifecycle_step,
)

deps = DepsRegistry.from_modules(
    KafkaDepsModule(
        client=kafka,
        streams={"events": KafkaStreamConfig()},
        commit_groups={"events": KafkaCommitStreamGroupConfig()},
    ),
)
lifecycle = LifecyclePlan.from_steps(
    kafka_lifecycle_step(bootstrap_servers="localhost:9092", config=KafkaConfig()),
)
```

`KafkaConfig` defaults are chosen for the offset-log guarantee: `acks="all"`
with an **idempotent producer** (broker-side dedup of retried produces), and no
auto-commit knob at all — the consumer runner commits explicitly, after
processing. Security is `PLAINTEXT` by default; set `security_protocol` (and the
`sasl_*` fields) for authenticated clusters.

## What it provides

| Contract | Methods | Keyed by |
|----------|---------|----------|
| Stream produce | `append` | `StreamSpec.name` (`streams`) |
| Commit-stream consumer group | `read`, `tail`, `commit`, `seek_to_committed` | `StreamSpec.name` (`commit_groups`) |
| Commit-stream admin | `ensure_topic`, `ensure_group`, `reset_offsets`, `lag` | `StreamSpec.name` (`commit_groups`) |

The contract surface is
[Streaming & pub/sub](../reference/contracts/streaming.md); the
[transactional outbox](../recipes/transactional-outbox.md) relay's `to_stream`
publishes through the same produce port, so outbox → Kafka needs no extra
wiring.

## The delivery model

A produced message's `key` becomes the native Kafka message key — same key, same
partition, so **per-key ordering** holds; `headers` ride native record headers.
Consumption is a consumer group over the partitioned log: a single committed
offset acknowledges every message up to it on that partition, and the broker
reassigns partitions across live members on a **rebalance**.

The transport is **at-least-once**. Pair the consumer with the
[inbox](../reference/contracts/messaging.md) — dedup on a stable message id
inside the same transaction as the handler's writes — and a redelivery is a
no-op: **exactly-once effect**, the same equation as every other at-least-once
backend. The dedup id is the outbox event id when the message carries one, else
the canonical `stream:partition:offset` — never `key`, which many distinct
events share.

## Consuming

`CommitStreamGroupConsumer` (from `forze_kits.integrations.consumer`) is the
consume loop: read a batch, process each message through `process_with_inbox` in
one transaction, and **commit the offset only after the handler commits** —
never auto-commit, which would decouple acknowledgment from processing. Run it
supervised as a lifecycle step:

```python
from forze_kits.integrations.consumer import (
    commit_stream_consumer_background_lifecycle_step,
)

step = commit_stream_consumer_background_lifecycle_step(
    topics=["events"],
    group="projections",
    consumer="worker-1",
    stream_spec=events_stream,
    handler=handle_event,
    inbox_spec=inbox,
    tx_route="main",
    max_attempts=3,
    dlq_stream="events.dlq",
)
```

Startup spawns a consume-forever task; shutdown cancels it. A **crash** of the
consume itself (broker connection loss, a transient fault) restarts the run
after a jittered backoff — and the restart is **loss-free**: the supervisor
first rewinds the group to its committed offset, so records fetched but not yet
committed are re-fetched, never skipped, then deduped by the inbox. One step
runs one sequential consumer; scale out with more steps or processes and let the
broker rebalance partitions across members.

## Poison and rebalance

A log cannot requeue one message without wedging its partition, so failure
handling is offset-shaped:

- **Handler poison** — a message that exhausts `max_attempts` is produced to
  `dlq_stream` **as received** and the offset is committed past it, freeing the
  partition. An end-to-end-sealed envelope is forwarded *still sealed*, with the
  event-id/tenant headers its AAD is bound to — the dead-letter copy stays
  decryptable and correlates back to the source event. With no `dlq_stream`, the
  run **pauses and alerts** instead: the offset stays uncommitted for
  redelivery, never silently skipped.
- **Decode or decrypt poison** — a malformed record never aborts its batch (the
  read path substitutes a poison marker instead of raising); it, and an
  undecryptable envelope (tampering, unknown key), always pause-and-alert —
  there is no decodable payload to forward to a dead-letter stream, so an
  operator must inspect it.
- **A transient decrypt fault is not poison** — the keyring's KMS being
  unavailable or throttled while unwrapping a cold data key is crash-shaped: the
  consumer commits the successes so far and raises, and the supervisor restarts
  it with backoff; the message is redelivered once the blip clears. Only a
  *non-retryable* decrypt failure pauses.
- **Pause is consumer-wide.** A pause stops every subscribed topic of that
  consumer, and the supervisor deliberately does **not** restart it (a restart
  would re-fetch the same poison and pause again) — it alerts and waits for an
  operator. Run one consumer per isolation boundary if poison in one topic must
  not stall others.

A **rebalance** is routine, not a failure: when the broker revokes partitions
mid-batch, the adapter drops their routing so a late commit on a revoked
partition is skipped (the records are redelivered to the new owner and deduped
by the inbox) rather than crashing the run, and freshly assigned partitions are
sought back to their committed offset so nothing is skipped or double-read.

## Replay and lag

The admin port is the control plane. `ensure_topic` provisions a topic
idempotently; `ensure_group` positions a fresh group; `reset_offsets` **replays**
by repositioning the group's committed offset — to `OffsetReset.EARLIEST` /
`LATEST`, `OffsetReset.at_timestamp(...)`, or `OffsetReset.at_offset(...)`; and
`lag` reports each partition's committed offset against the log end:

```python
admin = ctx.stream.commit_admin(events_stream)

await admin.reset_offsets("projections", "events", to=OffsetReset.EARLIEST)
for entry in await admin.lag("projections"):
    print(entry.stream, entry.partition, entry.end_offset - entry.committed_offset)
```

## Encryption

A `StreamSpec` route declares `encryption="end_to_end"` to seal payloads
**through the broker**: the producer encrypts, Kafka only ever stores
ciphertext, and the consumer decrypts after inbox dedup. A wired
`CryptoDepsModule` is required, and a deployment `required_reach` floor is
enforced at wiring; there is no `at_rest` tier on this transport (nothing
between producer and consumer decrypts). See
[Encryption](../identity-tenancy-enc/encryption.md#outbox-and-inbox) for the
tier model.

## Tenancy

Kafka offers two isolation tiers (there is no `tagged` tier — a topic has no
server-side row filter):

- **`namespace`** — set a per-tenant `namespace` on the route config; the topic
  is prefixed per tenant (`{namespace}.{topic}`), resolved from the bound tenant
  on every produce, read, and admin call.
- **`dedicated`** — a `RoutedKafkaClient` resolves a per-tenant cluster; topic
  names stay shared.

Declare a floor with `KafkaDepsModule(required_tenant_isolation=...)` and wiring
refuses any route below it, fail-closed — the same
[declared-minimum model](../identity-tenancy-enc/multi-tenancy.md) as every
other integration.

## Notes

- **Ordering is per partition.** Same key, same partition, in order; across
  keys and partitions there is no global order — design consumers accordingly.
- **Provision topics deliberately.** `ensure_topic` is available (and
  idempotent), but partition count and replication are capacity decisions;
  broker-side topology usually lives with your infrastructure config.
- **`auto_offset_reset`** (`latest` by default, per client or per group config)
  only applies when a group has no committed offset — a first consume; after
  that the committed offset always wins.
- **Alert on a paused consumer.** A run that returns with `failed > 0` is a
  stopped consumer awaiting an operator — surface it in your alerting.
