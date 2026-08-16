# Pub/sub and streams

## Pub/sub contracts

Use pub/sub for broadcast-style events where subscribers receive messages by topic.

```python
from forze.application.contracts.pubsub import PubSubCommandDepKey, PubSubSpec

events = PubSubSpec(
    name=ResourceName.ORDERS,
    codec=PydanticModelCodec(OrderPayload),
)
publisher = ctx.deps.resolve_configurable(
    ctx, PubSubCommandDepKey, events, route=events.name
)
await publisher.publish("orders.created", payload, type="order.created")
```

`MockDepsModule` registers pub/sub factories. For production, `RedisDepsModule` exposes a `pubsub={route: RedisPubSubConfig()}` map that registers `PubSubQueryDepKey` / `PubSubCommandDepKey` for those routes.

## Stream contracts

Streams model append-only logs and consumer-group reads.

```python
from forze.application.contracts.stream import StreamCommandDepKey, StreamSpec

stream_spec = StreamSpec(
    name=ResourceName.ORDERS,
    codec=PydanticModelCodec(OrderPayload),
)
stream = ctx.deps.resolve_configurable(
    ctx, StreamCommandDepKey, stream_spec, route=stream_spec.name
)
entry_id = await stream.append("orders", payload, type="order.created")
```

Use `StreamQueryDepKey` for `read` / `tail`. Consumer groups come in two disciplines: `AckStreamGroupQueryDepKey` for per-message ack + `claim` recovery (Redis-class), and `CommitStreamGroupQueryDepKey` for per-partition offset `commit` on a Kafka-class log (with a `CommitStreamGroupAdminDepKey` for `ensure_topic` / `ensure_group` / `reset_offsets` / `lag`). `MockDepsModule` registers all of them. In production, `RedisDepsModule` wires the ack discipline via `streams={route: RedisStreamConfig()}` (stream query/command + `AckStreamGroup*` keys), and `KafkaDepsModule` wires the commit discipline via `streams=` / `commit_groups=` (`CommitStreamGroupQueryDepKey` / `CommitStreamGroupAdminDepKey`).

## Processing rules

- Ack only after the business operation succeeds.
- Use `nack(..., requeue=True)` for transient failures and `requeue=False` when the message should move toward DLQ/provider handling.
- Prefer idempotent consumers; message brokers can redeliver.
- Wrap document mutations and enqueue/outbox-style side effects with transactions and `defer_after_commit` when duplicate or premature events would hurt.

## Shutdown

Consumers stop **gracefully**, not by cancellation: a running consumer accepts a stop signal and finishes the unit of work in hand before returning, and a commit-stream consumer commits the offsets it processed even when the stop lands mid-batch. A custom consumer or signal source you write must accept the stop signal too — one that ignores it gets torn down with work in flight.

A graceful shutdown is also not a poison verdict. An offset-log consumer refused by the drain gate stops with its offset **uncommitted** for redelivery rather than dead-lettering a healthy message.

An outbox relay can `drain_on_shutdown` — publish what is claimable before teardown — and a relay that names a transport spec nothing ever provides is now **rejected at construction** instead of silently dropping the route.

## Anti-patterns

- **Acknowledging before processing succeeds** — failures become data loss.
- **Mixing consumer-group disciplines** — Redis streams are ack-discipline (`AckStreamGroup*`), Kafka is commit-discipline (`CommitStreamGroup*`); a handler written for one does not port to the other unchanged.

## Reference

- [Kafka integration](https://morzecrew.github.io/forze/latest/integrations/kafka/)
- [Messaging delivery models](https://morzecrew.github.io/forze/latest/data-events/messaging-delivery-models/)
- [Stream contracts](https://morzecrew.github.io/forze/latest/reference/contracts/streaming/)
