# Pub/Sub contracts

Pub/Sub contracts broadcast messages to topic subscribers. Use them when every
subscriber interested in a topic should see the published event.

## `PubSubSpec[M]`

| Section | Details |
|---------|---------|
| Purpose | Names a logical pub/sub namespace and message payload model. |
| Import path | `from forze.application.contracts.pubsub import PubSubSpec` |
| Type parameters | `M`, the Pydantic payload model. |
| Required fields | `name`, `model`. |
| Returned values | Passed to pub/sub dep factories. |
| Common implementations | Mock pub/sub adapter, Redis / Valkey pub/sub adapter. |
| Related dependency keys | `PubSubCommandDepKey`, `PubSubQueryDepKey`. |
| Minimal example | `events = PubSubSpec(name="events", model=EventPayload)` |
| Related pages | [Redis / Valkey](../../integrations/redis.md). |

## `PubSubCommandPort[M]`

| Section | Details |
|---------|---------|
| Purpose | Publishes one message to a topic. |
| Import path | `from forze.application.contracts.pubsub import PubSubCommandPort` |
| Type parameters | `M`, the Pydantic payload model. |
| Required methods | `publish(topic, payload, *, type=None, key=None, published_at=None)`. |
| Returned values | `None`. |
| Common implementations | Mock, Redis / Valkey. |
| Related dependency keys | `PubSubCommandDepKey`. |
| Minimal example | `await publisher.publish("projects.created", payload)` |
| Related pages | [Contracts overview](../contracts.md). |

## `PubSubQueryPort[M]`

| Section | Details |
|---------|---------|
| Purpose | Subscribes to one or more topics as an async iterator. |
| Import path | `from forze.application.contracts.pubsub import PubSubQueryPort` |
| Type parameters | `M`, the Pydantic payload model. |
| Required methods | `subscribe(topics, *, timeout=None)`. |
| Returned values | `AsyncIterator[PubSubMessage[M]]`. |
| Common implementations | Mock, Redis / Valkey. |
| Related dependency keys | `PubSubQueryDepKey`. |
| Minimal example | `async for message in subscriber.subscribe(["projects.created"]): ...` |
| Related pages | [Stream contracts](stream.md). |

## `PubSubMessage[M]`

| Section | Details |
|---------|---------|
| Purpose | Typed message shape yielded by pub/sub subscriptions. |
| Import path | `from forze.application.contracts.pubsub import PubSubMessage` |
| Type parameters | `M`, the Pydantic payload model. |
| Required fields | `topic`, `payload`; optional `type`, `published_at`, `key`. |
| Returned values | N/A; this is the returned value type. |
| Common implementations | `TypedDict` produced by pub/sub adapters. |
| Related dependency keys | Produced through `PubSubQueryDepKey` implementations. |
| Minimal example | `event = message["payload"]` |
| Related pages | [Mock integration](../../integrations/mock.md). |

    :::python
    from forze.application.contracts.pubsub import PubSubCommandDepKey, PubSubSpec

    events = PubSubSpec(name="events", model=EventPayload)
    publisher = ctx.dep(PubSubCommandDepKey)(ctx, events)
    await publisher.publish("projects.created", EventPayload(project_id="p1"))
