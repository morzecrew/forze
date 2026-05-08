# Stream contracts

Stream contracts model append-only ordered message streams. They support both
plain reads and consumer-group reads with acknowledgments.

## `StreamSpec[M]`

| Section | Details |
|---------|---------|
| Purpose | Names a stream namespace and the Pydantic model for stream entries. |
| Import path | `from forze.application.contracts.stream import StreamSpec` |
| Type parameters | `M`, the stream payload model. |
| Required fields | `name`, `model`. |
| Returned values | Passed to stream dep factories. |
| Common implementations | Mock stream adapter, Redis / Valkey streams. |
| Related dependency keys | `StreamQueryDepKey`, `StreamCommandDepKey`, `StreamGroupQueryDepKey`. |
| Minimal example | `audit_stream = StreamSpec(name="audit", model=AuditEntry)` |
| Related pages | [Redis / Valkey](../../integrations/redis.md). |

## `StreamQueryPort[M]`

| Section | Details |
|---------|---------|
| Purpose | Reads or tails messages from one or more streams. |
| Import path | `from forze.application.contracts.stream import StreamQueryPort` |
| Type parameters | `M`, the stream payload model. |
| Required methods | `read`, `tail`. |
| Returned values | `list[StreamMessage[M]]` or `AsyncIterator[StreamMessage[M]]`. |
| Common implementations | Mock, Redis / Valkey. |
| Related dependency keys | `StreamQueryDepKey`. |
| Minimal example | `entries = await reader.read({"audit": "0-0"}, limit=100)` |
| Related pages | [Pub/Sub contracts](pubsub.md). |

## `StreamGroupQueryPort[M]`

| Section | Details |
|---------|---------|
| Purpose | Reads, tails, and acknowledges stream entries for a consumer group. |
| Import path | `from forze.application.contracts.stream import StreamGroupQueryPort` |
| Type parameters | `M`, the stream payload model. |
| Required methods | `read`, `tail`, `ack`. |
| Returned values | Message lists/iterators; `ack` returns acknowledged count. |
| Common implementations | Mock stream group adapter, Redis / Valkey streams. |
| Related dependency keys | `StreamGroupQueryDepKey`. |
| Minimal example | `await group_reader.ack("workers", "audit", [entry["id"]])` |
| Related pages | [Background Workflow](../../recipes/background-workflow.md). |

## `StreamCommandPort[M]`

| Section | Details |
|---------|---------|
| Purpose | Appends a message to a stream. |
| Import path | `from forze.application.contracts.stream import StreamCommandPort` |
| Type parameters | `M`, the stream payload model. |
| Required methods | `append(stream, payload, *, type=None, key=None, timestamp=None)`. |
| Returned values | Stream entry id string. |
| Common implementations | Mock, Redis / Valkey. |
| Related dependency keys | `StreamCommandDepKey`. |
| Minimal example | `entry_id = await writer.append("audit", AuditEntry(...))` |
| Related pages | [Contracts overview](../contracts.md). |

## `StreamMessage[M]`

| Section | Details |
|---------|---------|
| Purpose | Typed entry shape returned by stream readers. |
| Import path | `from forze.application.contracts.stream import StreamMessage` |
| Type parameters | `M`, the stream payload model. |
| Required fields | `stream`, `id`, `payload`; optional `type`, `timestamp`, `key`. |
| Returned values | N/A; this is the returned value type. |
| Common implementations | `TypedDict` produced by stream adapters. |
| Related dependency keys | Produced through stream query dep keys. |
| Minimal example | `payload = entry["payload"]` |
| Related pages | [Mock integration](../../integrations/mock.md). |

    :::python
    from forze.application.contracts.stream import StreamCommandDepKey, StreamSpec

    audit_stream = StreamSpec(name="audit", model=AuditEntry)
    writer = ctx.dep(StreamCommandDepKey)(ctx, audit_stream)
    entry_id = await writer.append("audit", AuditEntry(action="created"))
