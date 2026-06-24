---
title: Contracts
icon: lucide/plug
summary: Every capability the application can resolve from the execution context
---

The narrative is in [Contracts & adapters](../core-concepts/contracts.md); this is
the index — each capability, the spec it's keyed by, and how a handler reaches it
from the `ExecutionContext`. Each capability links to its reference page (the spec
fields, the method surface, and the integrations that implement it).

## Data & storage

| Capability | Spec | Resolve via |
|------------|------|-------------|
| [Document](contracts/document.md) (read / write) | `DocumentSpec` | `ctx.document.query(spec)` / `ctx.document.command(spec)` |
| [Cache](contracts/stores.md#cache) | `CacheSpec` | `ctx.cache(spec)` |
| [Counter](contracts/stores.md#counter) | `CounterSpec` | `ctx.counter(spec)` |
| [Object storage](contracts/stores.md#storage) | `StorageSpec` | `ctx.storage.query(spec)` / `ctx.storage.command(spec)` |
| [Graph](contracts/graph.md) | `GraphModuleSpec` | `ctx.graph` |
| Embeddings | `EmbeddingsSpec` | `ctx.embeddings` |

## Search & analytics

| Capability | Spec | Resolve via |
|------------|------|-------------|
| [Search](contracts/search.md) (query / index) | `SearchSpec` | `ctx.search.query(spec)` / `ctx.search.command(spec)` |
| [Analytics](contracts/analytics.md) | `AnalyticsSpec` | `ctx.analytics.query(spec)` (+ ingest) |
| [Procedures](contracts/procedure.md) (command / compute) | `ProcedureSpec` | `ctx.procedure.command(spec)` |

## Messaging & events

| Capability | Spec / key | Resolve via |
|------------|-----------|-------------|
| [Queue](contracts/messaging.md#queue) (produce / consume) | queue route | `QueueQueryDepKey` / `QueueCommandDepKey` |
| [Pub/Sub](contracts/streaming.md#pubsub) | topic route | `PubSubCommandDepKey` / `PubSubQueryDepKey` |
| [Stream](contracts/streaming.md#streams) | stream route | `StreamCommandDepKey` / `StreamQueryDepKey` |
| [Outbox](contracts/messaging.md#outbox) | `OutboxSpec` | `ctx.outbox.command(spec)` / `ctx.outbox.query(spec)` |
| [Inbox](contracts/messaging.md#inbox) | `InboxSpec` | `ctx.inbox` |
| Domain events | — | `ctx.domain` (dispatch) |
| Saga | `SagaDefinition` | `SagaExecutorDepKey` |

## Reliability & coordination

| Capability | Spec / key | Resolve via |
|------------|-----------|-------------|
| Transactions | tx route | `ctx.tx_ctx.scope(route)` |
| [Idempotency](contracts/coordination.md#idempotency) | `IdempotencySpec` | `ctx.idempotency(spec)` |
| [Resilience](resilience-tuning.md) | `ResiliencePolicy` | `ctx.resilience().run(fn, policy=…)` |
| [Distributed lock](contracts/coordination.md#distributed-lock) | `DistributedLockSpec` | `ctx.dlock` |

## Durable execution

| Capability | Spec | Resolve via |
|------------|------|-------------|
| [Durable workflows](contracts/durable.md#workflows) | `DurableWorkflowSpec` | `DurableWorkflowCommandDepKey` / `…QueryDepKey` |
| [Workflow schedules](contracts/durable.md#schedules) | — | `DurableWorkflowScheduleCommandDepKey` / `…QueryDepKey` |
| [Durable functions](contracts/durable.md#event-driven-functions) | `DurableFunctionSpec` | `DurableFunctionEventCommandDepKey` / `DurableFunctionStepDepKey` |

## Identity & access

| Capability | Spec | Resolve via |
|------------|------|-------------|
| [Authentication](contracts/identity.md#authentication) | `AuthnSpec` | `ctx.authn` |
| [Authorization](contracts/identity.md#authorization) | `AuthzSpec` | `ctx.authz.decision(spec)` / `ctx.authz.scope(spec)` |
| [Tenancy](tenancy-matrix.md) | — | `ctx.tenancy` |
| Secrets | `SecretRef` | `SecretsPort` (`SecretsDepKey`) |

## Integration

| Capability | Spec | Resolve via |
|------------|------|-------------|
| Outbound HTTP | `HttpServiceSpec` | `ctx.http.service(spec)` |

Capabilities resolved by a `*DepKey` (queue, pub/sub, stream, durable) have no
short `ctx.<x>` accessor — resolve them through the context's dependency
resolution by their dep key and route.
