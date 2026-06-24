---
title: Contracts
icon: lucide/plug
summary: Every capability the application can resolve from the execution context
---

The narrative is in [Contracts & adapters](../core-concepts/contracts.md); this is
the index — each capability, the spec it's keyed by, and how a handler reaches it
from the `ExecutionContext`. For the **method signatures** on each port, see
[Document](contracts/document.md), [Cache/counter/storage](contracts/stores.md),
[Search](contracts/search.md), and [Queue/outbox/inbox](contracts/messaging.md).

## Data & storage

| Capability | Spec | Resolve via |
|------------|------|-------------|
| Document (read / write) | `DocumentSpec` | `ctx.document.query(spec)` / `ctx.document.command(spec)` |
| Cache | `CacheSpec` | `ctx.cache(spec)` |
| Counter | `CounterSpec` | `ctx.counter(spec)` |
| Object storage | `StorageSpec` | `ctx.storage.query(spec)` / `ctx.storage.command(spec)` |
| Graph | `GraphModuleSpec` | `ctx.graph` |
| Embeddings | `EmbeddingsSpec` | `ctx.embeddings` |

## Search & analytics

| Capability | Spec | Resolve via |
|------------|------|-------------|
| Search (query / index) | `SearchSpec` | `ctx.search.query(spec)` / `ctx.search.command(spec)` |
| Analytics | `AnalyticsSpec` | `ctx.analytics.query(spec)` (+ ingest) |
| Procedures (command / compute) | `ProcedureSpec` | `ctx.procedure.command(spec)` |

## Messaging & events

| Capability | Spec / key | Resolve via |
|------------|-----------|-------------|
| Queue (produce / consume) | queue route | `QueueQueryDepKey` / `QueueCommandDepKey` |
| Pub/Sub | topic route | `PubSubCommandDepKey` / `PubSubQueryDepKey` |
| Stream | stream route | `StreamCommandDepKey` / `StreamQueryDepKey` |
| Outbox | `OutboxSpec` | `ctx.outbox.command(spec)` / `ctx.outbox.query(spec)` |
| Inbox | `InboxSpec` | `ctx.inbox` |
| Domain events | — | `ctx.domain` (dispatch) |
| Saga | `SagaDefinition` | `SagaExecutorDepKey` |

## Reliability & coordination

| Capability | Spec / key | Resolve via |
|------------|-----------|-------------|
| Transactions | tx route | `ctx.tx_ctx.scope(route)` |
| Idempotency | `IdempotencySpec` | `ctx.idempotency(spec)` |
| Resilience | `ResiliencePolicy` | `ctx.resilience().run(fn, policy=…)` |
| Distributed lock | `DistributedLockSpec` | `ctx.dlock` |

## Durable execution

| Capability | Spec | Resolve via |
|------------|------|-------------|
| Durable workflows | `DurableWorkflowSpec` | `DurableWorkflowCommandDepKey` / `…QueryDepKey` |
| Workflow schedules | `DurableWorkflowScheduleSpec` | `DurableWorkflowScheduleCommandDepKey` / `…QueryDepKey` |
| Durable functions | `DurableFunctionSpec` | `DurableFunctionEventCommandDepKey` / `DurableFunctionStepDepKey` |

## Identity & access

| Capability | Spec | Resolve via |
|------------|------|-------------|
| Authentication | `AuthnSpec` | `ctx.authn` |
| Authorization | `AuthzSpec` | `ctx.authz.decision(spec)` / `ctx.authz.scope(spec)` |
| Tenancy | — | `ctx.tenancy` |
| Secrets | `SecretRef` | `SecretsPort` (`SecretsDepKey`) |

## Integration

| Capability | Spec | Resolve via |
|------------|------|-------------|
| Outbound HTTP | `HttpServiceSpec` | `ctx.http.service(spec)` |

Capabilities resolved by a `*DepKey` (queue, pub/sub, stream, durable) have no
short `ctx.<x>` accessor — resolve them through the context's dependency
resolution by their dep key and route.
