---
title: Contracts
icon: lucide/plug
summary: Every capability the application can resolve from the execution context
---

The narrative is in [Contracts & adapters](../core-concepts/contracts.md); this is
the index — each capability, the spec it's keyed by, how a handler reaches it from
the `ExecutionContext`, and the dep key it resolves under. Capabilities with a
dedicated reference page are linked (the spec fields, the method surface, and the
integrations that implement it).

Read the last column when you are **wiring** rather than calling: a dep key is what
a deps module binds, so it is the name you register a custom provider against and
the name a resolution error prints. Handlers should keep to the accessor.

## Data & storage

| Capability | Spec | Resolve via | Dep key |
|------------|------|-------------|---------|
| [Document](contracts/document.md) (read / write) | `DocumentSpec` | `ctx.document.query(spec)` / `ctx.document.command(spec)` | `DocumentQueryDepKey` / `DocumentCommandDepKey` |
| [Cache](contracts/stores.md#cache) | `CacheSpec` | `ctx.cache(spec)` | `CacheDepKey` |
| [Counter](contracts/stores.md#counter) | `CounterSpec` | `ctx.counter(spec)` | `CounterDepKey` (+ `CounterAdminDepKey` for reset / drop) |
| [Object storage](contracts/stores.md#storage) | `StorageSpec` | `ctx.storage.query(spec)` / `ctx.storage.command(spec)` | `StorageQueryDepKey` / `StorageCommandDepKey` (+ `StorageUploadSessionDepKey` for presigned uploads) |
| [Graph](contracts/graph.md) | `GraphModuleSpec` | `ctx.graph` | `GraphQueryDepKey` / `GraphCommandDepKey` (+ `GraphManagementDepKey`, `GraphRawQueryDepKey`) |
| Embeddings | `EmbeddingsSpec` | `ctx.embeddings.provider(spec)` | `EmbeddingsProviderDepKey` |
| [Dynamic read](../data-events/dynamic-read.md) | `DynamicReadSpec` | by dep key + route | `DynamicReadDepKey` |

## Search & analytics

| Capability | Spec | Resolve via | Dep key |
|------------|------|-------------|---------|
| [Search](contracts/search.md) (query / index) | `SearchSpec` | `ctx.search.query(spec)` / `ctx.search.command(spec)` | `SearchQueryDepKey` / `SearchCommandDepKey` (+ `SearchManagementDepKey` for index admin) |
| [Federated & hub search](contracts/search.md) | `SearchSpec` | `ctx.search.federated(spec)` / `ctx.search.hub(spec)` | `FederatedSearchQueryDepKey` / `HubSearchQueryDepKey` (+ `SearchResultSnapshotDepKey` for late materialization) |
| [Analytics](contracts/analytics.md) | `AnalyticsSpec` | `ctx.analytics.query(spec)` (+ ingest) | `AnalyticsQueryDepKey` / `AnalyticsIngestDepKey` |
| [Procedures](contracts/procedure.md) (command / compute) | `ProcedureSpec` | `ctx.procedure.command(spec)` | `ProcedureCommandDepKey` |

## Messaging & events

| Capability | Spec / key | Resolve via | Dep key |
|------------|-----------|-------------|---------|
| [Queue](contracts/messaging.md#queue) (produce / consume) | queue route | by dep key + route | `QueueCommandDepKey` / `QueueQueryDepKey` |
| [Pub/Sub](contracts/streaming.md#pubsub) | topic route | by dep key + route | `PubSubCommandDepKey` / `PubSubQueryDepKey` |
| [Stream](contracts/streaming.md#streams) | stream route | by dep key + route; `ctx.stream.commit_query` / `.commit_admin` for the commit sub-model | `StreamCommandDepKey` / `StreamQueryDepKey` (+ `AckStreamGroupQueryDepKey`, `AckStreamGroupAdminDepKey`, `CommitStreamGroupQueryDepKey`, `CommitStreamGroupAdminDepKey`) |
| [Outbox](contracts/messaging.md#outbox) | `OutboxSpec` | `ctx.outbox.command(spec)` / `ctx.outbox.query(spec)` | `OutboxCommandDepKey` / `OutboxQueryDepKey` (+ `OutboxAdminDepKey` for the relay) |
| [Inbox](contracts/messaging.md#inbox) | `InboxSpec` | `ctx.inbox` | `InboxDepKey` |
| Domain events | — | `ctx.domain` (dispatch) | `DomainEventDispatcherDepKey` |
| Saga | `SagaDefinition` | by dep key | `SagaExecutorDepKey` |
| [Realtime egress](../data-events/realtime.md) | `RealtimeEvent` catalog | `build_realtime_publisher(ctx, …)` ([wire protocol](realtime-protocol.md)) | — (composed from the stream and pub/sub keys above) |

## Reliability & coordination

| Capability | Spec / key | Resolve via | Dep key |
|------------|-----------|-------------|---------|
| Transactions | tx route | `ctx.tx_ctx.scope(route)` | `TransactionManagerDepKey` |
| [Idempotency](contracts/coordination.md#idempotency) | `IdempotencySpec` | `ctx.idempotency(spec)` | `IdempotencyDepKey` |
| [Resilience](resilience-tuning.md) | `ResiliencePolicy` | `ctx.resilience().run(fn, policy=…)` | `ResilienceExecutorDepKey` (+ `ResiliencePortPoliciesDepKey` for per-port defaults, `ResilienceAdminDepKey` for breaker state) |
| [Distributed lock](contracts/coordination.md#distributed-lock) | `DistributedLockSpec` | `ctx.dlock` | `DistributedLockCommandDepKey` / `DistributedLockQueryDepKey` |
| Hybrid logical clock | — | by dep key | `HlcCheckpointDepKey` |
| [Sandboxed execution](../data-events/sandbox.md) | `SandboxSpec` | `ctx.sandbox.run(spec)` | `SandboxDepKey` |

## Durable execution

| Capability | Spec | Resolve via | Dep key |
|------------|------|-------------|---------|
| [Durable workflows](contracts/durable.md#workflows) | `DurableWorkflowSpec` | by dep key | `DurableWorkflowCommandDepKey` / `DurableWorkflowQueryDepKey` |
| [Workflow schedules](contracts/durable.md#schedules) | — | by dep key | `DurableWorkflowScheduleCommandDepKey` / `DurableWorkflowScheduleQueryDepKey` |
| [Durable functions](contracts/durable.md#event-driven-functions) | `DurableFunctionSpec` | by dep key | `DurableFunctionEventCommandDepKey` / `DurableFunctionStepDepKey` |
| [Run & schedule stores, run listing](contracts/durable.md#event-driven-functions) | — | by dep key | `DurableRunStoreDepKey` / `DurableRunAdminDepKey` / `DurableScheduleStoreDepKey` |

## Identity & access

| Capability | Spec | Resolve via | Dep key |
|------------|------|-------------|---------|
| [Authentication](contracts/identity.md#authentication) | `AuthnSpec` | `ctx.authn` | `AuthnDepKey` (the lifecycle ports it composes are in [identity](contracts/identity.md)) |
| [Authorization](contracts/identity.md#authorization) | `AuthzSpec` | `ctx.authz.decision(spec)` / `ctx.authz.scope(spec)` | `AuthzDecisionDepKey` / `AuthzScopeDepKey` |
| [Tenancy](tenancy-matrix.md) | — | `ctx.tenancy` | `TenantResolverDepKey` (+ `TenantManagementDepKey` for provisioning) |
| Secrets | `SecretRef` | by dep key | `SecretsDepKey` (+ `SecretsAdminDepKey`, `SecretsLeaseDepKey`) |
| [Counterparty-rotated credentials](../running-in-prod/credential-rotation.md#when-the-other-side-rotates-it) | `SecretRef` | by dep key | `RotatingCredentialsDepKey` (+ `RotatingCredentialsAdminDepKey` for the refresh sweeper) |
| [Field encryption](../identity-tenancy-enc/encryption.md) | `FieldEncryption` | declared on a spec; applied by the plane, never called directly | `AeadDepKey` / `DeterministicCipherDepKey` (+ `RequiredReachDepKey` for the tier floor) |
| [Key management](../identity-tenancy-enc/encryption.md#wiring-the-keyring) | `KeyRef` | by dep key | `KeyringDepKey` / `KeyManagementDepKey` (+ `KeyDirectoryDepKey` for per-tenant key lookup) |

## Integration

| Capability | Spec | Resolve via | Dep key |
|------------|------|-------------|---------|
| Outbound HTTP | `HttpServiceSpec` | `ctx.http.service(spec)` | `HttpServiceDepKey` |
| [Model inference](../data-events/inference.md) | `InferenceSpec` | `ctx.inference.model(spec)` | `InferenceDepKey` |

A capability whose **Resolve via** cell reads *by dep key* has no short `ctx.<x>`
accessor: resolve it through the context's dependency resolution, by the key in the
last column and the route it is registered under. Streams are the mixed case —
shortcuts exist for the commit sub-model only, and the rest of the family resolves
by key.
