# Temporal Integration

## Page opening

`forze_temporal` connects Forze workflow contracts to Temporal. It provides a Temporal client, dependency module, lifecycle hooks, workflow command/query adapters, and context propagation interceptors so application code can schedule or inspect workflows through Forze ports.

| Topic | Details |
|------|---------|
| What it provides | `TemporalClient`, optional routed client, workflow adapters, lifecycle hooks, and `ExecutionContextInterceptor`. |
| Supported Forze contracts | `WorkflowCommandDepKey` and `WorkflowQueryDepKey`, plus `TemporalClientDepKey` for infrastructure access. |
| When to use it | Use this integration for durable background workflows, long-running orchestration, retries managed by Temporal, and task-queue based worker execution. |

## Installation

```bash
uv add 'forze[temporal]'
```

| Requirement | Notes |
|-------------|-------|
| Package extra | `temporal` installs `temporalio`. |
| Required service | A Temporal frontend service and one or more workers registered to the relevant task queue. |
| Local development dependency | Temporal dev server, Docker Compose, Temporal Cloud dev namespace, or another local Temporal-compatible endpoint. |

## Minimal setup

### Client

```python
from forze_temporal import TemporalClient, TemporalConfig

client = TemporalClient()
```

Use `RoutedTemporalClient` when tenant or route identity selects a Temporal namespace/cluster.

### Config

```python
from forze_temporal import TemporalWorkflowConfig

workflow_config = TemporalWorkflowConfig(
    queue="projects-task-queue",
    tenant_aware=True,
)
```

The `queue` value must match the Temporal task queue your workers poll.

### Deps module

```python
from forze.application.execution import DepsPlan
from forze_temporal import TemporalDepsModule

temporal_module = TemporalDepsModule(
    client=client,
    workflows={"project-workflow": workflow_config},
)

deps_plan = DepsPlan.from_modules(temporal_module)
```

The route key should match your `WorkflowSpec.name`.

### Lifecycle step

```python
from forze.application.execution import LifecyclePlan
from forze_temporal import temporal_lifecycle_step

lifecycle = LifecyclePlan.from_steps(
    temporal_lifecycle_step(
        host="localhost:7233",
        config=TemporalConfig(namespace="default"),
    )
)
```

Use `routed_temporal_lifecycle_step(client=routed_temporal)` with `RoutedTemporalClient` and do not combine routed and non-routed lifecycle steps for the same client.

## Contract coverage table

| Forze contract | Adapter implementation | Dependency key/spec name | Limitations |
|----------------|------------------------|--------------------------|-------------|
| Workflow commands | `ConfigurableTemporalWorkflowCommand` / Temporal workflow command adapter. | `WorkflowCommandDepKey`, route usually equal to `WorkflowSpec.name`. | Requires a worker to register the workflow/activity implementation and poll the configured task queue. |
| Workflow queries | `ConfigurableTemporalWorkflowQuery` / Temporal workflow query adapter. | `WorkflowQueryDepKey`, route usually equal to `WorkflowSpec.name`. | Query availability depends on Temporal workflow state and query handlers. |
| Raw Temporal client | `TemporalClient` or `RoutedTemporalClient`. | `TemporalClientDepKey`. | Prefer workflow contracts in usecases to keep Temporal details at the infrastructure edge. |
| Context propagation | `ExecutionContextInterceptor`. | Configured in `TemporalConfig.interceptors`. | Only propagates context fields supported by the interceptor and Temporal payload/headers. |

## Complete recipe link

See [Background Workflow](../recipes/background-workflow.md) for a complete workflow-oriented recipe.

## Configuration reference

### Connection settings

`TemporalClient` connects to `target_host` and uses `TemporalConfig.namespace`. Configure TLS, API keys, or cloud endpoints according to your Temporal deployment and client construction.

### Pool settings

Temporal client connection pooling is handled by the Temporal SDK. Worker concurrency, activity slots, and task queue polling are configured in your worker process rather than the Forze dependency module.

### Serialization settings

Temporal payload serialization is controlled by the Temporal SDK and any interceptors/converters you configure. Keep workflow input/output models stable because Temporal histories may outlive code deployments.

### Retry/timeout behavior

Temporal workflow/activity retry policies and timeouts belong to workflow and activity definitions. Forze schedules and queries workflows through ports; Temporal owns durable retries after a workflow starts.

## Operational notes

| Concern | Notes |
|---------|-------|
| Migrations/schema requirements | Temporal server persistence is managed by Temporal. Application-level workflow versioning and compatibility are your responsibility. |
| Cleanup/shutdown | Register `temporal_lifecycle_step` or `routed_temporal_lifecycle_step` so clients connect on startup and close on shutdown. Stop workers gracefully so in-flight activities can finish or heartbeat. |
| Idempotency/caching behavior | Temporal workflow IDs are the usual deduplication boundary. Choose deterministic IDs when scheduling must be idempotent. |
| Production caveats | Version workflows with Temporal-safe patterns, keep task queue names explicit, monitor stuck workflows, and avoid non-deterministic code in workflow definitions. |

## Troubleshooting

| Common error | Likely cause | Fix |
|--------------|--------------|-----|
| Workflow never starts | No worker is polling the configured task queue or the queue name differs. | Start a worker for the same task queue configured in `TemporalWorkflowConfig.queue`. |
| Namespace not found | Client namespace does not exist in Temporal. | Create the namespace or change `TemporalConfig.namespace`. |
| Non-determinism failure during replay | Workflow code changed in a way Temporal cannot replay. | Use Temporal workflow versioning patterns and avoid non-deterministic calls inside workflows. |
| Context is missing in workflow/activity | The context interceptor was not configured or the field is not propagated. | Add `ExecutionContextInterceptor` to `TemporalConfig.interceptors` and verify supported context fields. |
