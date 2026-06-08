# Temporal Integration

## Page opening

`forze_temporal` connects Forze durable workflow contracts to Temporal. It provides a Temporal client, dependency module, lifecycle hooks, workflow and workflow-schedule adapters, and context propagation interceptors so application code can start, schedule, or inspect workflows through Forze ports.

| Topic | Details |
|------|---------|
| What it provides | `TemporalClient`, optional routed client, workflow adapters, lifecycle hooks, and `ExecutionContextInterceptor`. |
| Supported Forze contracts | `DurableWorkflowCommandDepKey`, `DurableWorkflowQueryDepKey`, `DurableWorkflowScheduleCommandDepKey`, `DurableWorkflowScheduleQueryDepKey`, plus `TemporalClientDepKey` for infrastructure access. |
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
from forze.application.execution import DepsRegistry
from forze_temporal import TemporalDepsModule

temporal_module = TemporalDepsModule(
    client=client,
    workflows={"project-workflow": workflow_config},
)

deps_registry = DepsRegistry.from_modules(temporal_module)
```

The route key should match your `DurableWorkflowSpec.name`.

For framework tests or advanced wiring, prefer `from forze_temporal.execution.deps import ConfigurableTemporalWorkflowQuery`, `ConfigurableTemporalWorkflowCommand`, `ConfigurableTemporalWorkflowScheduleQuery`, and `ConfigurableTemporalWorkflowScheduleCommand` rather than removed `forze_temporal.execution.deps.deps` paths.

### Lifecycle step

```python
from forze.application.execution import LifecyclePlan
from forze_temporal import temporal_lifecycle_step

lifecycle = LifecyclePlan.from_steps(
    temporal_lifecycle_step(
        host="localhost:7233",
        config=TemporalConfig(namespace="default"),
        workflow_configs={"project-workflow": workflow_config},
    )
)
```

Use `routed_temporal_lifecycle_step(client=routed_temporal)` with `RoutedTemporalClient` and do not combine routed and non-routed lifecycle steps for the same client.

### Schedule bootstrap

Register declarative schedules on `TemporalDepsModule` and pass the same workflow
config map to the lifecycle step so schedules are upserted after the client connects:

```python
from datetime import timedelta

from forze.application.contracts.durable.workflow import (
    DurableWorkflowScheduleBootstrap,
    DurableWorkflowScheduleTiming,
)
from forze_temporal import TemporalDepsModule, temporal_lifecycle_step

bootstrap = DurableWorkflowScheduleBootstrap(
    workflow_name="project-workflow",
    schedule_id="project-nightly",
    default_args=StartProjectSync(project_id="default"),
    timing=DurableWorkflowScheduleTiming(cron_expressions=("0 2 * * *",)),
)

temporal_module = TemporalDepsModule(
    client=client,
    workflows={"project-workflow": workflow_config},
    schedule_bootstraps=[bootstrap],
)

lifecycle = LifecyclePlan.from_steps(
    temporal_lifecycle_step(
        host="localhost:7233",
        workflow_configs={"project-workflow": workflow_config},
    )
)
```

Runtime schedule management uses `DurableWorkflowScheduleCommandDepKey` /
`DurableWorkflowScheduleQueryDepKey` (see [Durable workflow schedule contracts](../core-package/contracts/durable-workflow-schedule.md)).
Temporal **Schedules** require a server that implements the Schedules API (not the
time-skipping test environment). Schedule integration tests use a Docker
``temporalio/temporal`` dev server (``server start-dev``) via testcontainers.

When a schedule fires, Temporal typically **suffixes** the configured workflow id
with the scheduled time (for example ``my-run-2026-05-27T16:49:42Z``). Use
``describe`` on the schedule handle (or workflow search) to resolve the actual run id.

## Contract coverage table

| Forze contract | Adapter implementation | Dependency key/spec name | Limitations |
|----------------|------------------------|--------------------------|-------------|
| Durable workflow commands | `ConfigurableTemporalWorkflowCommand` / Temporal workflow command adapter. | `DurableWorkflowCommandDepKey`, route usually equal to `DurableWorkflowSpec.name`. | Requires a worker to register the workflow/activity implementation and poll the configured task queue. |
| Durable workflow queries | `ConfigurableTemporalWorkflowQuery` / Temporal workflow query adapter. | `DurableWorkflowQueryDepKey`, route usually equal to `DurableWorkflowSpec.name`. | `describe()` maps Temporal execution status; app `query()` handlers are optional for domain progress. |
| Durable workflow schedule commands | `ConfigurableTemporalWorkflowScheduleCommand` / schedule command adapter. | `DurableWorkflowScheduleCommandDepKey`, route usually equal to `DurableWorkflowSpec.name`. | Uses Temporal Schedules API (`create_schedule`, pause, trigger, etc.). |
| Durable workflow schedule queries | `ConfigurableTemporalWorkflowScheduleQuery` / schedule query adapter. | `DurableWorkflowScheduleQueryDepKey`, route usually equal to `DurableWorkflowSpec.name`. | `list()` filters to schedules whose action targets the workflow name. |
| Raw Temporal client | `TemporalClient` or `RoutedTemporalClient`. | `TemporalClientDepKey`. | Prefer workflow contracts in handlers to keep Temporal details at the infrastructure edge. |
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

Temporal workflow/activity retry policies and timeouts belong to workflow and activity definitions. Forze **starts** workflows and **manages schedule resources** through ports; Temporal owns durable retries after a workflow starts.

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
| `RuntimeError: Failed validating workflow <name>` | A process-wide import hook breaks the workflow sandbox's per-workflow module re-import. Most commonly `beartype.claw`, installed transitively by the MCP integration's `fastmcp` → `py-key-value-aio` dependency. | Build the worker with Forze's sandbox runner: `Worker(..., workflow_runner=sandboxed_workflow_runner())` (from `forze_temporal`), which passes `beartype` through the sandbox. |
| Workflow hangs indefinitely when running under `coverage` (Python 3.14) | `coverage`'s `sys.monitoring` tracer fires inside sandboxed workflow code and lazily imports `coverage.env`, which calls restricted `platform.python_implementation()` → `RestrictedWorkflowAccessError` fails the workflow task, and Temporal retries task failures forever. | Use `Worker(..., workflow_runner=sandboxed_workflow_runner())`, which also passes `coverage` through the sandbox. |
