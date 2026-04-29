# Temporal Integration

`forze_temporal` connects [Temporal.io](https://temporal.io) to Forze’s workflow contracts. It provides a **`TemporalClient`** wrapper around the Temporal Python SDK (Pydantic data conversion by default), **`TemporalDepsModule`** for dependency registration, **command and query adapters** implementing `WorkflowCommandPort` and `WorkflowQueryPort`, and optional **interceptors** so `ExecutionContext` (call context and `AuthIdentity`) flows through client and worker.

Kernel specs use logical workflow names (`WorkflowSpec.name`). **`TemporalDepsModule`** maps each name to a **`TemporalWorkflowConfig`** (task queue and optional multi-tenancy). See [Specs and infrastructure wiring](../core-concepts/specs-and-wiring.md).

## Installation

    :::bash
    uv add 'forze[temporal]'

The extra pulls `temporalio` and related dependencies.

## What ships in `forze_temporal`

| Area | Types |
|------|--------|
| Client | `TemporalClient`, `TemporalConfig` (`forze_temporal.kernel.platform`) |
| Wiring | `TemporalDepsModule`, `TemporalClientDepKey`, `TemporalWorkflowConfig`, `temporal_lifecycle_step` (`forze_temporal.execution`) |
| Adapters | `TemporalWorkflowCommandAdapter`, `TemporalWorkflowQueryAdapter` (`forze_temporal.adapters`) |
| Context propagation | `ExecutionContextInterceptor`, `TemporalContextCodec` (`forze_temporal.interceptors`) |

## Core contracts (forze)

Workflows are described with **`WorkflowSpec`**: a logical **`name`**, a **`run`** invocation (`WorkflowInvokeSpec` with Pydantic `args_type` / `return_type`), and optional **`signals`**, **`queries`**, and **`updates`** maps. Ports are split into:

- **`WorkflowCommandPort`**: `start`, `signal`, `update`, `cancel`, `terminate`
- **`WorkflowQueryPort`**: `query`, `result`

Both ports carry the same **`WorkflowSpec`** (`spec` field). Operations use typed **`WorkflowHandle`** values (workflow id and optional run id) returned from `start`.

Dependency injection uses **routed** factories (same pattern as documents or cache):

| Key | Role |
|-----|------|
| `WorkflowCommandDepKey` | `WorkflowCommandDepPort` — given `ExecutionContext` + `WorkflowSpec`, returns a `WorkflowCommandPort` |
| `WorkflowQueryDepKey` | `WorkflowQueryDepPort` — returns a `WorkflowQueryPort` |

The **route** for resolution is `WorkflowSpec.name`. There is no `ctx.workflow_command(...)` helper on `ExecutionContext`; resolve the factory with `dep(..., route=spec.name)` and call it with the spec (see below).

## Runtime wiring

Construct a **`TemporalClient`** (not connected until lifecycle **`initialize`**), register workflows by name, and merge the module into your **`DepsPlan`**. Use **`temporal_lifecycle_step`** so the client connects during application startup.

    :::python
    from forze.application.execution import DepsPlan, ExecutionRuntime, LifecyclePlan

    from forze_temporal.execution import TemporalDepsModule, temporal_lifecycle_step
    from forze_temporal.kernel.platform import TemporalClient, TemporalConfig

    temporal = TemporalClient()
    module = TemporalDepsModule(
        client=temporal,
        workflows={
            "ProjectOnboarding": {"queue": "project-tasks", "tenant_aware": True},
            "BillingRun": {"queue": "billing"},
        },
    )

    runtime = ExecutionRuntime(
        deps=DepsPlan.from_modules(module),
        lifecycle=LifecyclePlan.from_steps(
            temporal_lifecycle_step(
                host="localhost:7233",
                config=TemporalConfig(namespace="default"),
            )
        ),
    )

Keys in **`workflows`** must match **`WorkflowSpec.name`** for each workflow you resolve from usecases. If **`workflows`** is empty, only **`TemporalClientDepKey`** is registered (no workflow routes).

### `TemporalWorkflowConfig`

| Field | Purpose |
|-------|---------|
| `queue` | Temporal task queue passed to `start_workflow` |
| `tenant_aware` (optional) | If true, default workflow ids are prefixed with the current tenant (`tenant:{uuid}:...`); requires tenant context when generating ids |

### `TemporalConfig` (client)

| Field | Default | Purpose |
|-------|---------|---------|
| `namespace` | `"default"` | Temporal namespace |
| `lazy` | `False` | Lazy client initialization |
| `interceptors` | `None` | SDK interceptors; use this to attach **`ExecutionContextInterceptor`** (see below) |

The platform client uses **`pydantic_data_converter`** from `temporalio.contrib.pydantic` for workflow arguments and results.

## Using workflow ports in usecases

Resolve the routed **factory** with `WorkflowSpec.name` as the route, then invoke it with the **`ExecutionContext`** and your **`WorkflowSpec`**:

    :::python
    from forze.application.contracts.workflow import WorkflowCommandDepKey, WorkflowSpec
    from forze.application.execution import Usecase


    project_onboarding_spec: WorkflowSpec = ...  # defined once in your app


    class StartProjectOnboarding(Usecase[UUID, None]):
        async def main(self, args: UUID) -> None:
            factory = self.ctx.dep(
                WorkflowCommandDepKey,
                route=project_onboarding_spec.name,
            )
            cmd = factory(self.ctx, project_onboarding_spec)

            handle = await cmd.start(
                ProjectOnboardingIn(project_id=args),
                workflow_id=f"project-onboarding-{args}",
            )
            # persist handle.workflow_id / handle.run_id if needed


    class SignalProjectOnboarding(Usecase[SignalArgs, None]):
        async def main(self, args: SignalArgs) -> None:
            factory = self.ctx.dep(
                WorkflowCommandDepKey,
                route=project_onboarding_spec.name,
            )
            cmd = factory(self.ctx, project_onboarding_spec)

            await cmd.signal(
                args.handle,
                signal=project_onboarding_spec.signals["step_completed"],
                args=StepCompletedSignal(step=args.step, result=args.result),
            )

For reads and **`result`**, resolve **`WorkflowQueryDepKey`** the same way with **`route=spec.name`** and use **`WorkflowQueryPort`**.

## Adapters (direct use)

Integration tests and advanced setups can build adapters without the deps module:

- **`TemporalWorkflowCommandAdapter`**: `WorkflowCommandPort` — needs `client`, `queue`, `spec`, and optional **`tenant_aware`** / **`tenant_provider`** / **`workflow_id_factory`**
- **`TemporalWorkflowQueryAdapter`**: `WorkflowQueryPort` — same constructor fields

Both extend **`TemporalBaseAdapter`**: when **`tenant_aware`** is true, **`construct_workflow_id`** prefixes ids with the tenant from **`tenant_provider`** (typically `ctx.get_tenant_id` when using **`ConfigurableTemporalWorkflowCommand`** / **`ConfigurableTemporalWorkflowQuery`** from the deps module).

## Execution context in Temporal

To propagate **`ExecutionContext`** (e.g. correlation id) across Temporal headers, register **`ExecutionContextInterceptor`** on the client **`TemporalConfig.interceptors`** and use the same interceptor type on workers. See `forze_temporal.interceptors.ExecutionContextInterceptor` and integration tests under `tests/integration/test_forze_temporal_integration/`.

## Lifecycle and shutdown

**`temporal_lifecycle_step`** registers a **startup** hook that calls **`TemporalClient.initialize(host, config=...)`**. The Temporal SDK does not require explicit teardown for correctness; if you want to clear the client reference on shutdown, add a separate **`LifecycleStep`** whose shutdown calls **`TemporalClient.close()`**.

## Why this shape

- **Ports stay stable**: usecases depend on **`WorkflowCommandPort` / `WorkflowQueryPort`** and **`WorkflowSpec`**, not on `temporalio` types.
- **Temporal details stay in adapters**: task queues, workflow handles, and SDK calls live in **`forze_temporal`**.
- **Same wiring model as other integrations**: routed keys, **`Deps.merge`**, and lifecycle steps match Redis, Postgres, and the rest of Forze.
