"""Lifecycle hooks for Temporal client initialization and shutdown."""

from collections.abc import Mapping
from typing import Any, cast, final

import attrs

from forze.application.contracts.durable.workflow import (
    DurableWorkflowInvokeSpec,
    DurableWorkflowSpec,
)
from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze.application.execution import ExecutionContext
from forze.base.exceptions import exc

from ..adapters.schedule import TemporalWorkflowScheduleCommandAdapter
from ..kernel.platform import RoutedTemporalClient, TemporalClient, TemporalConfig
from .deps import TemporalClientDepKey
from .deps.configs import TemporalWorkflowConfig
from .deps.keys import TemporalScheduleBootstrapDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class TemporalStartupHook(LifecycleHook):
    """Startup hook that initializes the Temporal client from the deps container."""

    host: str
    """Connection host for the Temporal server."""

    config: TemporalConfig = attrs.field(factory=TemporalConfig, repr=False)
    """Configuration for the Temporal client."""

    bootstrap_schedules: bool = True
    """Whether to upsert declarative schedules after the client connects."""

    workflow_configs: Mapping[str, TemporalWorkflowConfig] | None = attrs.field(
        default=None,
        repr=False,
    )
    """Workflow route configs keyed by workflow name (for schedule bootstrap)."""

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        temporal_client = cast(TemporalClient, ctx.deps.provide(TemporalClientDepKey))
        await temporal_client.initialize(self.host, config=self.config)

        if self.bootstrap_schedules:
            await _bootstrap_schedules(ctx, workflow_configs=self.workflow_configs)


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class TemporalShutdownHook(LifecycleHook):
    """Shutdown hook that releases the Temporal client reference."""

    async def __call__(self, ctx: ExecutionContext) -> None:
        temporal_client = ctx.deps.provide(TemporalClientDepKey)
        await temporal_client.close()


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class RoutedTemporalStartupHook(LifecycleHook):
    """Startup hook that marks a :class:`RoutedTemporalClient` as ready."""

    client: RoutedTemporalClient

    bootstrap_schedules: bool = True
    """Whether to upsert declarative schedules after startup."""

    workflow_configs: Mapping[str, TemporalWorkflowConfig] | None = attrs.field(
        default=None,
        repr=False,
    )
    """Workflow route configs keyed by workflow name (for schedule bootstrap)."""

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        await self.client.startup()

        if self.bootstrap_schedules:
            await _bootstrap_schedules(ctx, workflow_configs=self.workflow_configs)


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class RoutedTemporalShutdownHook(LifecycleHook):
    """Shutdown hook that closes all per-tenant Temporal clients."""

    client: RoutedTemporalClient

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        await self.client.close()


# ....................... #


async def _bootstrap_schedules(
    ctx: ExecutionContext,
    *,
    workflow_configs: Mapping[str, TemporalWorkflowConfig] | None,
) -> None:
    """Upsert declarative schedules registered on the deps container."""

    if not ctx.deps.exists(TemporalScheduleBootstrapDepKey):
        return

    bootstraps = ctx.deps.provide(TemporalScheduleBootstrapDepKey)

    if not bootstraps:
        return

    if workflow_configs is None:
        raise exc.internal(
            "Temporal schedule bootstrap requires workflow_configs on the lifecycle hook",
        )

    client = ctx.deps.provide(TemporalClientDepKey)

    for bootstrap in bootstraps:
        config = workflow_configs.get(bootstrap.workflow_name)

        if config is None:
            raise exc.internal(
                f"No Temporal workflow config for schedule bootstrap "
                f"{bootstrap.workflow_name!r}",
            )

        spec = DurableWorkflowSpec[Any, Any](
            name=bootstrap.workflow_name,
            run=DurableWorkflowInvokeSpec(
                args_type=cast(type[Any], type(bootstrap.default_args)),  # type: ignore[redundant-cast]
                return_type=None,
            ),
        )

        adapter = TemporalWorkflowScheduleCommandAdapter(
            client=client,
            queue=config["queue"],
            spec=spec,
            tenant_aware=config.get("tenant_aware", False),
            tenant_provider=ctx.inv_ctx.get_tenant,
        )

        await adapter.upsert(
            bootstrap.schedule_id,
            bootstrap.default_args,
            bootstrap.timing,
            workflow_id_template=bootstrap.workflow_id_template,
            trigger_immediately=bootstrap.trigger_immediately,
            note=bootstrap.note,
        )


# ....................... #


def temporal_lifecycle_step(
    name: str = "temporal_lifecycle",
    *,
    host: str,
    config: TemporalConfig = TemporalConfig(),
    bootstrap_schedules: bool = True,
    workflow_configs: Mapping[str, TemporalWorkflowConfig] | None = None,
) -> LifecycleStep:
    """Build a lifecycle step for Temporal client init and shutdown."""
    startup_hook = TemporalStartupHook(
        host=host,
        config=config,
        bootstrap_schedules=bootstrap_schedules,
        workflow_configs=workflow_configs,
    )

    return LifecycleStep(
        id=name,
        startup=startup_hook,
        shutdown=TemporalShutdownHook(),
    )


# ....................... #


def routed_temporal_lifecycle_step(
    name: str = "routed_temporal_lifecycle",
    *,
    client: RoutedTemporalClient,
    bootstrap_schedules: bool = True,
    workflow_configs: Mapping[str, TemporalWorkflowConfig] | None = None,
) -> LifecycleStep:
    """Lifecycle for :class:`RoutedTemporalClient` registered as :data:`TemporalClientDepKey`.

    Do not combine with :func:`temporal_lifecycle_step` on the same instance.
    """

    return LifecycleStep(
        id=name,
        startup=RoutedTemporalStartupHook(
            client=client,
            bootstrap_schedules=bootstrap_schedules,
            workflow_configs=workflow_configs,
        ),
        shutdown=RoutedTemporalShutdownHook(client=client),
    )
