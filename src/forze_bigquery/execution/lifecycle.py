"""Lifecycle hooks for BigQuery client initialization and shutdown."""

from typing import cast, final

import attrs

from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze.application.execution import ExecutionContext

from ..kernel.platform import BigQueryClient, BigQueryConfig
from .deps import BigQueryClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class BigQueryStartupHook(LifecycleHook):
    """Startup hook that initializes the BigQuery client."""

    project_id: str
    """GCP project id."""

    service_file: str | None = attrs.field(default=None, repr=False)
    """Optional service account JSON path."""

    config: BigQueryConfig | None = attrs.field(default=None, repr=False)
    """Optional client configuration."""

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        bq_client = cast(BigQueryClient, ctx.deps.provide(BigQueryClientDepKey))
        await bq_client.initialize(
            self.project_id,
            service_file=self.service_file,
            config=self.config,
        )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class BigQueryShutdownHook(LifecycleHook):
    """Shutdown hook that closes the BigQuery client."""

    async def __call__(self, ctx: ExecutionContext) -> None:
        bq_client = ctx.deps.provide(BigQueryClientDepKey)
        await bq_client.close()


# ....................... #


def bigquery_lifecycle_step(
    name: str = "bigquery_lifecycle",
    *,
    project_id: str,
    service_file: str | None = None,
    config: BigQueryConfig | None = None,
) -> LifecycleStep:
    """Build a lifecycle step for BigQuery client init and shutdown."""

    return LifecycleStep(
        id=name,
        startup=BigQueryStartupHook(
            project_id=project_id,
            service_file=service_file,
            config=config,
        ),
        shutdown=BigQueryShutdownHook(),
    )
