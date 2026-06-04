"""BigQuery client pool lifecycle hooks and step factories."""

from typing import Any, cast, final

import attrs

from forze.application.contracts.deps import DepKey
from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze.application.execution import ExecutionContext
from forze.application.execution.lifecycle.builtin import (
    ClientShutdownHook,
    routed_client_lifecycle_step,
)

from ...kernel.client import BigQueryClient, BigQueryConfig, RoutedBigQueryClient
from ..deps import BigQueryClientDepKey

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
class BigQueryShutdownHook(ClientShutdownHook):
    """Shutdown hook that closes the BigQuery client."""

    dep_key: DepKey[Any] = attrs.field(default=BigQueryClientDepKey, init=False)


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


# ....................... #


def routed_bigquery_lifecycle_step(
    name: str = "routed_bigquery_lifecycle",
    *,
    client: RoutedBigQueryClient,
) -> LifecycleStep:
    """Lifecycle for :class:`RoutedBigQueryClient` registered as :data:`BigQueryClientDepKey`.

    Do not combine with :func:`bigquery_lifecycle_step` on the same instance.
    """

    return routed_client_lifecycle_step(name, client=client)
