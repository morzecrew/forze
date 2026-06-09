"""GCS client pool lifecycle hooks and step factories."""

from typing import Any, cast, final

import attrs

from forze.application.contracts.deps import DepKey
from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze.application.execution import ExecutionContext
from forze.application.execution.lifecycle.builtin import (
    ClientShutdownHook,
    routed_client_lifecycle_step,
)

from ...kernel.client import GCSClient, GCSConfig, RoutedGCSClient
from ..deps import GCSClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class GCSStartupHook(LifecycleHook):
    """Startup hook that initializes the GCS client from the deps container."""

    project_id: str
    """GCP project id."""

    service_file: str | None = attrs.field(default=None, repr=False)
    """Optional path to a service account JSON key file."""

    config: GCSConfig | None = attrs.field(default=None, repr=False)
    """Optional client configuration."""

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        gcs_client = cast(GCSClient, ctx.deps.provide(GCSClientDepKey))

        await gcs_client.initialize(
            self.project_id,
            service_file=self.service_file,
            config=self.config,
        )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class GCSShutdownHook(ClientShutdownHook):
    """Shutdown hook that closes the GCS client."""

    dep_key: DepKey[Any] = attrs.field(default=GCSClientDepKey, init=False)


# ....................... #


def gcs_lifecycle_step(
    name: str = "gcs_lifecycle",
    *,
    project_id: str,
    service_file: str | None = None,
    config: GCSConfig | None = None,
) -> LifecycleStep:
    """Build a lifecycle step for GCS client init and shutdown.

    :param name: Step name for collision detection.
    :param project_id: GCP project id for the storage client.
    :param service_file: Optional service account JSON path (ADC if omitted).
    :param config: Optional client configuration.
    :returns: Lifecycle step with startup and shutdown hooks.
    """

    return LifecycleStep(
        id=name,
        startup=GCSStartupHook(
            project_id=project_id,
            service_file=service_file,
            config=config,
        ),
        shutdown=GCSShutdownHook(),
    )


def routed_gcs_lifecycle_step(
    name: str = "routed_gcs_lifecycle",
    *,
    client: RoutedGCSClient,
) -> LifecycleStep:
    """Lifecycle for :class:`RoutedGCSClient` registered as :data:`GCSClientDepKey`."""

    return routed_client_lifecycle_step(name, client=client)
