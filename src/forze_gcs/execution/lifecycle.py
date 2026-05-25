"""Lifecycle hooks for GCS client initialization and shutdown."""

from typing import cast, final

import attrs

from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze.application.execution import ExecutionContext

from ..kernel.platform import GCSClient, GCSConfig
from .deps import GCSClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class GCSStartupHook(LifecycleHook):
    """Startup hook that initializes the GCS client from the deps container."""

    project_id: str
    """GCP project id."""

    emulator_host: str | None = None
    """Optional ``STORAGE_EMULATOR_HOST`` value (e.g. fake-gcs-server)."""

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
            emulator_host=self.emulator_host,
            config=self.config,
        )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class GCSShutdownHook(LifecycleHook):
    """Shutdown hook that closes the GCS client."""

    async def __call__(self, ctx: ExecutionContext) -> None:
        gcs_client = ctx.deps.provide(GCSClientDepKey)
        await gcs_client.close()


# ....................... #


def gcs_lifecycle_step(
    name: str = "gcs_lifecycle",
    *,
    project_id: str,
    emulator_host: str | None = None,
    service_file: str | None = None,
    config: GCSConfig | None = None,
) -> LifecycleStep:
    """Build a lifecycle step for GCS client init and shutdown.

    :param name: Step name for collision detection.
    :param project_id: GCP project id for the storage client.
    :param emulator_host: Optional emulator URL for local/testing.
    :param service_file: Optional service account JSON path (ADC if omitted).
    :param config: Optional client configuration.
    :returns: Lifecycle step with startup and shutdown hooks.
    """

    return LifecycleStep(
        id=name,
        startup=GCSStartupHook(
            project_id=project_id,
            emulator_host=emulator_host,
            service_file=service_file,
            config=config,
        ),
        shutdown=GCSShutdownHook(),
    )
