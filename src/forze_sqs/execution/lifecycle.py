"""Lifecycle hooks for SQS client initialization and shutdown."""

from typing import final

import attrs

from forze.application.execution import ExecutionContext, LifecycleHook, LifecycleStep

from ..kernel.platform import SQSConfig
from .deps import SQSClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class SQSStartupHook(LifecycleHook):
    """Startup hook that initializes the SQS client from the deps container."""

    endpoint: str
    region_name: str
    access_key_id: str
    secret_access_key: str
    config: SQSConfig | None = None

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        sqs_client = ctx.dep(SQSClientDepKey)

        await sqs_client.initialize(
            endpoint=self.endpoint,
            region_name=self.region_name,
            access_key_id=self.access_key_id,
            secret_access_key=self.secret_access_key,
            config=self.config,
        )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class SQSShutdownHook(LifecycleHook):
    """Shutdown hook that closes the SQS session."""

    async def __call__(self, ctx: ExecutionContext) -> None:
        sqs_client = ctx.dep(SQSClientDepKey)
        sqs_client.close()


# ....................... #


def sqs_lifecycle_step(
    name: str = "sqs_lifecycle",
    *,
    endpoint: str,
    region_name: str,
    access_key_id: str,
    secret_access_key: str,
    config: SQSConfig | None = None,
) -> LifecycleStep:
    """Build a lifecycle step for SQS client init and shutdown."""
    startup_hook = SQSStartupHook(
        endpoint=endpoint,
        region_name=region_name,
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        config=config,
    )
    shutdown_hook = SQSShutdownHook()

    return LifecycleStep(name=name, startup=startup_hook, shutdown=shutdown_hook)
