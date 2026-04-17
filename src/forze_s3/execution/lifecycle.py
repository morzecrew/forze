"""Lifecycle hooks for S3 client initialization and shutdown."""

from typing import final

import attrs

from forze.application.execution import ExecutionContext, LifecycleHook, LifecycleStep

from ..kernel.platform import S3Config
from .deps import S3ClientDepKey

# ----------------------- #
#! typed dicts maybe ?


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class S3StartupHook(LifecycleHook):
    """Startup hook that initializes the S3 client from the deps container.

    Resolves :data:`S3ClientDepKey` and calls :meth:`S3Client.initialize`
    with endpoint and credentials. The client must be registered before
    startup runs.
    """

    endpoint: str
    """S3-compatible endpoint URL."""

    access_key_id: str
    """Access key for authentication."""

    secret_access_key: str
    """Secret key for authentication."""

    config: S3Config | None = attrs.field(default=None)
    """Optional botocore config for retries, timeouts, etc."""

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        s3_client = ctx.dep(S3ClientDepKey)

        await s3_client.initialize(
            self.endpoint,
            self.access_key_id,
            self.secret_access_key,
            config=self.config,
        )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class S3ShutdownHook(LifecycleHook):
    """Shutdown hook that closes the S3 client session.

    Resolves :data:`S3ClientDepKey` and calls :meth:`S3Client.close`.
    """

    async def __call__(self, ctx: ExecutionContext) -> None:
        s3_client = ctx.dep(S3ClientDepKey)
        s3_client.close()


# ....................... #


def s3_lifecycle_step(
    name: str = "s3_lifecycle",
    *,
    endpoint: str,
    access_key_id: str,
    secret_access_key: str,
    config: S3Config | None = None,
) -> LifecycleStep:
    """Build a lifecycle step for S3 client init and shutdown.

    :param name: Step name for collision detection.
    :param endpoint: S3-compatible endpoint URL.
    :param access_key_id: Access key for authentication.
    :param secret_access_key: Secret key for authentication.
    :param config: Optional botocore config.
    :returns: Lifecycle step with startup and shutdown hooks.
    """
    startup_hook = S3StartupHook(
        endpoint=endpoint,
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        config=config,
    )
    shutdown_hook = S3ShutdownHook()
    return LifecycleStep(name=name, startup=startup_hook, shutdown=shutdown_hook)
