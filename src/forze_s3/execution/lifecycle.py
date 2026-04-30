"""Lifecycle hooks for S3 client initialization and shutdown."""

from typing import cast, final

import attrs

from forze.application.execution import ExecutionContext, LifecycleHook, LifecycleStep

from ..kernel.platform import RoutedS3Client, S3Client, S3Config
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
        s3_client = cast(S3Client, ctx.dep(S3ClientDepKey))

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

    Resolves :data:`S3ClientDepKey` and awaits :meth:`S3Client.close`.
    """

    async def __call__(self, ctx: ExecutionContext) -> None:
        s3_client = ctx.dep(S3ClientDepKey)
        await s3_client.close()


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class RoutedS3StartupHook(LifecycleHook):
    """Startup hook that marks a :class:`RoutedS3Client` as ready."""

    client: RoutedS3Client

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        await self.client.startup()


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class RoutedS3ShutdownHook(LifecycleHook):
    """Shutdown hook that closes all per-tenant S3 sessions."""

    client: RoutedS3Client

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        await self.client.close()


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


# ....................... #


def routed_s3_lifecycle_step(
    name: str = "routed_s3_lifecycle",
    *,
    client: RoutedS3Client,
) -> LifecycleStep:
    """Lifecycle for :class:`RoutedS3Client` registered as :data:`S3ClientDepKey`.

    Do not combine with :func:`s3_lifecycle_step` on the same instance.
    """

    return LifecycleStep(
        name=name,
        startup=RoutedS3StartupHook(client=client),
        shutdown=RoutedS3ShutdownHook(client=client),
    )
