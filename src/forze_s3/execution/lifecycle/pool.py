"""S3 client pool lifecycle hooks and step factories."""

from typing import cast, final

import attrs
from pydantic import SecretStr

from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze.application.execution.context import ExecutionContext
from forze.application.execution.lifecycle.builtin import routed_client_lifecycle_step
from forze.base.serialization.pydantic import pydantic_secret_converter

from ...kernel.client import RoutedS3Client, S3Client, S3Config
from ..deps import S3ClientDepKey

# ----------------------- #


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

    access_key_id: str = attrs.field(repr=False)
    """Access key for authentication."""

    secret_access_key: SecretStr = attrs.field(
        converter=pydantic_secret_converter,
        repr=False,
    )
    """Secret key for authentication."""

    config: S3Config | None = attrs.field(default=None, repr=False)
    """Optional botocore config for retries, timeouts, etc."""

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        s3_client = cast(S3Client, ctx.deps.provide(S3ClientDepKey))

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
        s3_client = ctx.deps.provide(S3ClientDepKey)
        await s3_client.close()


# ....................... #


def s3_lifecycle_step(
    name: str = "s3_lifecycle",
    *,
    endpoint: str,
    access_key_id: str,
    secret_access_key: str | SecretStr,
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
    return LifecycleStep(id=name, startup=startup_hook, shutdown=shutdown_hook)


# ....................... #


def routed_s3_lifecycle_step(
    name: str = "routed_s3_lifecycle",
    *,
    client: RoutedS3Client,
) -> LifecycleStep:
    """Lifecycle for :class:`RoutedS3Client` registered as :data:`S3ClientDepKey`.

    Do not combine with :func:`s3_lifecycle_step` on the same instance.
    """

    return routed_client_lifecycle_step(name, client=client)
