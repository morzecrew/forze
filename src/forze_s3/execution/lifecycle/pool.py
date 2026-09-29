"""S3 client pool lifecycle hooks and step factories."""

from collections.abc import Sequence
from typing import Any, Final, cast, final

import attrs
from pydantic import SecretStr

from forze.application.contracts.deps import DepKey
from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze.application.execution.context import ExecutionContext
from forze.application.execution.lifecycle.builtin import (
    ClientShutdownHook,
)
from forze.base.exceptions import exc
from forze.base.serialization.pydantic import pydantic_secret_converter

from ...kernel.client import S3Client, S3Config
from ..deps import S3ClientDepKey

# ----------------------- #

S3_CLIENT_CAPABILITY: Final = "s3.client"
"""Capability :func:`s3_lifecycle_step` provides: the S3 client is initialized."""

# ....................... #


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

    access_key_id: str | None = attrs.field(default=None, repr=False)
    """Access key for authentication; ``None`` defers to the default
    botocore credential chain."""

    secret_access_key: SecretStr | None = attrs.field(
        default=None,
        converter=attrs.converters.optional(pydantic_secret_converter),
        repr=False,
    )
    """Secret key for authentication; ``None`` defers to the default
    botocore credential chain."""

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
class S3ShutdownHook(ClientShutdownHook):
    """Shutdown hook that closes the S3 client session.

    Resolves :data:`S3ClientDepKey` and awaits :meth:`S3Client.close`.
    """

    dep_key: DepKey[Any] = attrs.field(default=S3ClientDepKey, init=False)


# ....................... #


def s3_lifecycle_step(
    name: str = "s3_lifecycle",
    *,
    endpoint: str,
    access_key_id: str | None = None,
    secret_access_key: str | SecretStr | None = None,
    config: S3Config | None = None,
) -> LifecycleStep:
    """Build a lifecycle step for S3 client init and shutdown.

    :param name: Step name for collision detection.
    :param endpoint: S3-compatible endpoint URL.
    :param access_key_id: Access key for authentication, or ``None`` to
        defer to botocore's default credential chain.
    :param secret_access_key: Secret key for authentication, or ``None`` to
        defer to the chain.
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
    return LifecycleStep(
        id=name,
        startup=startup_hook,
        shutdown=shutdown_hook,
        provides=(S3_CLIENT_CAPABILITY,),
    )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class S3BucketStartupHook(LifecycleHook):
    """Startup hook creating each configured bucket that does not exist yet."""

    buckets: tuple[str, ...]
    """Bucket names to ensure, in order."""

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        s3_client = ctx.deps.provide(S3ClientDepKey)

        async with s3_client.client():
            for bucket in self.buckets:
                await s3_client.ensure_bucket(bucket)


# ....................... #


def s3_bucket_lifecycle_step(
    name: str = "s3_buckets",
    *,
    buckets: Sequence[str],
) -> LifecycleStep:
    """Build a startup step that creates the app's buckets when they are missing.

    Runs after :func:`s3_lifecycle_step` (it requires :data:`S3_CLIENT_CAPABILITY`) and is
    idempotent: an existing bucket is left as it is. A bucket that cannot be created fails
    startup, since nothing could be stored in it. It creates shared infrastructure, so under a
    ``FLEET`` profile wrap it with a singleton lifecycle step. A tenant-routed client has no
    tenant at startup; provision per-tenant buckets with ``ObjectStorageTenantProvisioner``.

    :param name: Step name for collision detection.
    :param buckets: Bucket names to ensure.
    :returns: Lifecycle step with a startup hook only.
    """

    # A bare string is a sequence too: it would ensure one bucket per character.
    if isinstance(buckets, str) or not buckets or any(not b.strip() for b in buckets):
        raise exc.configuration(
            f"s3_bucket_lifecycle_step needs a non-empty list of bucket names, not {buckets!r}.",
        )

    return LifecycleStep(
        id=name,
        startup=S3BucketStartupHook(buckets=tuple(buckets)),
        requires=(S3_CLIENT_CAPABILITY,),
        mutates_shared_state=True,
    )
