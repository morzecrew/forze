"""AWS KMS client pool lifecycle hooks and step factory."""

from typing import Any, cast, final

import attrs
from pydantic import SecretStr

from forze.application.contracts.deps import DepKey
from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze.application.execution.context import ExecutionContext
from forze.application.execution.lifecycle.builtin import ClientShutdownHook
from forze.base.serialization.pydantic import pydantic_secret_converter

from ...kernel.client import AwsKmsClient, AwsKmsConfig
from ..deps import AwsKmsClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class AwsKmsStartupHook(LifecycleHook):
    """Startup hook that initializes the AWS KMS client from the deps container."""

    endpoint: str | None = None
    """S3-compatible/LocalStack endpoint URL; ``None`` for the real AWS endpoint."""

    region_name: str | None = None
    """AWS region; ``None`` defers to the botocore chain."""

    access_key_id: str | None = attrs.field(default=None, repr=False)
    """Access key; ``None`` defers to the default botocore credential chain."""

    secret_access_key: SecretStr | None = attrs.field(
        default=None,
        converter=attrs.converters.optional(pydantic_secret_converter),
        repr=False,
    )
    """Secret key; ``None`` defers to the default botocore credential chain."""

    config: AwsKmsConfig | None = attrs.field(default=None, repr=False)
    """Optional botocore config for retries, timeouts, etc."""

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        client = cast(AwsKmsClient, ctx.deps.provide(AwsKmsClientDepKey))

        await client.initialize(
            endpoint=self.endpoint,
            region_name=self.region_name,
            access_key_id=self.access_key_id,
            secret_access_key=self.secret_access_key,
            config=self.config,
        )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class AwsKmsShutdownHook(ClientShutdownHook):
    """Shutdown hook that closes the AWS KMS client."""

    dep_key: DepKey[Any] = attrs.field(default=AwsKmsClientDepKey, init=False)


# ....................... #


def awskms_lifecycle_step(
    name: str = "awskms_lifecycle",
    *,
    endpoint: str | None = None,
    region_name: str | None = None,
    access_key_id: str | None = None,
    secret_access_key: str | SecretStr | None = None,
    config: AwsKmsConfig | None = None,
) -> LifecycleStep:
    """Build a lifecycle step for AWS KMS client init and shutdown.

    :param name: Step name for collision detection.
    :param endpoint: Optional S3-compatible/LocalStack endpoint URL.
    :param region_name: AWS region, or ``None`` for the botocore chain.
    :param access_key_id: Access key, or ``None`` for the default chain.
    :param secret_access_key: Secret key, or ``None`` for the default chain.
    :param config: Optional botocore config.
    :returns: Lifecycle step with startup and shutdown hooks.
    """

    return LifecycleStep(
        id=name,
        startup=AwsKmsStartupHook(
            endpoint=endpoint,
            region_name=region_name,
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
            config=config,
        ),
        shutdown=AwsKmsShutdownHook(),
    )
