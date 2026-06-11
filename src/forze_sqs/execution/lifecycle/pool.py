"""SQS client pool lifecycle hooks and step factories."""

from typing import Any, cast, final

import attrs
from pydantic import SecretStr

from forze.application.contracts.deps import DepKey
from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze.application.execution.context import ExecutionContext
from forze.application.execution.lifecycle.builtin import (
    ClientShutdownHook,
    routed_client_lifecycle_step,
)
from forze.base.serialization.pydantic import pydantic_secret_converter

from ...kernel.client import RoutedSQSClient, SQSClient, SQSConfig
from ..deps import SQSClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class SQSStartupHook(LifecycleHook):
    """Startup hook that initializes the SQS client from the deps container.

    Leave *access_key_id* / *secret_access_key* as ``None`` to defer to
    botocore's default credential chain (env vars, shared config files,
    container/instance roles) instead of static credentials. Leave
    *region_name* as ``None`` to defer to the chain-resolved region
    (``AWS_REGION``/``AWS_DEFAULT_REGION``, profile, IMDS).
    """

    endpoint: str
    region_name: str | None = None
    access_key_id: str | None = attrs.field(default=None, repr=False)
    secret_access_key: SecretStr | None = attrs.field(
        default=None,
        converter=attrs.converters.optional(pydantic_secret_converter),
        repr=False,
    )
    config: SQSConfig | None = attrs.field(default=None, repr=False)

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        sqs_client = cast(SQSClient, ctx.deps.provide(SQSClientDepKey))

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
class SQSShutdownHook(ClientShutdownHook):
    """Shutdown hook that closes the SQS session (await :meth:`SQSClient.close`)."""

    dep_key: DepKey[Any] = attrs.field(default=SQSClientDepKey, init=False)


# ....................... #


def sqs_lifecycle_step(
    name: str = "sqs_lifecycle",
    *,
    endpoint: str,
    region_name: str | None = None,
    access_key_id: str | None = None,
    secret_access_key: str | SecretStr | None = None,
    config: SQSConfig | None = None,
) -> LifecycleStep:
    """Build a lifecycle step for SQS client init and shutdown.

    Omit *access_key_id* / *secret_access_key* to use botocore's default
    credential chain instead of static credentials. Omit *region_name* to
    use the chain-resolved region (``AWS_REGION``/``AWS_DEFAULT_REGION``,
    profile, IMDS).
    """
    startup_hook = SQSStartupHook(
        endpoint=endpoint,
        region_name=region_name,
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        config=config,
    )
    shutdown_hook = SQSShutdownHook()

    return LifecycleStep(id=name, startup=startup_hook, shutdown=shutdown_hook)


# ....................... #


def routed_sqs_lifecycle_step(
    name: str = "routed_sqs_lifecycle",
    *,
    client: RoutedSQSClient,
) -> LifecycleStep:
    """Lifecycle for :class:`RoutedSQSClient` registered as :data:`SQSClientDepKey`.

    Do not combine with :func:`sqs_lifecycle_step` on the same instance.
    """

    return routed_client_lifecycle_step(name, client=client)
