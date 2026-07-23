"""SageMaker runtime client lifecycle hooks and step factory."""

from typing import TYPE_CHECKING, Any, final

import attrs
from botocore.config import Config as AioConfig
from pydantic import SecretStr

from forze.application.contracts.deps import DepKey
from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze.application.execution.lifecycle.builtin import (
    ClientShutdownHook,
    routed_client_lifecycle_step,
)
from forze.base.primitives import StrKey
from forze.base.serialization.pydantic import pydantic_secret_converter

from ...kernel import RoutedSageMakerRuntimeClient, SageMakerRuntimeClient
from ..deps.keys import SageMakerRuntimeClientDepKey

if TYPE_CHECKING:
    from forze.application.execution import ExecutionContext

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class SageMakerInferenceStartupHook(LifecycleHook):
    """Initialize the runtime client registered under ``SageMakerRuntimeClientDepKey``."""

    region_name: str | None = None
    """AWS region; ``None`` defers to the default botocore chain."""

    endpoint_url: str | None = None
    """Override URL (emulators / VPC endpoints); ``None`` = the real service."""

    access_key_id: str | None = attrs.field(default=None, repr=False)
    """Access key; ``None`` defers to the default botocore credential chain."""

    secret_access_key: SecretStr | None = attrs.field(
        default=None,
        converter=attrs.converters.optional(pydantic_secret_converter),
        repr=False,
    )
    """Secret key; ``None`` defers to the default botocore credential chain."""

    config: AioConfig | None = attrs.field(default=None, repr=False)
    """Optional botocore configuration. Botocore retries stay pinned to a single
    attempt unless ``retries`` is set here explicitly — ``invoke_endpoint`` is metered
    and non-idempotent, so silent transport-level retries are opt-in only."""

    # ....................... #

    async def __call__(self, ctx: "ExecutionContext") -> None:
        client = ctx.deps.provide(SageMakerRuntimeClientDepKey)

        if not isinstance(client, SageMakerRuntimeClient):
            return  # a custom port implementation owns its own initialization

        await client.initialize(
            region_name=self.region_name,
            endpoint_url=self.endpoint_url,
            access_key_id=self.access_key_id,
            secret_access_key=self.secret_access_key,
            config=self.config,
        )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class SageMakerInferenceShutdownHook(ClientShutdownHook):
    """Close the runtime client on shutdown."""

    dep_key: DepKey[Any] = attrs.field(default=SageMakerRuntimeClientDepKey, init=False)


# ....................... #


def sagemaker_inference_lifecycle_step(
    *,
    region_name: str | None = None,
    endpoint_url: str | None = None,
    access_key_id: str | None = None,
    secret_access_key: SecretStr | str | None = None,
    config: AioConfig | None = None,
    name: StrKey = "sagemaker_inference_client",
    depends_on: tuple[StrKey, ...] = (),
) -> LifecycleStep:
    """Lifecycle step initializing and closing the SageMaker runtime client.

    :param config: Optional botocore configuration, forwarded to the client. Botocore
        retries stay pinned to a single attempt unless ``retries`` is set here
        explicitly (``invoke_endpoint`` is metered and non-idempotent).
    """

    return LifecycleStep(
        id=name,
        depends_on=depends_on,
        startup=SageMakerInferenceStartupHook(
            region_name=region_name,
            endpoint_url=endpoint_url,
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,  # type: ignore[arg-type]
            config=config,
        ),
        shutdown=SageMakerInferenceShutdownHook(),
    )


# ....................... #


def routed_sagemaker_inference_lifecycle_step(
    client: RoutedSageMakerRuntimeClient,
    *,
    name: StrKey = "routed_sagemaker_inference_client",
) -> LifecycleStep:
    """Lifecycle step for a tenant-routed runtime client (``dedicated`` isolation).

    Unlike the single-client step there are no ambient credentials here — each tenant's AWS
    identity comes from its own secret, resolved on first use. Botocore configuration
    (retries, timeouts, proxies) is likewise not a parameter of this step: set it as
    ``RoutedSageMakerRuntimeClient(config=...)`` when constructing *client*, and it
    applies to every tenant's client.
    """

    return routed_client_lifecycle_step(str(name), client=client)
