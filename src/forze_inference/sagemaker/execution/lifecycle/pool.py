"""SageMaker runtime client lifecycle hooks and step factory."""

from typing import TYPE_CHECKING, Any, final

import attrs
from pydantic import SecretStr

from forze.application.contracts.deps import DepKey
from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze.application.execution.lifecycle.builtin import ClientShutdownHook
from forze.base.primitives import StrKey
from forze.base.serialization.pydantic import pydantic_secret_converter

from ...kernel import SageMakerRuntimeClient
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
    id: StrKey = "sagemaker_inference_client",
    depends_on: tuple[StrKey, ...] = (),
) -> LifecycleStep:
    """Lifecycle step initializing and closing the SageMaker runtime client."""

    return LifecycleStep(
        id=id,
        depends_on=depends_on,
        startup=SageMakerInferenceStartupHook(
            region_name=region_name,
            endpoint_url=endpoint_url,
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,  # type: ignore[arg-type]
        ),
        shutdown=SageMakerInferenceShutdownHook(),
    )
