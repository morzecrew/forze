"""GCP KMS client lifecycle hooks and step factory."""

from typing import Any, cast, final

import attrs

from forze.application.contracts.deps import DepKey
from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze.application.execution.context import ExecutionContext
from forze.application.execution.lifecycle.builtin import ClientShutdownHook

from ...kernel.client import GcpKmsClient, GcpKmsConfig
from ..deps import GcpKmsClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class GcpKmsStartupHook(LifecycleHook):
    """Startup hook that initializes the GCP KMS client from the deps container."""

    endpoint: str | None = None
    """Plaintext emulator endpoint (``host:port``); ``None`` targets real GCP."""

    credentials: Any | None = attrs.field(default=None, repr=False)
    """Optional ``google.auth`` credentials; ``None`` = application-default."""

    config: GcpKmsConfig | None = attrs.field(default=None, repr=False)
    """Optional client config (e.g. request timeout)."""

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        client = cast(GcpKmsClient, ctx.deps.provide(GcpKmsClientDepKey))

        await client.initialize(
            endpoint=self.endpoint,
            credentials=self.credentials,
            config=self.config,
        )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class GcpKmsShutdownHook(ClientShutdownHook):
    """Shutdown hook that closes the GCP KMS client."""

    dep_key: DepKey[Any] = attrs.field(default=GcpKmsClientDepKey, init=False)


# ....................... #


def gcpkms_lifecycle_step(
    name: str = "gcpkms_lifecycle",
    *,
    endpoint: str | None = None,
    credentials: Any | None = None,
    config: GcpKmsConfig | None = None,
) -> LifecycleStep:
    """Build a lifecycle step for GCP KMS client init and shutdown.

    :param name: Step name for collision detection.
    :param endpoint: Optional plaintext emulator endpoint (``host:port``).
    :param credentials: Optional ``google.auth`` credentials (``None`` = ADC).
    :param config: Optional client config.
    :returns: Lifecycle step with startup and shutdown hooks.
    """

    return LifecycleStep(
        id=name,
        startup=GcpKmsStartupHook(
            endpoint=endpoint,
            credentials=credentials,
            config=config,
        ),
        shutdown=GcpKmsShutdownHook(),
    )
