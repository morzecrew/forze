"""Lifecycle hooks for Vault client initialization and shutdown."""

from typing import cast, final

import attrs

from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze.application.execution import ExecutionContext

from ...kernel.client import VaultClient
from ..deps import VaultClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class VaultStartupHook(LifecycleHook):
    """Startup hook that initializes the Vault client from the deps container."""

    async def __call__(self, ctx: ExecutionContext) -> None:
        vault_client = cast(VaultClient, ctx.deps.provide(VaultClientDepKey))
        await vault_client.initialize()


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class VaultShutdownHook(LifecycleHook):
    """Shutdown hook that releases the Vault client."""

    async def __call__(self, ctx: ExecutionContext) -> None:
        vault_client = ctx.deps.provide(VaultClientDepKey)
        await vault_client.close()


# ....................... #


def vault_lifecycle_step(name: str = "vault_lifecycle") -> LifecycleStep:
    """Build a lifecycle step for Vault client init and shutdown.

    The client registered under :data:`VaultClientDepKey` must already carry
    its :class:`~forze_vault.kernel.client.VaultConfig`.
    """

    return LifecycleStep(
        id=name,
        startup=VaultStartupHook(),
        shutdown=VaultShutdownHook(),
    )
