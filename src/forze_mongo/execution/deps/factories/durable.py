"""Mongo durable-execution dep factories."""

from __future__ import annotations

from typing import TYPE_CHECKING, final

import attrs

from forze.application.contracts.crypto import KeyringDepKey, KeyringPort
from forze.base.exceptions import exc

from ....adapters.durable import (
    MongoDurableFunctionStepAdapter,
    MongoDurableRunStore,
    MongoDurableScheduleStore,
)
from ..configs.durable import (
    MongoDurableRunConfig,
    MongoDurableScheduleConfig,
    MongoDurableStepConfig,
)
from ..keys import MongoClientDepKey

if TYPE_CHECKING:
    from forze.application.execution.context import ExecutionContext

# ----------------------- #


def _cipher(ctx: ExecutionContext, *, encrypt: bool, what: str) -> KeyringPort | None:
    """Resolve the keyring for a route that seals payloads, or fail closed.

    Shared by the step and run factories because the failure has to read the same from
    both: a route that asked for encryption and got none must not start and quietly
    journal plaintext.
    """

    if not encrypt:
        return None

    if not ctx.deps.exists(KeyringDepKey):
        raise exc.configuration(
            f"Durable {what} encryption is enabled but no keyring is wired. "
            "Register a CryptoDepsModule or disable encrypt on the config.",
        )

    return ctx.deps.provide(KeyringDepKey)


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableMongoDurableStep:
    """Build a :class:`MongoDurableFunctionStepAdapter` for the durable step port.

    Execution-scoped (a :class:`~forze.application.contracts.deps.SimpleDepPort`): the active
    run is read from the ambient ``DurableRunContext``, not from a route.
    """

    config: MongoDurableStepConfig
    """Mongo-specific configuration for the durable step journal."""

    def __call__(self, ctx: ExecutionContext) -> MongoDurableFunctionStepAdapter:
        return MongoDurableFunctionStepAdapter(
            client=ctx.deps.provide(MongoClientDepKey),
            config=self.config,
            cipher=_cipher(ctx, encrypt=self.config.encrypt, what="step"),
            tenant_aware=self.config.tenant_aware,
            tenant_provider=ctx.inv_ctx.get_tenant,
        )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableMongoDurableRun:
    """Build a :class:`MongoDurableRunStore` for the durable run store port."""

    config: MongoDurableRunConfig
    """Mongo-specific configuration for the durable run store."""

    def __call__(self, ctx: ExecutionContext) -> MongoDurableRunStore:
        return MongoDurableRunStore(
            client=ctx.deps.provide(MongoClientDepKey),
            config=self.config,
            cipher=_cipher(ctx, encrypt=self.config.encrypt, what="run"),
            tenant_aware=self.config.tenant_aware,
            tenant_provider=ctx.inv_ctx.get_tenant,
        )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableMongoDurableSchedule:
    """Build a :class:`MongoDurableScheduleStore` for the durable schedule store port."""

    config: MongoDurableScheduleConfig
    """Mongo-specific configuration for the durable schedule store."""

    def __call__(self, ctx: ExecutionContext) -> MongoDurableScheduleStore:
        return MongoDurableScheduleStore(
            client=ctx.deps.provide(MongoClientDepKey),
            config=self.config,
            tenant_aware=self.config.tenant_aware,
            tenant_provider=ctx.inv_ctx.get_tenant,
        )
