"""Postgres durable-execution dep factories."""

from __future__ import annotations

from typing import TYPE_CHECKING, final

import attrs

from forze.application.contracts.crypto import KeyringDepKey
from forze.base.exceptions import exc

from ....adapters.durable import (
    PostgresDurableFunctionStepAdapter,
    PostgresDurableRunStore,
    PostgresDurableScheduleStore,
)
from ..configs.durable import (
    PostgresDurableRunConfig,
    PostgresDurableScheduleConfig,
    PostgresDurableStepConfig,
)
from ..keys import PostgresClientDepKey

if TYPE_CHECKING:
    from forze.application.execution.context import ExecutionContext

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurablePostgresDurableStep:
    """Build a :class:`PostgresDurableFunctionStepAdapter` for the durable step port.

    Execution-scoped (a :class:`~forze.application.contracts.deps.SimpleDepPort`): resolves
    the client and, when the route seals results, the keyring — failing closed if
    encryption is requested without a wired keyring.
    """

    config: PostgresDurableStepConfig
    """Postgres-specific configuration for the durable step journal."""

    def __call__(
        self,
        ctx: ExecutionContext,
    ) -> PostgresDurableFunctionStepAdapter:
        client = ctx.deps.provide(PostgresClientDepKey)

        cipher = None

        if self.config.encrypt:
            if not ctx.deps.exists(KeyringDepKey):
                raise exc.configuration(
                    "Durable step encryption is enabled but no keyring is wired. "
                    "Register a CryptoDepsModule or disable encrypt on the config.",
                )

            cipher = ctx.deps.provide(KeyringDepKey)

        return PostgresDurableFunctionStepAdapter(
            client=client,
            config=self.config,
            cipher=cipher,
            tenant_aware=self.config.tenant_aware,
            tenant_provider=ctx.inv_ctx.get_tenant,
        )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurablePostgresDurableRun:
    """Build a :class:`PostgresDurableRunStore` for the durable run store port.

    Execution-scoped (a :class:`~forze.application.contracts.deps.SimpleDepPort`): resolves
    the client and, when the store seals input/output, the keyring — failing closed if
    encryption is requested without a wired keyring.
    """

    config: PostgresDurableRunConfig
    """Postgres-specific configuration for the durable run store."""

    def __call__(
        self,
        ctx: ExecutionContext,
    ) -> PostgresDurableRunStore:
        client = ctx.deps.provide(PostgresClientDepKey)

        cipher = None

        if self.config.encrypt:
            if not ctx.deps.exists(KeyringDepKey):
                raise exc.configuration(
                    "Durable run encryption is enabled but no keyring is wired. "
                    "Register a CryptoDepsModule or disable encrypt on the config.",
                )

            cipher = ctx.deps.provide(KeyringDepKey)

        return PostgresDurableRunStore(
            client=client,
            config=self.config,
            cipher=cipher,
        )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurablePostgresDurableSchedule:
    """Build a :class:`PostgresDurableScheduleStore` for the durable schedule store port."""

    config: PostgresDurableScheduleConfig
    """Postgres-specific configuration for the durable schedule store."""

    def __call__(
        self,
        ctx: ExecutionContext,
    ) -> PostgresDurableScheduleStore:
        return PostgresDurableScheduleStore(
            client=ctx.deps.provide(PostgresClientDepKey),
            config=self.config,
        )
