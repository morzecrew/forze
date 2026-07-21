"""Postgres counter dep factories (allocation port + admin enumeration port)."""

from __future__ import annotations

from typing import TYPE_CHECKING, final

import attrs

from ....adapters.counter import PostgresCounterAdapter, PostgresCounterAdminAdapter
from ..configs.counter import PostgresCounterConfig
from ..keys import PostgresClientDepKey

if TYPE_CHECKING:
    from forze.application.contracts.counter import CounterSpec
    from forze.application.execution.context import ExecutionContext

# ----------------------- #


@attrs.define(slots=True, frozen=True, kw_only=True)
class _ConfigurablePostgresCounterBase:
    """Shared config for the counter data and admin factories."""

    config: PostgresCounterConfig
    """Postgres-specific configuration for the route."""


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurablePostgresCounter(_ConfigurablePostgresCounterBase):
    """Build a :class:`PostgresCounterAdapter` for a counter spec route."""

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: CounterSpec,
    ) -> PostgresCounterAdapter:
        return PostgresCounterAdapter(
            client=ctx.deps.provide(PostgresClientDepKey),
            config=self.config,
            route=str(spec.name),
            tenant_aware=self.config.tenant_aware,
            tenant_provider=ctx.inv_ctx.get_tenant,
        )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurablePostgresCounterAdmin(_ConfigurablePostgresCounterBase):
    """Build a :class:`PostgresCounterAdminAdapter` for a counter spec route.

    Built from the **same** route config as the allocation port, so a wired counter is
    always enumerable: an admin port behind its own opt-in flag would be missing exactly
    when an export needed it.
    """

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: CounterSpec,
    ) -> PostgresCounterAdminAdapter:
        return PostgresCounterAdminAdapter(
            client=ctx.deps.provide(PostgresClientDepKey),
            config=self.config,
            route=str(spec.name),
            tenant_aware=self.config.tenant_aware,
            tenant_provider=ctx.inv_ctx.get_tenant,
        )
