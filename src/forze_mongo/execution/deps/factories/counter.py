"""Mongo counter dep factories (allocation port + admin enumeration port)."""

from __future__ import annotations

from typing import TYPE_CHECKING, final

import attrs

from ....adapters.counter import MongoCounterAdapter, MongoCounterAdminAdapter
from ..configs.counter import MongoCounterConfig
from ..keys import MongoClientDepKey

if TYPE_CHECKING:
    from forze.application.contracts.counter import CounterSpec
    from forze.application.execution.context import ExecutionContext

# ----------------------- #


@attrs.define(slots=True, frozen=True, kw_only=True)
class _ConfigurableMongoCounterBase:
    """Shared config for the counter data and admin factories."""

    config: MongoCounterConfig
    """Mongo-specific configuration for the route."""


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableMongoCounter(_ConfigurableMongoCounterBase):
    """Build a :class:`MongoCounterAdapter` for a counter spec route."""

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: CounterSpec,
    ) -> MongoCounterAdapter:
        return MongoCounterAdapter(
            client=ctx.deps.provide(MongoClientDepKey),
            config=self.config,
            tenant_aware=self.config.tenant_aware,
            tenant_provider=ctx.inv_ctx.get_tenant,
        )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableMongoCounterAdmin(_ConfigurableMongoCounterBase):
    """Build a :class:`MongoCounterAdminAdapter` for a counter spec route.

    Built from the **same** route config as the allocation port, so a wired counter is
    always enumerable: an admin port behind its own opt-in flag would be missing exactly
    when an export needed it.
    """

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: CounterSpec,
    ) -> MongoCounterAdminAdapter:
        return MongoCounterAdminAdapter(
            client=ctx.deps.provide(MongoClientDepKey),
            config=self.config,
            tenant_aware=self.config.tenant_aware,
            tenant_provider=ctx.inv_ctx.get_tenant,
        )
