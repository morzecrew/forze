"""Postgres idempotency dep factory."""

from __future__ import annotations

from typing import TYPE_CHECKING, final

import attrs

from ....adapters.idempotency import PostgresIdempotencyStore
from ..configs.idempotency import PostgresIdempotencyConfig
from ..keys import PostgresClientDepKey

if TYPE_CHECKING:
    from forze.application.contracts.idempotency import IdempotencySpec
    from forze.application.execution.context import ExecutionContext

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurablePostgresIdempotency:
    """Build a :class:`PostgresIdempotencyStore` for an idempotency spec route."""

    config: PostgresIdempotencyConfig
    """Postgres-specific configuration for the route."""

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: IdempotencySpec,
    ) -> PostgresIdempotencyStore:
        client = ctx.deps.provide(PostgresClientDepKey)
        return PostgresIdempotencyStore(
            client=client,
            spec=spec,
            config=self.config,
            tenant_aware=self.config.tenant_aware,
            tenant_provider=ctx.inv_ctx.get_tenant,
        )
