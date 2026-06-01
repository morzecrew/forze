"""Postgres outbox dep factory."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, final

import attrs

from ....adapters.outbox import PostgresOutboxAdapter
from ..configs.outbox import PostgresOutboxConfig
from ..keys import PostgresClientDepKey

if TYPE_CHECKING:
    from forze.application.contracts.outbox import OutboxSpec
    from forze.application.execution.context import ExecutionContext

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurablePostgresOutbox:
    """Build a :class:`PostgresOutboxAdapter` for an outbox spec route."""

    config: PostgresOutboxConfig
    """Postgres-specific configuration for the route."""

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: OutboxSpec[Any],
    ) -> PostgresOutboxAdapter[Any]:
        client = ctx.deps.provide(PostgresClientDepKey)
        return PostgresOutboxAdapter(
            ctx=ctx,
            client=client,
            spec=spec,
            config=self.config,
            tenant_aware=self.config.tenant_aware,
            tenant_provider=ctx.inv_ctx.get_tenant,
        )
