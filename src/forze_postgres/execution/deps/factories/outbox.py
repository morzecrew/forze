"""Postgres outbox dep factories."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, final

import attrs

from forze.application.execution.outbox import build_staging_outbox_command_for_store

from ....adapters.outbox import PostgresOutboxStore
from ..configs.outbox import PostgresOutboxConfig
from ..keys import PostgresClientDepKey

if TYPE_CHECKING:
    from forze.application.contracts.outbox import OutboxSpec
    from forze.application.execution.context import ExecutionContext
    from forze.application.integrations.outbox import StagingOutboxCommand

# ----------------------- #


def _build_store[M](
    ctx: ExecutionContext,
    spec: OutboxSpec[Any],
    config: PostgresOutboxConfig,
) -> PostgresOutboxStore[Any]:
    client = ctx.deps.provide(PostgresClientDepKey)
    return PostgresOutboxStore(
        client=client,
        spec=spec,
        config=config,
        tenant_aware=config.tenant_aware,
        tenant_provider=ctx.inv_ctx.get_tenant,
    )


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurablePostgresOutboxQuery:
    """Build a :class:`PostgresOutboxStore` for an outbox spec route."""

    config: PostgresOutboxConfig
    """Postgres-specific configuration for the route."""

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: OutboxSpec[Any],
    ) -> PostgresOutboxStore[Any]:
        return _build_store(ctx, spec, self.config)


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurablePostgresOutboxAdmin:
    """Build the read-only admin (depth/age) view of an outbox spec route.

    The same store serves it — it implements both protocols — but the key is separate so a
    read-only ``QUERY`` can acquire the admin surface without the claim/mark port coming
    along with it.
    """

    config: PostgresOutboxConfig
    """Postgres-specific configuration for the route."""

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: OutboxSpec[Any],
    ) -> PostgresOutboxStore[Any]:
        return _build_store(ctx, spec, self.config)


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurablePostgresOutboxCommand:
    """Build a :class:`~forze.application.integrations.outbox.StagingOutboxCommand`."""

    config: PostgresOutboxConfig
    """Postgres-specific configuration for the route."""

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: OutboxSpec[Any],
    ) -> StagingOutboxCommand[Any]:
        store = _build_store(ctx, spec, self.config)
        return build_staging_outbox_command_for_store(ctx, spec, store)


# Backward-compatible alias for docs/tests referencing a single factory name.
ConfigurablePostgresOutbox = ConfigurablePostgresOutboxQuery
