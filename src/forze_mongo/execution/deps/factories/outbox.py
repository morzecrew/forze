"""Mongo outbox dep factories."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, final

import attrs

from forze.application.execution.outbox import build_staging_outbox_command_for_store

from ....adapters.outbox import MongoOutboxStore
from ..configs.outbox import MongoOutboxConfig
from ..keys import MongoClientDepKey

if TYPE_CHECKING:
    from forze.application.contracts.outbox import OutboxSpec
    from forze.application.execution.context import ExecutionContext
    from forze.application.integrations.outbox import StagingOutboxCommand

# ----------------------- #


def _build_store(
    ctx: ExecutionContext,
    spec: OutboxSpec[Any],
    config: MongoOutboxConfig,
) -> MongoOutboxStore[Any]:
    client = ctx.deps.provide(MongoClientDepKey)
    return MongoOutboxStore(
        client=client,
        spec=spec,
        config=config,
        tenant_aware=config.tenant_aware,
        tenant_provider=ctx.inv_ctx.get_tenant,
    )


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableMongoOutboxQuery:
    """Build a :class:`MongoOutboxStore` for an outbox spec route."""

    config: MongoOutboxConfig
    """Mongo-specific configuration for the route."""

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: OutboxSpec[Any],
    ) -> MongoOutboxStore[Any]:
        return _build_store(ctx, spec, self.config)


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableMongoOutboxCommand:
    """Build a :class:`~forze.application.integrations.outbox.StagingOutboxCommand`."""

    config: MongoOutboxConfig
    """Mongo-specific configuration for the route."""

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: OutboxSpec[Any],
    ) -> StagingOutboxCommand[Any]:
        store = _build_store(ctx, spec, self.config)
        return build_staging_outbox_command_for_store(ctx, spec, store)


ConfigurableMongoOutbox = ConfigurableMongoOutboxQuery
