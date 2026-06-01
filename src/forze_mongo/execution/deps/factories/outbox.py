"""Mongo outbox dep factory."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, final

import attrs

from ....adapters.outbox import MongoOutboxAdapter
from ..configs.outbox import MongoOutboxConfig
from ..keys import MongoClientDepKey

if TYPE_CHECKING:
    from forze.application.contracts.outbox import OutboxSpec
    from forze.application.execution.context import ExecutionContext

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableMongoOutbox:
    """Build a :class:`MongoOutboxAdapter` for an outbox spec route."""

    config: MongoOutboxConfig
    """Mongo-specific configuration for the route."""

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: OutboxSpec[Any],
    ) -> MongoOutboxAdapter[Any]:
        client = ctx.deps.provide(MongoClientDepKey)
        return MongoOutboxAdapter(
            ctx=ctx,
            client=client,
            spec=spec,
            config=self.config,
            tenant_aware=self.config.tenant_aware,
            tenant_provider=ctx.inv_ctx.get_tenant,
        )
