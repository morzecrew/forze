"""Mongo idempotency dep factory."""

from __future__ import annotations

from typing import TYPE_CHECKING, final

import attrs

from ....adapters.idempotency import MongoIdempotencyStore
from ..configs.idempotency import MongoIdempotencyConfig
from ..keys import MongoClientDepKey

if TYPE_CHECKING:
    from forze.application.contracts.idempotency import IdempotencySpec
    from forze.application.execution.context import ExecutionContext

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableMongoIdempotency:
    """Build a :class:`MongoIdempotencyStore` for an idempotency spec route."""

    config: MongoIdempotencyConfig
    """Mongo-specific configuration for the route."""

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: IdempotencySpec,
    ) -> MongoIdempotencyStore:
        client = ctx.deps.provide(MongoClientDepKey)
        return MongoIdempotencyStore(
            client=client,
            spec=spec,
            config=self.config,
            tenant_aware=self.config.tenant_aware,
            tenant_provider=ctx.inv_ctx.get_tenant,
            owner_provider=ctx.inv_ctx.get_execution_id,
        )
