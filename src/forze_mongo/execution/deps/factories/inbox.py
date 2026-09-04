"""Mongo inbox dep factory."""

from __future__ import annotations

from typing import TYPE_CHECKING, final

import attrs

from ....adapters.inbox import MongoInboxStore
from ..configs.inbox import MongoInboxConfig
from ..keys import MongoClientDepKey

if TYPE_CHECKING:
    from forze.application.contracts.inbox import InboxSpec
    from forze.application.execution.context import ExecutionContext

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableMongoInbox:
    """Build a :class:`MongoInboxStore` for an inbox spec route."""

    config: MongoInboxConfig
    """Mongo-specific configuration for the route."""

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: InboxSpec,
    ) -> MongoInboxStore:
        client = ctx.deps.provide(MongoClientDepKey)
        return MongoInboxStore(
            client=client,
            spec=spec,
            config=self.config,
            tenant_aware=self.config.tenant_aware,
            tenant_provider=ctx.inv_ctx.get_tenant,
        )
