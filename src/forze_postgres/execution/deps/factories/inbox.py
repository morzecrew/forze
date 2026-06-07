"""Postgres inbox dep factory."""

from __future__ import annotations

from typing import TYPE_CHECKING, final

import attrs

from ....adapters.inbox import PostgresInboxStore
from ..configs.inbox import PostgresInboxConfig
from ..keys import PostgresClientDepKey

if TYPE_CHECKING:
    from forze.application.contracts.inbox import InboxSpec
    from forze.application.execution.context import ExecutionContext

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurablePostgresInbox:
    """Build a :class:`PostgresInboxStore` for an inbox spec route."""

    config: PostgresInboxConfig
    """Postgres-specific configuration for the route."""

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: InboxSpec,
    ) -> PostgresInboxStore:
        client = ctx.deps.provide(PostgresClientDepKey)
        return PostgresInboxStore(
            client=client,
            spec=spec,
            config=self.config,
            tenant_aware=self.config.tenant_aware,
            tenant_provider=ctx.inv_ctx.get_tenant,
        )
