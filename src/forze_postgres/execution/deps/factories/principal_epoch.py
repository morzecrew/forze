"""Postgres principal epoch dep factory."""

from __future__ import annotations

from typing import TYPE_CHECKING, final

import attrs

from ....adapters.principal_epoch import PostgresPrincipalEpochStore
from ..configs.principal_epoch import PostgresPrincipalEpochConfig
from ..keys import PostgresClientDepKey

if TYPE_CHECKING:
    from forze.application.execution.context import ExecutionContext

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurablePostgresPrincipalEpoch:
    """Build a :class:`PostgresPrincipalEpochStore` — one store per deployment."""

    config: PostgresPrincipalEpochConfig
    """Postgres-specific configuration (relation)."""

    def __call__(self, ctx: ExecutionContext) -> PostgresPrincipalEpochStore:
        return PostgresPrincipalEpochStore(
            client=ctx.deps.provide(PostgresClientDepKey),
            config=self.config,
        )
