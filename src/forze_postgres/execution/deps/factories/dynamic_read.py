"""Postgres governed dynamic-read dep factory."""

from typing import TYPE_CHECKING, final

import attrs

from ....adapters.dynamic_read import PostgresDynamicReadAdapter
from ..configs import PostgresDynamicReadConfig
from ..keys import PostgresClientDepKey

if TYPE_CHECKING:
    from forze.application.contracts.dynamic_read import DynamicReadSpec
    from forze.application.execution.context import ExecutionContext

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurablePostgresDynamicRead:
    """Build a :class:`PostgresDynamicReadAdapter` for a dynamic-read route."""

    config: PostgresDynamicReadConfig
    """Postgres-specific configuration for the route."""

    # ....................... #

    def __call__(
        self,
        ctx: "ExecutionContext",
        spec: "DynamicReadSpec",
    ) -> PostgresDynamicReadAdapter:
        # No codec resolution and no keyring: the plane declares no encryption, because a
        # statement's output shape is unknowable and a sealed column would come back as
        # ciphertext no matter what was wired here.
        return PostgresDynamicReadAdapter(
            client=ctx.deps.provide(PostgresClientDepKey),
            spec=spec,
            config=self.config,
            statement_timeout=self.config.statement_timeout,
            tenant_aware=self.config.tenant_aware,
            tenant_provider=ctx.inv_ctx.get_tenant,
        )
