"""Postgres rotating-credential-store dep factory."""

from __future__ import annotations

from typing import TYPE_CHECKING, final

import attrs

from ....adapters.rotating_credentials import PostgresRotatingCredentialStore
from ..configs.rotating_credentials import PostgresRotatingCredentialsConfig
from ..keys import PostgresClientDepKey

if TYPE_CHECKING:
    from forze.application.execution.context import ExecutionContext

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurablePostgresRotatingCredentials:
    """Build a :class:`PostgresRotatingCredentialStore` for the rotating-credentials port."""

    config: PostgresRotatingCredentialsConfig
    """Postgres-specific configuration for the credential store."""

    def __call__(self, ctx: ExecutionContext) -> PostgresRotatingCredentialStore:
        return PostgresRotatingCredentialStore(
            client=ctx.deps.provide(PostgresClientDepKey),
            relation=self.config.relation,
            exchanger=self.config.exchanger,
            exchange_timeout=self.config.exchange_timeout,
            tenant_aware=self.config.tenant_aware,
            tenant_provider=ctx.inv_ctx.get_tenant,
        )
