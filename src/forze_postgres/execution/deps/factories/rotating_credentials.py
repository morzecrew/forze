"""Postgres rotating-credential-store dep factory."""

from __future__ import annotations

from typing import TYPE_CHECKING, final

import attrs

from forze.application.contracts.crypto import KeyringDepKey
from forze.base.exceptions import exc

from ....adapters.rotating_credentials import (
    PostgresRotatingCredentialsAdmin,
    PostgresRotatingCredentialStore,
)
from ..configs.rotating_credentials import PostgresRotatingCredentialsConfig
from ..keys import PostgresClientDepKey

if TYPE_CHECKING:
    from forze.application.execution.context import ExecutionContext

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurablePostgresRotatingCredentials:
    """Build a :class:`PostgresRotatingCredentialStore` for the rotating-credentials port.

    Execution-scoped: resolves the client and, when the route seals credentials at rest, the
    keyring — failing closed if encryption is requested without one.
    """

    config: PostgresRotatingCredentialsConfig
    """Postgres-specific configuration for the credential store."""

    def __call__(self, ctx: ExecutionContext) -> PostgresRotatingCredentialStore:
        cipher = None

        if self.config.encrypt:
            if not ctx.deps.exists(KeyringDepKey):
                raise exc.configuration(
                    "Rotating-credential encryption is enabled but no keyring is wired. "
                    "Register a CryptoDepsModule, or set encrypt=False with "
                    "acknowledge_plaintext=True to store credentials in the clear.",
                )

            cipher = ctx.deps.provide(KeyringDepKey)

        return PostgresRotatingCredentialStore(
            client=ctx.deps.provide(PostgresClientDepKey),
            relation=self.config.relation,
            exchanger=self.config.exchanger,
            exchange_timeout=self.config.exchange_timeout,
            cipher=cipher,
            tenant_aware=self.config.tenant_aware,
            tenant_provider=ctx.inv_ctx.get_tenant,
        )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurablePostgresRotatingCredentialsAdmin:
    """Build a :class:`PostgresRotatingCredentialsAdmin` for the control-plane scan.

    Registered alongside the store from the same config, so the scan reads exactly the
    table the store writes. Needs no exchanger and no keyring — the admin plane never
    opens a payload.
    """

    config: PostgresRotatingCredentialsConfig
    """The store's configuration; only ``relation`` and ``tenant_aware`` are read."""

    def __call__(self, ctx: ExecutionContext) -> PostgresRotatingCredentialsAdmin:
        return PostgresRotatingCredentialsAdmin(
            client=ctx.deps.provide(PostgresClientDepKey),
            relation=self.config.relation,
            tenant_aware=self.config.tenant_aware,
            tenant_provider=ctx.inv_ctx.get_tenant,
        )
