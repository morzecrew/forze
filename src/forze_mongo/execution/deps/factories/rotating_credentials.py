"""Mongo rotating-credential-store dep factories."""

from __future__ import annotations

from typing import TYPE_CHECKING, final

import attrs

from forze.application.contracts.crypto import KeyringDepKey
from forze.base.exceptions import exc

from ....adapters.rotating_credentials import (
    MongoRotatingCredentialsAdmin,
    MongoRotatingCredentialStore,
)
from ..configs.rotating_credentials import MongoRotatingCredentialsConfig
from ..keys import MongoClientDepKey

if TYPE_CHECKING:
    from forze.application.execution.context import ExecutionContext

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableMongoRotatingCredentials:
    """Build a :class:`MongoRotatingCredentialStore` for the rotating-credentials port.

    Execution-scoped: resolves the client and, when the route seals credentials at rest, the
    keyring — failing closed if encryption is requested without one.
    """

    config: MongoRotatingCredentialsConfig
    """Mongo-specific configuration for the credential store."""

    def __call__(self, ctx: ExecutionContext) -> MongoRotatingCredentialStore:
        cipher = None

        if self.config.encrypt:
            if not ctx.deps.exists(KeyringDepKey):
                raise exc.configuration(
                    "Rotating-credential encryption is enabled but no keyring is wired. "
                    "Register a CryptoDepsModule, or set encrypt=False with "
                    "acknowledge_plaintext=True to store credentials in the clear.",
                )

            cipher = ctx.deps.provide(KeyringDepKey)

        return MongoRotatingCredentialStore(
            client=ctx.deps.provide(MongoClientDepKey),
            config=self.config,
            exchanger=self.config.exchanger,
            exchange_timeout=self.config.exchange_timeout,
            cipher=cipher,
            tenant_aware=self.config.tenant_aware,
            tenant_provider=ctx.inv_ctx.get_tenant,
        )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableMongoRotatingCredentialsAdmin:
    """Build a :class:`MongoRotatingCredentialsAdmin` for the control-plane scan.

    Registered alongside the store from the same config, so the scan reads exactly the
    collection the store writes. Needs no exchanger and no keyring — the admin plane never
    opens a payload.
    """

    config: MongoRotatingCredentialsConfig
    """The store's configuration; only ``collection`` and ``tenant_aware`` are read."""

    def __call__(self, ctx: ExecutionContext) -> MongoRotatingCredentialsAdmin:
        return MongoRotatingCredentialsAdmin(
            client=ctx.deps.provide(MongoClientDepKey),
            config=self.config,
            tenant_aware=self.config.tenant_aware,
            tenant_provider=ctx.inv_ctx.get_tenant,
        )
