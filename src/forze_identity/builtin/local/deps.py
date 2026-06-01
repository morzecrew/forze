"""Configurable factories for local identity verifiers and tenant resolvers."""

from typing import final

import attrs

from forze.application.contracts.authn import ApiKeyVerifierPort, AuthnSpec
from forze.application.contracts.tenancy import TenantResolverDepPort, TenantResolverPort
from forze.application.execution import ExecutionContext

from .api_key_verifier import LocalApiKeyVerifier
from .config import LocalIdentityConfig
from .tenant_resolver import LocalTenantResolver

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableLocalApiKeyVerifier:
    """Build :class:`LocalApiKeyVerifier` from a frozen local identity config."""

    config: LocalIdentityConfig
    """Static API key mapping."""

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: AuthnSpec,
    ) -> ApiKeyVerifierPort:
        _ = ctx, spec
        return LocalApiKeyVerifier(config=self.config)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableLocalTenantResolver(TenantResolverDepPort):
    """Build :class:`LocalTenantResolver` from a frozen local identity config."""

    config: LocalIdentityConfig
    """Static principal → tenant mapping."""

    # ....................... #

    def __call__(self, ctx: ExecutionContext) -> TenantResolverPort:
        _ = ctx
        return LocalTenantResolver(config=self.config)
