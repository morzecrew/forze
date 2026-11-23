"""Configurable factories for local identity verifiers, eligibility and tenant resolvers."""

from typing import final

import attrs

from forze.application.contracts.authn import (
    ApiKeyVerifierPort,
    AuthnSpec,
    PrincipalEligibilityPort,
)
from forze.application.contracts.document import DocumentQueryDepKey
from forze.application.contracts.tenancy import TenantResolverDepPort, TenantResolverPort
from forze.application.execution import ExecutionContext
from forze_identity.authn.execution.deps.deps import ConfigurablePolicyPrincipalEligibility
from forze_identity.authz.application.specs import policy_principal_spec

from .api_key_verifier import LocalApiKeyVerifier
from .config import LocalIdentityConfig
from .eligibility import LocalPrincipalEligibility
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
class ConfigurableLocalPrincipalEligibility:
    """Today's policy-principal gate where its store resolves; the config-backed gate elsewhere.

    Decided when the gate is built. Where ``authz_policy_principals`` resolves, routed or
    through a plain fallback such as the mock's, this builds exactly the gate
    :class:`~forze_identity.authn.execution.AuthnDepsModule` defaults to, so an application that
    deactivates principals through the authz registry keeps that. Elsewhere no other registry
    exists, and the preset's config is the registry (:class:`LocalPrincipalEligibility`).
    Neither branch skips a read: the preset's kernel carries no revocation states.
    """

    config: LocalIdentityConfig
    """Static API key mapping; the principals it names are the eligible ones."""

    _local: LocalPrincipalEligibility = attrs.field(
        init=False,
        default=attrs.Factory(
            lambda self: LocalPrincipalEligibility.of(self.config),
            takes_self=True,
        ),
    )

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: AuthnSpec,
    ) -> PrincipalEligibilityPort:
        if ctx.deps.resolvable(DocumentQueryDepKey, route=policy_principal_spec.name):
            return ConfigurablePolicyPrincipalEligibility()(ctx, spec)

        return self._local


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
