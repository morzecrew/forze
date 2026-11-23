"""Convenience wiring for local identity on a single authn/tenancy route."""

from forze.application.contracts.deps import Deps
from forze_identity.authn.execution import AuthnDepsModule, AuthnKernelConfig, EligibilityChoice
from forze_identity.tenancy.execution.deps import TenancyDepsModule

from .config import LocalIdentityConfig
from .deps import (
    ConfigurableLocalApiKeyVerifier,
    ConfigurableLocalPrincipalEligibility,
    ConfigurableLocalTenantResolver,
)

# ----------------------- #


def local_identity_deps(
    config: LocalIdentityConfig,
    *,
    authn_route: str = "main",
    tenancy_route: str = "main",
    eligibility: EligibilityChoice | None = None,
) -> Deps:
    """Merge authn + tenancy deps for API-key-only local identity (demo/MVP).

    The tenant resolver is registered only when *config* maps a tenant (per principal, on
    an API key, or by default): one that maps none could never bind a tenant, and a
    registered resolver declares a tenanted deployment, whose realtime connections must
    carry one.

    :param config: Frozen local identity configuration.
    :param authn_route: Route name for :class:`AuthnDepsModule` registration.
    :param tenancy_route: Route name for :class:`TenancyDepsModule` registration.
    :param eligibility: The principal-eligibility gate, as
        :attr:`AuthnDepsModule.eligibility` takes it. ``None`` (default) builds
        :class:`ConfigurableLocalPrincipalEligibility`: the authz plane's policy-principal
        gate where its document resolves, and otherwise a gate admitting exactly the
        principals *config* names on an API key. Pass ``"policy_principal"`` to insist on the
        authz gate wherever the preset runs.
    :returns: Merged :class:`Deps` ready to merge into the application kernel.
    """

    authn = AuthnDepsModule(
        kernel=AuthnKernelConfig(),
        authn={authn_route: frozenset({"api_key"})},
        api_key_verifiers={
            authn_route: ConfigurableLocalApiKeyVerifier(config=config),
        },
        eligibility=(
            ConfigurableLocalPrincipalEligibility(config=config)
            if eligibility is None
            else eligibility
        ),
    )()

    if not config.maps_a_tenant:
        return authn

    tenancy = TenancyDepsModule(
        tenant_resolver={tenancy_route},
        tenant_resolvers={
            tenancy_route: ConfigurableLocalTenantResolver(config=config),
        },
    )()

    return authn.merge(tenancy)
