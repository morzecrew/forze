"""Convenience wiring for local identity on a single authn/tenancy route."""

from forze.application.execution import Deps

from ..authn.execution import AuthnDepsModule, AuthnKernelConfig
from ..authn.execution.deps import ConfigurableLocalApiKeyVerifier
from ..tenancy.execution.deps import ConfigurableLocalTenantResolver, TenancyDepsModule
from .config import LocalIdentityConfig

# ----------------------- #


def local_identity_deps(
    config: LocalIdentityConfig,
    *,
    authn_route: str = "main",
    tenancy_route: str = "main",
) -> Deps:
    """Merge authn + tenancy deps for API-key-only local identity (demo/MVP).

    :param config: Frozen local identity configuration.
    :param authn_route: Route name for :class:`AuthnDepsModule` registration.
    :param tenancy_route: Route name for :class:`TenancyDepsModule` registration.
    :returns: Merged :class:`Deps` ready to merge into the application kernel.
    """

    authn = AuthnDepsModule(
        kernel=AuthnKernelConfig(),
        authn={authn_route: frozenset({"api_key"})},
        api_key_verifiers={
            authn_route: ConfigurableLocalApiKeyVerifier(config=config),
        },
    )()

    tenancy = TenancyDepsModule(
        tenant_resolver={tenancy_route},
        tenant_resolvers={
            tenancy_route: ConfigurableLocalTenantResolver(config=config),
        },
    )()

    return authn.merge(tenancy)
