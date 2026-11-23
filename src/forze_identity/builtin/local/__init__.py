"""File/env-backed local identity configuration for demos and MVPs (not for production)."""

from typing import TYPE_CHECKING

from forze.application.contracts.deps import Deps

from .api_key_verifier import LocalApiKeyVerifier
from .config import LocalApiKeyEntry, LocalIdentityConfig
from .deps import (
    ConfigurableLocalApiKeyVerifier,
    ConfigurableLocalPrincipalEligibility,
    ConfigurableLocalTenantResolver,
)
from .eligibility import LocalPrincipalEligibility
from .load import from_env, from_json_path, from_mapping
from .tenant_resolver import LocalTenantResolver

if TYPE_CHECKING:
    from forze_identity.authn.execution import EligibilityChoice

# ----------------------- #


def local_identity_deps(
    config: LocalIdentityConfig,
    *,
    authn_route: str = "main",
    tenancy_route: str = "main",
    eligibility: "EligibilityChoice | None" = None,
) -> Deps:
    """Lazy wrapper avoiding import cycles with :mod:`forze_identity.authn.execution`."""

    from .wiring import local_identity_deps as _impl

    return _impl(
        config,
        authn_route=authn_route,
        tenancy_route=tenancy_route,
        eligibility=eligibility,
    )


# ....................... #

__all__ = [
    "ConfigurableLocalApiKeyVerifier",
    "ConfigurableLocalPrincipalEligibility",
    "ConfigurableLocalTenantResolver",
    "LocalApiKeyEntry",
    "LocalApiKeyVerifier",
    "LocalIdentityConfig",
    "LocalPrincipalEligibility",
    "LocalTenantResolver",
    "from_env",
    "from_json_path",
    "from_mapping",
    "local_identity_deps",
]
