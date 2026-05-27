"""File/env-backed local identity configuration for demos and MVPs (not for production)."""

from forze.application.execution import Deps

from .config import LocalApiKeyEntry, LocalIdentityConfig
from .load import from_env, from_json_path, from_mapping

# ----------------------- #


def local_identity_deps(
    config: LocalIdentityConfig,
    *,
    authn_route: str = "main",
    tenancy_route: str = "main",
) -> Deps[str]:
    """Lazy wrapper avoiding import cycles with :mod:`forze_identity.authn.execution`."""

    from .wiring import local_identity_deps as _impl

    return _impl(config, authn_route=authn_route, tenancy_route=tenancy_route)


# ....................... #

__all__ = [
    "LocalApiKeyEntry",
    "LocalIdentityConfig",
    "from_env",
    "from_json_path",
    "from_mapping",
    "local_identity_deps",
]
