"""Bootstrap authn deps for external OIDC ``id_token`` routes."""

from forze.application.contracts.authn import PrincipalResolverDepPort, TokenVerifierDepPort
from forze.application.contracts.deps import Deps
from forze_identity.authn.execution import AuthnDepsModule, AuthnKernelConfig
from forze_identity.authn.execution.deps import ConfigurableDeterministicUuidResolver

from ._compat import require_oidc

require_oidc()

# ----------------------- #


def oidc_bootstrap_identity_deps(
    *,
    authn_route: str,
    token_verifier: TokenVerifierDepPort,
    resolver: PrincipalResolverDepPort | None = None,
) -> Deps:
    """Authn deps for an OIDC bootstrap route (external ``id_token`` only).

    Uses an empty :class:`AuthnKernelConfig` — valid because the route registers a
    ``token_verifiers`` override (no first-party JWT secret required on this route).
    """

    resolved_resolver = resolver or ConfigurableDeterministicUuidResolver()

    return AuthnDepsModule(
        kernel=AuthnKernelConfig(),
        authn={authn_route: frozenset({"token"})},
        token_verifiers={authn_route: token_verifier},
        resolvers={authn_route: resolved_resolver},
    )()
