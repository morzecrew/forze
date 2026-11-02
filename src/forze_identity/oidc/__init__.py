"""Generic OIDC :class:`TokenVerifierPort` implementations.

Reference for the seam introduced by the strategic authn refactor: a third-party IdP
package only needs to:

#. Implement :class:`forze.application.contracts.authn.TokenVerifierPort` (this is what
   :class:`OidcTokenVerifier` does for JWTs, :class:`IntrospectionTokenVerifier` for opaque
   tokens and :class:`WhoamiSessionVerifier` for Kratos sessions, generically).
#. Optionally provide a custom :class:`OidcClaimMapper` for IdP-specific claims, with
   :class:`ClaimsPath` / :class:`ZitadelProjectRoles` extractors for roles and groups.
#. Wire its verifier under :class:`forze.application.contracts.authn.TokenVerifierDepKey`
   for the relevant routes; reuse a :class:`forze_authn` resolver
   (:class:`MappingTableResolver` or :class:`DeterministicUuidResolver`) to map external
   subjects to internal Forze principals.

:meth:`OidcIdpPreset.from_issuer` builds a preset from the issuer's discovery document.
"""

from .claims import (
    FORZE_CLAIMS,
    ClaimsExtractor,
    ClaimsPath,
    OidcClaimMapper,
    ZitadelProjectRoles,
)
from .discovery import OidcProviderMetadata, fetch_oidc_configuration
from .introspection import (
    BasicClientAuth,
    ConfigurableIntrospectionVerifier,
    IntrospectionConfig,
    IntrospectionTokenVerifier,
    PrivateKeyJwtAuth,
)
from .keys import JwksKeyProvider, SigningKeyProviderPort, StaticKeyProvider
from .nonce import verify_id_token_nonce
from .preset import ConfigurableOidcIdpVerifier, OidcIdpPreset
from .verifier import OidcTokenVerifier
from .whoami import ConfigurableWhoamiVerifier, WhoamiConfig, WhoamiSessionVerifier

# ----------------------- #

__all__ = [
    "FORZE_CLAIMS",
    "BasicClientAuth",
    "ClaimsExtractor",
    "ClaimsPath",
    "ConfigurableIntrospectionVerifier",
    "ConfigurableOidcIdpVerifier",
    "ConfigurableWhoamiVerifier",
    "IntrospectionConfig",
    "IntrospectionTokenVerifier",
    "JwksKeyProvider",
    "OidcClaimMapper",
    "OidcIdpPreset",
    "OidcProviderMetadata",
    "OidcTokenVerifier",
    "PrivateKeyJwtAuth",
    "SigningKeyProviderPort",
    "StaticKeyProvider",
    "WhoamiConfig",
    "WhoamiSessionVerifier",
    "ZitadelProjectRoles",
    "fetch_oidc_configuration",
    "verify_id_token_nonce",
]
