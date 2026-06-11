"""Generic OIDC :class:`TokenVerifierPort` implementation.

Reference for the seam introduced by the strategic authn refactor: a third-party IdP
package only needs to:

#. Implement :class:`forze.application.contracts.authn.TokenVerifierPort` (this is what
   :class:`OidcTokenVerifier` does, generically).
#. Optionally provide a custom :class:`OidcClaimMapper` for IdP-specific claims.
#. Wire its verifier under :class:`forze.application.contracts.authn.TokenVerifierDepKey`
   for the relevant routes; reuse a :class:`forze_authn` resolver
   (:class:`MappingTableResolver` or :class:`DeterministicUuidResolver`) to map external
   subjects to internal Forze principals.
"""

from .claims import OidcClaimMapper
from .keys import JwksKeyProvider, SigningKeyProviderPort, StaticKeyProvider
from .nonce import verify_id_token_nonce
from .preset import ConfigurableOidcIdpVerifier, OidcIdpPreset
from .verifier import OidcTokenVerifier

# ----------------------- #

__all__ = [
    "ConfigurableOidcIdpVerifier",
    "JwksKeyProvider",
    "OidcClaimMapper",
    "OidcIdpPreset",
    "OidcTokenVerifier",
    "SigningKeyProviderPort",
    "StaticKeyProvider",
    "verify_id_token_nonce",
]
