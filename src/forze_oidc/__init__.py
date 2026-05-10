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

No core contract changes are required to support new IdPs (Firebase, Casdoor, Auth0, …);
each ships its own thin package built on top of :mod:`forze_oidc`.
"""

from .claims import OidcClaimMapper
from .keys import JwksKeyProvider, SigningKeyProviderPort, StaticKeyProvider
from .verifier import OidcTokenVerifier

# ----------------------- #

__all__ = [
    "JwksKeyProvider",
    "OidcClaimMapper",
    "OidcTokenVerifier",
    "SigningKeyProviderPort",
    "StaticKeyProvider",
]
