"""First-party verifier implementations producing :class:`VerifiedAssertion` outputs.

External IdP packages (forze_oidc, forze_firebase_auth, forze_casdoor) ship their own
verifiers and reuse the resolvers from :mod:`forze_authn.resolvers`.
"""

from .argon2_password import Argon2PasswordVerifier
from .forze_jwt_token import ForzeJwtTokenVerifier
from .hmac_api_key import HmacApiKeyVerifier

# ----------------------- #

__all__ = [
    "Argon2PasswordVerifier",
    "ForzeJwtTokenVerifier",
    "HmacApiKeyVerifier",
]
