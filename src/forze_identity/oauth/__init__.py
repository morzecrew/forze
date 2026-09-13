"""OAuth 2.x helpers (PKCE, token exchange utilities).

PKCE and authorization-code helpers used by :mod:`forze_identity.builtin.idp` presets
and custom IdP integrations. No optional extras required for PKCE (stdlib only).
"""

from .authorize import build_authorize_url
from .callback import (
    CALLBACK_NO_CODE_CODE,
    CALLBACK_PROVIDER_ERROR_CODE,
    CALLBACK_STATE_MISMATCH_CODE,
    read_authorization_callback,
)
from .pkce import PkcePair, generate_pkce
from .state import generate_nonce, generate_state

__all__ = [
    "CALLBACK_NO_CODE_CODE",
    "CALLBACK_PROVIDER_ERROR_CODE",
    "CALLBACK_STATE_MISMATCH_CODE",
    "PkcePair",
    "build_authorize_url",
    "read_authorization_callback",
    "generate_nonce",
    "generate_pkce",
    "generate_state",
]
