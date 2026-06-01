"""OAuth 2.x helpers (PKCE, token exchange utilities).

PKCE and authorization-code helpers used by :mod:`forze_identity.builtin.idp` presets
and custom IdP integrations. No optional extras required for PKCE (stdlib only).
"""

from .pkce import PkcePair, generate_pkce

__all__ = [
    "PkcePair",
    "generate_pkce",
]
