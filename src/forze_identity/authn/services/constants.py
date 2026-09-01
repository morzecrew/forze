"""Shared constants for the authn services."""

MIN_SECRET_BYTES = 32
"""Shortest secret, pepper or signing key any authn service accepts.

Everything it guards is SHA-256-based — the peppered-HMAC digests and the HS256
signatures — so 32 bytes is the key length at which the key stops being the weakest part
of the construction. Exported (``forze_identity.MIN_SECRET_BYTES``) because an application that
validates its own settings — so a short secret fails at boot naming the environment
variable, rather than later inside an attrs validator naming a field — otherwise copies
the number, and a copied number drifts.
"""

# ----------------------- #

__all__ = ["MIN_SECRET_BYTES"]
