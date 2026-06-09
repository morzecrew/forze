"""Optional ``forze[oidc]`` guard for builtin IdP presets."""

from forze_identity.oidc._compat import require_oidc

__all__ = ["require_oidc"]
