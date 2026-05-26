import attrs

from .identity import AuthnIdentity

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class AuthnResult:
    """Boundary authn result for one successful authentication attempt.

    Keeps the canonical authenticated principal separate from issuer-originated
    tenant metadata so middleware can pass the latter into tenancy resolution
    without smuggling it into :class:`AuthnIdentity`.
    """

    identity: AuthnIdentity
    """Canonical authenticated principal."""

    issuer_tenant_hint: str | None = None
    """Non-authoritative tenant hint asserted by the credential issuer."""
