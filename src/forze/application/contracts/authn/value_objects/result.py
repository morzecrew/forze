from datetime import datetime

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

    expires_at: datetime | None = None
    """When the presented credential expires, as the verifier asserted it — a
    timezone-aware instant, or ``None`` when the credential carries no expiry.

    A request-scoped boundary discards it (the request is over long before the token
    is), but a *connection* outlives it: a realtime transport holds one verified
    credential open for hours, so it needs the instant to close the socket on, and
    the alternative is every resolver decoding the token a second time — behind the
    verifier's back, in a format only some verifiers speak."""
