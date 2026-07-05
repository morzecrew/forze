from ._compat import require_oidc

require_oidc()

# ....................... #

from datetime import UTC, datetime
from typing import Any, Mapping, final

import attrs

from forze.application.contracts.authn import VerifiedAssertion

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class OidcClaimMapper:
    """Map a verified OIDC token's claim payload onto a :class:`VerifiedAssertion`.

    Defaults follow the OIDC core spec (``iss`` / ``sub`` / ``aud`` / ``iat`` / ``exp``).
    Override the claim names when an IdP uses non-standard keys (e.g. Firebase puts the
    tenant id under ``firebase.tenant``).
    """

    issuer_claim: str = "iss"
    """The issuer claim to use."""

    subject_claim: str = "sub"
    """The subject claim to use."""

    audience_claim: str | None = "aud"
    """The audience claim to use."""

    issued_at_claim: str | None = "iat"
    """The issued-at claim to use."""

    expires_at_claim: str | None = "exp"
    """The expiry claim to use."""

    tenant_claim: str | None = None
    """When set, the resolver picks tenant context from this claim instead of leaving it ``None``."""

    # ....................... #

    def map(
        self,
        claims: Mapping[str, Any],
        *,
        validated_audience: str | None = None,
    ) -> VerifiedAssertion:
        issuer_raw = claims.get(self.issuer_claim)
        subject_raw = claims.get(self.subject_claim)

        if not isinstance(issuer_raw, str) or not isinstance(subject_raw, str):
            raise ValueError(
                f"OIDC claims missing required '{self.issuer_claim}' or '{self.subject_claim}'",
            )

        if not issuer_raw.strip() or not subject_raw.strip():
            raise ValueError(
                f"OIDC claims '{self.issuer_claim}' and '{self.subject_claim}' must be non-empty",
            )

        audience: str | None = None

        if validated_audience is not None:
            # The verifier passes the ``aud`` entry it actually validated against its
            # configured audience — record *that*, not an arbitrary first element of a
            # multi-audience token (which may be a different party's audience).
            audience = validated_audience

        elif self.audience_claim is not None:
            aud_raw = claims.get(self.audience_claim)

            if isinstance(aud_raw, str):
                audience = aud_raw

            elif isinstance(aud_raw, list) and aud_raw and isinstance(aud_raw[0], str):
                # No audience was enforced (mapper used standalone); OIDC permits aud as
                # an array, so fall back to the first string entry.
                audience = aud_raw[0]

        issuer_tenant_hint: str | None = None

        if self.tenant_claim is not None:
            tid_raw = claims.get(self.tenant_claim)

            if isinstance(tid_raw, str):
                issuer_tenant_hint = tid_raw

        return VerifiedAssertion(
            issuer=issuer_raw,
            subject=subject_raw,
            audience=audience,
            issuer_tenant_hint=issuer_tenant_hint,
            issued_at=self._coerce_timestamp(claims, self.issued_at_claim),
            expires_at=self._coerce_timestamp(claims, self.expires_at_claim),
            claims=dict(claims),
        )

    # ....................... #
    #! support for strings (?)

    @staticmethod
    def _coerce_timestamp(
        claims: Mapping[str, Any],
        name: str | None,
    ) -> datetime | None:
        """Coerce timestamp claim to a :class:`datetime` if present."""

        if name is None:
            return None

        raw = claims.get(name)

        if isinstance(raw, (int, float)):
            return datetime.fromtimestamp(raw, tz=UTC)

        return None
