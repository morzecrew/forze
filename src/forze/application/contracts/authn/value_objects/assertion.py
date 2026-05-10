from collections.abc import Mapping
from datetime import datetime
from typing import Any

import attrs

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class VerifiedAssertion:
    """Vendor-flavored proof produced by a credential verifier.

    Single seam between credential verification and principal resolution. Holds external
    identifiers (e.g. ``iss`` / ``sub`` claims, Firebase UID, Casdoor user id) and optional
    metadata; consumed by a
    :class:`~forze.application.contracts.authn.ports.resolution.PrincipalResolverPort`
    to produce a canonical
    :class:`~forze.application.contracts.authn.value_objects.identity.AuthnIdentity`.
    """

    issuer: str
    """Stable identifier of the authority that produced the assertion (e.g. an ``iss`` URL or a sentinel like ``"forze:jwt"``)."""

    subject: str
    """Raw external subject identifier as provided by the issuer (string form, not coerced to UUID)."""

    audience: str | None = attrs.field(default=None)
    """Optional audience the assertion is bound to."""

    tenant_hint: str | None = attrs.field(default=None)
    """Raw tenant identifier as provided by the issuer; resolvers decide how to map it."""

    issued_at: datetime | None = attrs.field(default=None)
    """Optional issued-at timestamp."""

    expires_at: datetime | None = attrs.field(default=None)
    """Optional expiry timestamp."""

    claims: Mapping[str, Any] = attrs.field(factory=dict[str, Any])
    """Opaque claims snapshot kept for resolvers and audit trails; not consumed by domain code."""
