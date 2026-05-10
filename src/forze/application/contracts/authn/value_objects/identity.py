from uuid import UUID

import attrs

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class AuthnIdentity:
    """Canonical authenticated subject inside Forze.

    Resolved from a :class:`~forze.application.contracts.authn.value_objects.assertion.VerifiedAssertion`
    by a :class:`~forze.application.contracts.authn.ports.resolution.PrincipalResolverPort`.
    Downstream consumers (use cases, guards, document/tenancy layers) only see this minimal shape.
    """

    principal_id: UUID
    """Internal principal identifier; aligns with :class:`~forze.application.contracts.authz.value_objects.PrincipalRef`."""

    tenant_id: UUID | None = None
    """When set, the credential or token was bound to this tenant scope."""
