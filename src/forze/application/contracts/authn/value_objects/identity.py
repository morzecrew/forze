from uuid import UUID

import attrs

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class AuthnIdentity:
    """Canonical authenticated subject inside Forze.

    Resolved from a :class:`~forze.application.contracts.authn.value_objects.assertion.VerifiedAssertion`
    by a :class:`~forze.application.contracts.authn.ports.resolution.PrincipalResolverPort`.
    Downstream consumers (use cases, guards, document/tenancy layers) only see this
    principal-only shape. Effective tenant context lives separately on
    :class:`~forze.application.contracts.tenancy.value_objects.TenantIdentity`.
    """

    principal_id: UUID
    """Internal principal identifier; aligns with :class:`~forze.application.contracts.authz.value_objects.PrincipalRef`."""
