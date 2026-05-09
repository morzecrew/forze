"""Authorization contract value objects — policy-facing shapes, not domain entities."""

from datetime import datetime
from uuid import UUID

import attrs

from forze.base.errors import CoreError
from forze.base.primitives import utcnow

from .types import PrincipalKind

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class PermissionRef:
    """Stable reference to a permission catalog entry."""

    permission_id: UUID
    """Surrogate identifier for the permission in storage (joins, bindings)."""

    permission_key: str
    """Stable human-facing key (APIs, guards, logging)."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class RoleRef:
    """Stable reference to a role catalog entry."""

    role_id: UUID
    """Surrogate identifier for the role in storage (joins, bindings)."""

    role_key: str
    """Stable human-facing key (APIs, guards, logging)."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class GroupRef:
    """Stable reference to a group catalog entry."""

    group_id: UUID
    """Surrogate identifier for the group in storage (joins, bindings)."""

    group_key: str
    """Stable human-facing key (APIs, guards, logging)."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class PrincipalRef:
    """Stable anchor for principals that receive roles and permission checks."""

    principal_id: UUID
    """Identifier shared with authentication."""

    kind: PrincipalKind
    """Rough class of actor; adapters may map this to storage or naming."""

    is_active: bool = True
    """When false, adapters should deny authentication-adjacent use (policy-dependent, etc.)."""

    tenant_id: UUID | None = None
    """Optional tenant partition for authorization calls; ``None`` means unscoped."""


# ....................... #


def coalesce_authz_tenant_id(
    principal: PrincipalRef | UUID,
    *,
    tenant_id: UUID | None,
) -> UUID | None:
    """Return the effective tenant scope for an authz call.

    Explicit ``tenant_id`` wins over :attr:`PrincipalRef.tenant_id`. When both are
    set they must match or :class:`~forze.base.errors.CoreError` is raised.

    :param principal: Policy principal reference or bare principal id.
    :param tenant_id: Explicit tenant from the port call site.
    :returns: Resolved tenant id or ``None`` when the call is tenant-unscoped.
    """

    ref_tid = principal.tenant_id if isinstance(principal, PrincipalRef) else None

    if tenant_id is not None and ref_tid is not None and tenant_id != ref_tid:
        raise CoreError(
            "Conflicting tenant_id: PrincipalRef.tenant_id and explicit tenant_id disagree",
        )

    if tenant_id is not None:
        return tenant_id

    return ref_tid


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class EffectiveGrants:
    """Effective grants for a principal."""

    roles: frozenset[RoleRef] = attrs.field(factory=frozenset)
    """Roles directly assigned (principal-role and group-role grants)."""

    permissions: frozenset[PermissionRef] = attrs.field(factory=frozenset)
    """Effective permissions including role expansion and direct grants."""

    resolved_at: datetime = attrs.field(factory=utcnow)
    """Timestamp when the grants were resolved."""
