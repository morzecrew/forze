"""Authorization contract value objects — policy-facing shapes, not domain entities."""

from datetime import datetime
from uuid import UUID

import attrs

from forze.base.primitives import utcnow

from .types import PrincipalKind

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class PrincipalRef:
    """Stable anchor for principals that receive roles and permission checks."""

    principal_id: UUID
    """Identifier shared with authentication."""

    kind: PrincipalKind
    """Rough class of actor; adapters may map this to storage or naming."""

    is_active: bool = True
    """When false, adapters should deny authentication-adjacent use (policy-dependent, etc.)."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class EffectiveGrants:
    """Effective grants for a principal."""

    roles: frozenset[str] = attrs.field(factory=frozenset)
    """Roles assigned to the principal."""

    permissions: frozenset[str] = attrs.field(factory=frozenset)
    """Permissions assigned to the principal."""

    resolved_at: datetime = attrs.field(factory=utcnow)
    """Timestamp when the grants were resolved."""
