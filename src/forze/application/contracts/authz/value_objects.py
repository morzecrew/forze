"""Authorization contract value objects — policy-facing shapes, not domain entities."""

from typing import Literal
from uuid import UUID

import attrs

# ----------------------- #

PrincipalKind = Literal["user", "service"]
"""Kind of principal."""

# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class PrincipalRef:
    """Stable anchor for principals that receive roles and permission checks."""

    principal_id: UUID
    """Identifier shared with authentication (see :attr:`AuthnIdentity.principal_id`)."""

    kind: PrincipalKind
    """Rough class of actor; adapters may map this to storage or naming."""

    #! Hmmm ... questionable - maybe 'is_active' or so
    deactivated: bool = attrs.field(default=False)
    """When true, adapters should deny authentication-adjacent use (policy-dependent)."""
