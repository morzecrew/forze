"""Effective grant snapshots for a subject."""

from datetime import datetime

import attrs

from forze.base.primitives import utcnow

from .catalog import PermissionRef, RoleRef

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class EffectiveGrants:
    """Effective grants for a principal."""

    roles: frozenset[RoleRef] = attrs.field(factory=frozenset)
    permissions: frozenset[PermissionRef] = attrs.field(factory=frozenset)
    resolved_at: datetime = attrs.field(factory=utcnow)
