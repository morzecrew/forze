"""Catalog references for roles, permissions, groups, and principals."""

from uuid import UUID

import attrs

from ..types import PrincipalKind

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class PermissionRef:
    """Stable reference to a permission catalog entry."""

    permission_id: UUID
    permission_key: str


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class RoleRef:
    """Stable reference to a role catalog entry."""

    role_id: UUID
    role_key: str


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class GroupRef:
    """Stable reference to a group catalog entry."""

    group_id: UUID
    group_key: str


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class PrincipalRef:
    """Stable anchor for principals that receive roles and permission checks."""

    principal_id: UUID
    kind: PrincipalKind
    is_active: bool = True
