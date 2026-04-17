from uuid import UUID

from pydantic import Field

from forze.domain.mixins import NameMixin
from forze.domain.models import CoreModel, Document

from ..enums.iam import IamPrincipalKind
from ..mixins import IsActiveMixin, TenantIdMixin

# ----------------------- #
#! review tenant mixin assignment later


class IamPrincipal(Document, NameMixin, IsActiveMixin, TenantIdMixin):
    """IAM principal model."""

    kind: IamPrincipalKind = Field(default=IamPrincipalKind.USER, frozen=True)
    """Principal kind."""


# ....................... #


class IamPermission(Document, NameMixin, IsActiveMixin):
    """IAM permission model."""

    resource: str | None = None
    """Resource name."""

    action: str | None = None
    """Action name."""


# ....................... #


class IamRole(Document, NameMixin, IsActiveMixin, TenantIdMixin):
    """IAM role model."""


# ....................... #


class IamGroup(Document, NameMixin, IsActiveMixin, TenantIdMixin):
    """IAM group model."""


# ....................... #


class IamPrincipalRoleReferences(CoreModel):
    """References to roles for a principal."""

    principal_id: UUID = Field(frozen=True)
    """Principal ID."""

    role_id: UUID = Field(frozen=True)
    """Role ID."""


class IamPrincipalRole(
    Document,
    TenantIdMixin,
    IamPrincipalRoleReferences,
):
    """Principal-to-role assignment."""


# ....................... #


class IamPrincipalPermissionReferences(CoreModel):
    """References to permissions for a principal."""

    principal_id: UUID = Field(frozen=True)
    """Principal ID."""

    permission_id: UUID = Field(frozen=True)
    """Permission ID."""


class IamPrincipalPermission(
    Document,
    TenantIdMixin,
    IamPrincipalPermissionReferences,
):
    """Principal-to-permission assignment."""


# ....................... #


class IamPrincipalGroupReferences(CoreModel):
    """References to groups for a principal."""

    principal_id: UUID = Field(frozen=True)
    """Principal ID."""

    group_id: UUID = Field(frozen=True)
    """Group ID."""


class IamPrincipalGroup(
    Document,
    TenantIdMixin,
    IamPrincipalGroupReferences,
):
    """Principal-to-group assignment."""


# ....................... #


class IamGroupRoleReferences(CoreModel):
    """References to roles for a group."""

    group_id: UUID = Field(frozen=True)
    """Group ID."""

    role_id: UUID = Field(frozen=True)
    """Role ID."""


class IamGroupRole(
    Document,
    TenantIdMixin,
    IamGroupRoleReferences,
):
    """Group-to-role assignment."""


# ....................... #


class IamRolePermissionReferences(CoreModel):
    """References to permissions for a role."""

    role_id: UUID = Field(frozen=True)
    """Role ID."""

    permission_id: UUID = Field(frozen=True)
    """Permission ID."""


class IamRolePermission(
    Document,
    TenantIdMixin,
    IamRolePermissionReferences,
):
    """Role-to-permission assignment."""
