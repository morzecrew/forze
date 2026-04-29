from uuid import UUID

from pydantic import Field

from forze.domain.mixins import NameMixin
from forze.domain.models import BaseDTO, CoreModel, CreateDocumentCmd, Document, ReadDocument

from ..enums.iam import IamPrincipalKind
from ..mixins import IsActiveMixin, TenantIdMixin

# ----------------------- #
#! review tenant mixin assignment later


class IamPrincipal(Document, NameMixin, IsActiveMixin, TenantIdMixin):
    """IAM principal model."""

    kind: IamPrincipalKind = Field(default=IamPrincipalKind.USER, frozen=True)
    """Principal kind."""


class CreateIamPrincipalCmd(CreateDocumentCmd, NameMixin, TenantIdMixin):
    """Create IAM principal command."""

    kind: IamPrincipalKind = Field(default=IamPrincipalKind.USER, frozen=True)
    """Principal kind."""

    is_active: bool = True
    """Whether the principal is active."""


class UpdateIamPrincipalCmd(BaseDTO):
    """Update IAM principal command."""

    name: str | None = None
    """Principal name."""

    is_active: bool | None = None
    """Whether the principal is active."""


class ReadIamPrincipal(ReadDocument, NameMixin, IsActiveMixin, TenantIdMixin):
    """Read IAM principal model."""

    kind: IamPrincipalKind = Field(default=IamPrincipalKind.USER, frozen=True)
    """Principal kind."""


# ....................... #


class IamPermission(Document, NameMixin, IsActiveMixin):
    """IAM permission model."""

    resource: str | None = None
    """Resource name."""

    action: str | None = None
    """Action name."""


class CreateIamPermissionCmd(CreateDocumentCmd, NameMixin):
    """Create IAM permission command."""

    resource: str | None = None
    """Resource name."""

    action: str | None = None
    """Action name."""

    is_active: bool = True
    """Whether the permission is active."""


class UpdateIamPermissionCmd(BaseDTO):
    """Update IAM permission command."""

    name: str | None = None
    """Permission name."""

    resource: str | None = None
    """Resource name."""

    action: str | None = None
    """Action name."""

    is_active: bool | None = None
    """Whether the permission is active."""


class ReadIamPermission(ReadDocument, NameMixin, IsActiveMixin):
    """Read IAM permission model."""

    resource: str | None = None
    """Resource name."""

    action: str | None = None
    """Action name."""


# ....................... #


class IamRole(Document, NameMixin, IsActiveMixin, TenantIdMixin):
    """IAM role model."""


class CreateIamRoleCmd(CreateDocumentCmd, NameMixin, TenantIdMixin):
    """Create IAM role command."""

    is_active: bool = True
    """Whether the role is active."""


class UpdateIamRoleCmd(BaseDTO):
    """Update IAM role command."""

    name: str | None = None
    """Role name."""

    is_active: bool | None = None
    """Whether the role is active."""


class ReadIamRole(ReadDocument, NameMixin, IsActiveMixin, TenantIdMixin):
    """Read IAM role model."""


# ....................... #


class IamGroup(Document, NameMixin, IsActiveMixin, TenantIdMixin):
    """IAM group model."""


class CreateIamGroupCmd(CreateDocumentCmd, NameMixin, TenantIdMixin):
    """Create IAM group command."""

    is_active: bool = True
    """Whether the group is active."""


class UpdateIamGroupCmd(BaseDTO):
    """Update IAM group command."""

    name: str | None = None
    """Group name."""

    is_active: bool | None = None
    """Whether the group is active."""


class ReadIamGroup(ReadDocument, NameMixin, IsActiveMixin, TenantIdMixin):
    """Read IAM group model."""


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


class CreateIamPrincipalRoleCmd(
    CreateDocumentCmd,
    TenantIdMixin,
    IamPrincipalRoleReferences,
):
    """Create principal-to-role assignment command."""


class UpdateIamPrincipalRoleCmd(BaseDTO):
    """Update principal-to-role assignment command."""


class ReadIamPrincipalRole(
    ReadDocument,
    TenantIdMixin,
    IamPrincipalRoleReferences,
):
    """Read principal-to-role assignment model."""


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


class CreateIamPrincipalPermissionCmd(
    CreateDocumentCmd,
    TenantIdMixin,
    IamPrincipalPermissionReferences,
):
    """Create principal-to-permission assignment command."""


class UpdateIamPrincipalPermissionCmd(BaseDTO):
    """Update principal-to-permission assignment command."""


class ReadIamPrincipalPermission(
    ReadDocument,
    TenantIdMixin,
    IamPrincipalPermissionReferences,
):
    """Read principal-to-permission assignment model."""


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


class CreateIamPrincipalGroupCmd(
    CreateDocumentCmd,
    TenantIdMixin,
    IamPrincipalGroupReferences,
):
    """Create principal-to-group assignment command."""


class UpdateIamPrincipalGroupCmd(BaseDTO):
    """Update principal-to-group assignment command."""


class ReadIamPrincipalGroup(
    ReadDocument,
    TenantIdMixin,
    IamPrincipalGroupReferences,
):
    """Read principal-to-group assignment model."""


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


class CreateIamGroupRoleCmd(
    CreateDocumentCmd,
    TenantIdMixin,
    IamGroupRoleReferences,
):
    """Create group-to-role assignment command."""


class UpdateIamGroupRoleCmd(BaseDTO):
    """Update group-to-role assignment command."""


class ReadIamGroupRole(
    ReadDocument,
    TenantIdMixin,
    IamGroupRoleReferences,
):
    """Read group-to-role assignment model."""


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


class CreateIamRolePermissionCmd(
    CreateDocumentCmd,
    TenantIdMixin,
    IamRolePermissionReferences,
):
    """Create role-to-permission assignment command."""


class UpdateIamRolePermissionCmd(BaseDTO):
    """Update role-to-permission assignment command."""


class ReadIamRolePermission(
    ReadDocument,
    TenantIdMixin,
    IamRolePermissionReferences,
):
    """Read role-to-permission assignment model."""
