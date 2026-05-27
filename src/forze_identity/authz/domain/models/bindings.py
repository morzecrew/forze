"""Junction documents for authorization bindings."""

from uuid import UUID

from pydantic import Field

from forze.domain.models import CreateDocumentCmd, Document, ReadDocument

# ----------------------- #
# Role ↔ Permission


class RolePermissionBinding(Document):
    """Maps a role catalog entry to a permission catalog entry."""

    role_id: UUID = Field(frozen=True)
    """Role definition document id."""

    permission_id: UUID = Field(frozen=True)
    """Permission definition document id."""


class CreateRolePermissionBindingCmd(CreateDocumentCmd):
    """Create role-permission binding."""

    role_id: UUID = Field(frozen=True)
    permission_id: UUID = Field(frozen=True)


class ReadRolePermissionBinding(ReadDocument):
    """Read model for role-permission binding."""

    role_id: UUID
    permission_id: UUID


# ....................... #
# Principal ↔ Role


class PrincipalRoleBinding(Document):
    """Assigns a role to a policy principal."""

    principal_id: UUID = Field(frozen=True)
    """Policy principal document id."""

    role_id: UUID = Field(frozen=True)
    """Role definition document id."""


class CreatePrincipalRoleBindingCmd(CreateDocumentCmd):
    """Create principal-role binding."""

    principal_id: UUID = Field(frozen=True)
    role_id: UUID = Field(frozen=True)


class ReadPrincipalRoleBinding(ReadDocument):
    """Read model for principal-role binding."""

    principal_id: UUID
    role_id: UUID


# ....................... #
# Principal ↔ Permission


class PrincipalPermissionBinding(Document):
    """Direct permission grant on a principal."""

    principal_id: UUID = Field(frozen=True)
    """Policy principal document id."""

    permission_id: UUID = Field(frozen=True)
    """Permission definition document id."""


class CreatePrincipalPermissionBindingCmd(CreateDocumentCmd):
    """Create principal-permission binding."""

    principal_id: UUID = Field(frozen=True)
    permission_id: UUID = Field(frozen=True)


class ReadPrincipalPermissionBinding(ReadDocument):
    """Read model for principal-permission binding."""

    principal_id: UUID
    permission_id: UUID


# ....................... #
# Group ↔ Principal (membership)


class GroupPrincipalBinding(Document):
    """Membership of a principal in a group."""

    group_id: UUID = Field(frozen=True)
    """Group document id."""

    principal_id: UUID = Field(frozen=True)
    """Policy principal document id."""


class CreateGroupPrincipalBindingCmd(CreateDocumentCmd):
    """Create group membership."""

    group_id: UUID = Field(frozen=True)
    principal_id: UUID = Field(frozen=True)


class ReadGroupPrincipalBinding(ReadDocument):
    """Read model for group membership."""

    group_id: UUID
    principal_id: UUID


# ....................... #
# Group ↔ Role


class GroupRoleBinding(Document):
    """Grants a role to all members of a group."""

    group_id: UUID = Field(frozen=True)
    """Group document id."""

    role_id: UUID = Field(frozen=True)
    """Role definition document id."""


class CreateGroupRoleBindingCmd(CreateDocumentCmd):
    """Create group-role binding."""

    group_id: UUID = Field(frozen=True)
    role_id: UUID = Field(frozen=True)


class ReadGroupRoleBinding(ReadDocument):
    """Read model for group-role binding."""

    group_id: UUID
    role_id: UUID


# ....................... #
# Group ↔ Permission


class GroupPermissionBinding(Document):
    """Direct permission granted via group membership."""

    group_id: UUID = Field(frozen=True)
    """Group document id."""

    permission_id: UUID = Field(frozen=True)
    """Permission definition document id."""


class CreateGroupPermissionBindingCmd(CreateDocumentCmd):
    """Create group-permission binding."""

    group_id: UUID = Field(frozen=True)
    permission_id: UUID = Field(frozen=True)


class ReadGroupPermissionBinding(ReadDocument):
    """Read model for group-permission binding."""

    group_id: UUID
    permission_id: UUID
