"""Request/response DTOs for tenancy admin (create tenant, manage members)."""

from uuid import UUID

from pydantic import Field

from forze.domain.models import BaseDTO

# ----------------------- #


class CreateTenantRequestDTO(BaseDTO):
    """Request to provision a new tenant."""

    tenant_key: str | None = None
    """Optional human-facing key; the adapter assigns one when omitted."""


# ....................... #


class CreatedTenantDTO(BaseDTO):
    """The tenant that was just provisioned."""

    tenant_id: UUID
    """Identifier of the new tenant."""

    tenant_key: str | None = None
    """Human-facing tenant key, when known."""


# ....................... #


class TenantRefDTO(BaseDTO):
    """A tenant referenced by id (path-bound: list members / deactivate)."""

    id: UUID
    """Tenant id to act on."""


# ....................... #


class MembershipDTO(BaseDTO):
    """A (tenant, principal) membership pair (invite / remove)."""

    tenant_id: UUID
    """Tenant the membership belongs to."""

    principal_id: UUID
    """Principal being granted or revoked."""


# ....................... #


class MemberListItemDTO(BaseDTO):
    """One member of a tenant."""

    principal_id: UUID
    """Principal id of the member (join with identity-plane details out of band)."""


# ....................... #


class MemberListDTO(BaseDTO):
    """The principals that belong to a tenant."""

    members: list[MemberListItemDTO] = Field(default_factory=list)
