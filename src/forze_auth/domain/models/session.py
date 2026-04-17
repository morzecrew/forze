from datetime import datetime
from uuid import UUID

from pydantic import Field, IPvAnyAddress

from forze.base.primitives import uuid4
from forze.domain.models import (
    BaseDTO,
    CoreModel,
    CreateDocumentCmd,
    Document,
    ReadDocument,
)

# ----------------------- #


class RefreshGrantImmutableFields(CoreModel):
    """Immutable fields for refresh grant."""

    principal_id: UUID = Field(frozen=True)
    """Owning principal identifier."""

    account_id: UUID | None = Field(default=None, frozen=True)
    """Optional authentication account that issued the grant."""

    family_id: UUID = Field(default_factory=uuid4, frozen=True)
    """Family ID."""

    refresh_hash: bytes = Field(frozen=True)
    """Refresh hash."""

    expires_at: datetime = Field(frozen=True)
    """Expiration timestamp."""

    user_agent: str | None = Field(default=None, frozen=True)
    """User agent."""

    ip: IPvAnyAddress | None = Field(default=None, frozen=True)
    """IP address."""


class RefreshGrantMutableFields(CoreModel):
    """Mutable fields for authentication session."""

    revoked_at: datetime | None = None
    """Revocation timestamp."""

    rotated_at: datetime | None = None
    """Rotation timestamp."""

    replaced_by: UUID | None = None
    """Replaced by session ID."""


class RefreshGrant(
    Document,
    RefreshGrantImmutableFields,
    RefreshGrantMutableFields,
):
    """Refresh grant model."""


class CreateRefreshGrantCmd(CreateDocumentCmd, RefreshGrantImmutableFields):
    """Create refresh grant command."""


class UpdateRefreshGrantCmd(BaseDTO, RefreshGrantMutableFields):
    """Update refresh grant command."""


class ReadRefreshGrant(
    ReadDocument,
    RefreshGrantImmutableFields,
    RefreshGrantMutableFields,
):
    """Read refresh grant model."""
