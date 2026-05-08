from datetime import datetime
from uuid import UUID

from pydantic import Field

from forze.base.primitives import uuid4
from forze.domain.models import (
    BaseDTO,
    CoreModel,
    CreateDocumentCmd,
    Document,
    ReadDocument,
)

# ----------------------- #


class SessionImmutableFields(CoreModel):
    """Immutable fields for token-based authentication session."""

    principal_id: UUID = Field(frozen=True)
    """Owning principal identifier."""

    family_id: UUID = Field(default_factory=uuid4, frozen=True)
    """Family ID."""

    refresh_digest: bytes = Field(frozen=True)
    """Refresh hash digest."""

    expires_at: datetime = Field(frozen=True)
    """Expiration timestamp."""


# ....................... #


class SessionMutableFields(CoreModel):
    """Mutable fields for token-based authentication session."""

    revoked_at: datetime | None = None
    """Revocation timestamp."""

    rotated_at: datetime | None = None
    """Rotation timestamp."""

    replaced_by: UUID | None = None
    """Replaced by session ID."""


# ....................... #


class Session(
    Document,
    SessionImmutableFields,
    SessionMutableFields,
):
    """Token-based authentication session model."""


# ....................... #


class CreateSessionCmd(
    CreateDocumentCmd,
    SessionImmutableFields,
):
    """Create token-based authentication session command."""


# ....................... #


class UpdateSessionCmd(
    BaseDTO,
    SessionMutableFields,
):
    """Update token-based authentication session command."""


# ....................... #


class ReadSession(
    ReadDocument,
    SessionImmutableFields,
    SessionMutableFields,
):
    """Read token-based authentication session model."""
