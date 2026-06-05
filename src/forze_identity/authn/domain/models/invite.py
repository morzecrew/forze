from datetime import datetime
from uuid import UUID

from pydantic import Field

from forze.domain.models import (
    BaseDTO,
    CoreModel,
    CreateDocumentCmd,
    Document,
    ReadDocument,
)

# ----------------------- #


class PasswordInviteImmutableFields(CoreModel):
    """Immutable fields for a single-use password provisioning invite."""

    principal_id: UUID = Field(frozen=True)
    """Principal the invite provisions a password account for once accepted."""

    token_digest: str = Field(frozen=True)
    """Invite token HMAC digest; the raw token is never persisted."""

    expires_at: datetime = Field(frozen=True)
    """Absolute expiration time."""


# ....................... #


class PasswordInviteMutableFields(CoreModel):
    """Mutable fields for a single-use password provisioning invite."""

    consumed_at: datetime | None = None
    """When the invite was accepted; ``None`` means still pending."""


# ....................... #


class PasswordInvite(
    Document,
    PasswordInviteImmutableFields,
    PasswordInviteMutableFields,
):
    """Single-use password provisioning invite model."""


# ....................... #


class CreatePasswordInviteCmd(
    CreateDocumentCmd,
    PasswordInviteImmutableFields,
):
    """Create password provisioning invite command."""


# ....................... #


class UpdatePasswordInviteCmd(
    BaseDTO,
    PasswordInviteMutableFields,
):
    """Update password provisioning invite command."""


# ....................... #


class ReadPasswordInvite(
    ReadDocument,
    PasswordInviteImmutableFields,
    PasswordInviteMutableFields,
):
    """Read password provisioning invite model."""
