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


class PasswordResetImmutableFields(CoreModel):
    """Immutable fields for a single-use self-service password reset."""

    principal_id: UUID = Field(frozen=True)
    """Principal whose password the reset re-keys once confirmed."""

    token_digest: str = Field(frozen=True)
    """Reset token HMAC digest; the raw token is never persisted."""

    expires_at: datetime = Field(frozen=True)
    """Absolute expiration time."""


# ....................... #


class PasswordResetMutableFields(CoreModel):
    """Mutable fields for a single-use self-service password reset."""

    used_at: datetime | None = None
    """When the reset stopped being usable — consumed by a successful
    ``reset_password`` or superseded by a newer reset for the same principal;
    ``None`` means still outstanding."""


# ....................... #


class PasswordReset(
    Document,
    PasswordResetImmutableFields,
    PasswordResetMutableFields,
):
    """Single-use self-service password reset model."""


# ....................... #


class CreatePasswordResetCmd(
    CreateDocumentCmd,
    PasswordResetImmutableFields,
):
    """Create password reset command."""


# ....................... #


class UpdatePasswordResetCmd(
    BaseDTO,
    PasswordResetMutableFields,
):
    """Update password reset command."""


# ....................... #


class ReadPasswordReset(
    ReadDocument,
    PasswordResetImmutableFields,
    PasswordResetMutableFields,
):
    """Read password reset model."""
