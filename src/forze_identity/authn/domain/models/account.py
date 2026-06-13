from datetime import datetime
from uuid import UUID

from pydantic import EmailStr, Field

from forze.domain.models import (
    BaseDTO,
    CoreModel,
    CreateDocumentCmd,
    Document,
    ReadDocument,
)

from ..mixins import IsActiveMixin

# ----------------------- #


class PasswordAccountImmutableFields(CoreModel):
    """Immutable fields for password-based authentication account."""

    principal_id: UUID = Field(frozen=True)
    """Principal ID."""

    username: str = Field(frozen=True, min_length=2, max_length=4096)
    """Username."""


# ....................... #


class PasswordAccountMutableFields(CoreModel):
    """Mutable fields for password-based authentication account."""

    email: EmailStr | None = None
    """Email address."""


# ....................... #


class PasswordAccount(
    Document,
    PasswordAccountImmutableFields,
    PasswordAccountMutableFields,
    IsActiveMixin,
):
    """Password-based authentication account model."""

    password_hash: str
    """Hashed password."""


# ....................... #


class CreatePasswordAccountCmd(
    CreateDocumentCmd,
    PasswordAccountImmutableFields,
    PasswordAccountMutableFields,
):
    """Create password-based authentication account command."""

    password_hash: str
    """Hashed password."""


# ....................... #


class UpdatePasswordAccountCmd(BaseDTO, PasswordAccountMutableFields):
    """Update password-based authentication account command."""

    password_hash: str | None = None
    """Hashed password."""

    is_active: bool | None = None
    """Whether the password account is active."""


# ....................... #


class ReadPasswordAccount(
    ReadDocument,
    PasswordAccountImmutableFields,
    PasswordAccountMutableFields,
    IsActiveMixin,
):
    """Read password-based authentication account model."""

    password_hash: str
    """Hashed password."""


# ....................... #


class ApiKeyAccountImmutableFields(CoreModel):
    """Immutable fields for API key-based authentication account."""

    principal_id: UUID = Field(frozen=True)
    """Principal ID (the effective subject — the user the key acts for)."""

    actor_principal_id: UUID | None = Field(default=None, frozen=True)
    """Optional delegation **actor** (the agent acting on the subject's behalf).

    When set, the key is a user→agent delegation: verification attaches this principal
    as :attr:`~forze.application.contracts.authn.AuthnIdentity.actor`, so the engine
    enforces the least-privilege intersection of the subject's and agent's grants. The
    same agent principal (e.g. one per connector type) can back many keys, while each
    key stays independently revocable. ``None`` is a direct (non-delegated) key."""

    prefix: str | None = Field(default=None, frozen=True)
    """Prefix."""

    expires_at: datetime | None = Field(default=None, frozen=True)
    """Absolute expiration time; ``None`` means the key does not expire."""


# ....................... #


class ApiKeyAccount(
    Document,
    ApiKeyAccountImmutableFields,
    IsActiveMixin,
):
    """API key-based authentication account model."""

    key_hash: str
    """Hashed API key."""


# ....................... #


class CreateApiKeyAccountCmd(CreateDocumentCmd, ApiKeyAccountImmutableFields):
    """Create API key-based authentication account command."""

    key_hash: str
    """Hashed API key."""


# ....................... #


class UpdateApiKeyAccountCmd(BaseDTO):
    """Update API key-based authentication account command."""

    is_active: bool | None = None
    """Whether the API key account is active."""


# ....................... #


class ReadApiKeyAccount(ReadDocument, ApiKeyAccountImmutableFields, IsActiveMixin):
    """Read API key-based authentication account model."""

    key_hash: str
    """Hashed API key."""
