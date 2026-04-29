from uuid import UUID

from pydantic import EmailStr, Field

from forze.base.primitives import String
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

    username: String = Field(frozen=True)
    """Username."""


class PasswordAccountMutableFields(CoreModel):
    """Mutable fields for password-based authentication account."""

    email: EmailStr | None = None
    """Email address."""


class PasswordAccount(
    Document,
    PasswordAccountImmutableFields,
    PasswordAccountMutableFields,
    IsActiveMixin,
):
    """Password-based authentication account model."""

    password_hash: str
    """Hashed password."""


class CreatePasswordAccountCmd(
    CreateDocumentCmd,
    PasswordAccountImmutableFields,
    PasswordAccountMutableFields,
):
    """Create password-based authentication account command."""

    password_hash: str
    """Hashed password."""


class UpdatePasswordAccountCmd(BaseDTO, PasswordAccountMutableFields):
    """Update password-based authentication account command."""

    password_hash: str | None = None
    """Hashed password."""


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
    """Principal ID."""

    prefix: str | None = Field(default=None, frozen=True)
    """Prefix."""


class ApiKeyAccount(
    Document,
    ApiKeyAccountImmutableFields,
    IsActiveMixin,
):
    """API key-based authentication account model."""

    key_hash: str
    """Hashed API key."""


class CreateApiKeyAccountCmd(CreateDocumentCmd, ApiKeyAccountImmutableFields):
    """Create API key-based authentication account command."""

    key_hash: str
    """Hashed API key."""


class UpdateApiKeyAccountCmd(BaseDTO):
    """Update API key-based authentication account command."""

    is_active: bool | None = None
    """Whether the API key account is active."""


class ReadApiKeyAccount(ReadDocument, ApiKeyAccountImmutableFields, IsActiveMixin):
    """Read API key-based authentication account model."""

    key_hash: str
    """Hashed API key."""
