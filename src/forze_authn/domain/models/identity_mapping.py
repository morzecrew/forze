from uuid import UUID

from pydantic import Field

from forze.base.primitives import String
from forze.domain.models import (
    BaseDTO,
    CoreModel,
    CreateDocumentCmd,
    Document,
    ReadDocument,
)

# ----------------------- #


class IdentityMappingImmutableFields(CoreModel):
    """Immutable fields for an external-identity mapping row."""

    issuer: String = Field(frozen=True)
    """Stable identifier of the authority that issued the assertion (e.g. ``iss`` URL)."""

    subject: String = Field(frozen=True)
    """Raw external subject identifier as provided by the issuer."""

    principal_id: UUID = Field(frozen=True)
    """Internal Forze principal id this mapping resolves to."""


# ....................... #


class IdentityMapping(Document, IdentityMappingImmutableFields):
    """Mapping between an external (issuer, subject) tuple and an internal principal."""


# ....................... #


class CreateIdentityMappingCmd(CreateDocumentCmd, IdentityMappingImmutableFields):
    """Create an external-identity mapping row."""


# ....................... #


class UpdateIdentityMappingCmd(BaseDTO):
    """Update command (no mutable fields today; reserved for future flags)."""


# ....................... #


class ReadIdentityMapping(ReadDocument, IdentityMappingImmutableFields):
    """Read view of an external-identity mapping row."""
