from pydantic import Field

from forze.domain.models import (
    BaseDTO,
    CoreModel,
    CreateDocumentCmd,
    Document,
    ReadDocument,
)

from ..constants import PrincipalKind
from ..mixins import IsActiveMixin

# ----------------------- #


class PolicyPrincipalImmutableFields(CoreModel):
    """Immutable fields for a policy principal document."""

    kind: PrincipalKind = Field(frozen=True)
    """Rough class of actor."""


# ....................... #


class PolicyPrincipal(Document, PolicyPrincipalImmutableFields, IsActiveMixin):
    """Policy principal aggregate: identity anchor for bindings (roles and permissions are edges)."""


# ....................... #


class CreatePolicyPrincipalCmd(CreateDocumentCmd, PolicyPrincipalImmutableFields):
    """Create policy principal command."""


# ....................... #


class UpdatePolicyPrincipalCmd(BaseDTO):
    """Partial update for policy principal."""

    is_active: bool | None = None
    """Whether the principal is active."""


# ....................... #


class ReadPolicyPrincipal(ReadDocument, PolicyPrincipalImmutableFields, IsActiveMixin):
    """Read model for policy principal."""
