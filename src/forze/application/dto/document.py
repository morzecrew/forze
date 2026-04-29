from uuid import UUID

from pydantic import BaseModel, PositiveInt

from forze.base.primitives import JsonDict
from forze.domain.models import BaseDTO

# ----------------------- #


class DocumentIdDTO(BaseDTO):
    """DTO for the document ID."""

    id: UUID
    """Document primary key."""


# ....................... #


class DocumentIdRevDTO(DocumentIdDTO):
    """DTO for the document ID and revision."""

    rev: int
    """Expected revision for optimistic concurrency."""


# ....................... #


class DocumentUpdateDTO[In: BaseDTO](DocumentIdRevDTO):
    """DTO for the document update."""

    dto: In
    """Update payload DTO."""


# ....................... #


class DocumentNumberIdDTO(BaseDTO):
    """DTO for the document number ID."""

    number_id: PositiveInt
    """Document number ID."""


# ....................... #


class DocumentUpdateRes[Out: BaseModel](BaseDTO):
    """DTO for the document update response."""

    data: Out
    """Updated read model."""

    diff: JsonDict
    """Diff of the update."""
