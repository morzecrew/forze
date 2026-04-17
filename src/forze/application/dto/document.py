from uuid import UUID

from pydantic import PositiveInt

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
