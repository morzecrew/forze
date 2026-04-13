from uuid import UUID

from pydantic import Field

from ..models import BaseDTO, CoreModel

# ----------------------- #


class CreatorMixin(CoreModel):
    """Mixin that adds an immutable ``creator_id`` field to a document model."""

    creator_id: UUID = Field(frozen=True)


# ....................... #


class CreatorCreateCmdMixin(BaseDTO):
    """Mixin that adds a ``creator_id`` field to a create command DTO."""

    creator_id: UUID
