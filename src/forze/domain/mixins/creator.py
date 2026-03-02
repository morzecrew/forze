from uuid import UUID

from pydantic import Field

from ..models import BaseDTO, CoreModel

# ----------------------- #


class CreatorMixin(CoreModel):
    creator_id: UUID = Field(frozen=True)


# ....................... #


class CreatorCreateCmdMixin(BaseDTO):
    creator_id: UUID
