import attrs
from pydantic import BaseModel

from forze.base.serialization import pydantic_dump, pydantic_validate
from forze.domain.constants import NUMBER_ID_FIELD
from forze.domain.models import BaseDTO

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class DTOMapper[Out: BaseDTO]:
    dto: type[Out]

    # ....................... #

    def __call__(self, x: BaseModel) -> Out:
        data = pydantic_dump(x, exclude={"unset": True})

        return pydantic_validate(self.dto, data, forbid_extra=True)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class NumberedDTOMapper[Out: BaseDTO]:
    dto: type[Out]

    # ....................... #

    def __call__(self, x: BaseModel, number_id: int) -> Out:
        data = pydantic_dump(x, exclude={"unset": True})
        data[NUMBER_ID_FIELD] = number_id

        return pydantic_validate(self.dto, data, forbid_extra=True)
