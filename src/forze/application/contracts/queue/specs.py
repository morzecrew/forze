from typing import final

import attrs
from pydantic import BaseModel

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class QueueSpec[M: BaseModel]:
    namespace: str
    model: type[M]
