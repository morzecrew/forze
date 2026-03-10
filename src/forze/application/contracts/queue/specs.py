from typing import final

import attrs
from pydantic import BaseModel

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class QueueSpec[M: BaseModel]:
    """Specification binding a queue namespace to its message model type."""

    namespace: str
    """Logical queue namespace (used as a routing/naming prefix)."""

    model: type[M]
    """Pydantic model class for messages in this queue."""
