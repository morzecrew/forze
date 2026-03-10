from typing import final

import attrs
from pydantic import BaseModel

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class StreamSpec[M: BaseModel]:
    """Specification binding a stream namespace to its message model type."""

    namespace: str
    """Logical stream namespace (used as a routing/naming prefix)."""

    model: type[M]
    """Pydantic model class for messages in this stream."""
