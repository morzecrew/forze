from typing import final

import attrs
from pydantic import BaseModel

from ..base import BaseSpec

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class StreamSpec[M: BaseModel](BaseSpec):
    """Specification binding a stream namespace to its message model type."""

    model: type[M]
    """Pydantic model class for messages in this stream."""
