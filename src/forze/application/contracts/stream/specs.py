from typing import final

import attrs

from ..base import MessageCodecSpec

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class StreamSpec[M](MessageCodecSpec[M]):
    """Specification binding a stream namespace to its payload record codec."""
