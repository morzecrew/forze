from typing import final

import attrs

from ..base import MessageCodecSpec

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class QueueSpec[M](MessageCodecSpec[M]):
    """Specification binding a queue namespace to its payload record codec."""
