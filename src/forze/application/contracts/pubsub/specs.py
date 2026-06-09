from typing import final

import attrs

from ..base import MessageCodecSpec

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PubSubSpec[M](MessageCodecSpec[M]):
    """Specification binding a pubsub namespace to its payload record codec."""
