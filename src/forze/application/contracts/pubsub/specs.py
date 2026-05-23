from typing import Any, final

import attrs

from forze.base.serialization import RecordMappingCodec

from ..base import BaseSpec

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PubSubSpec[M](BaseSpec):
    """Specification binding a pubsub namespace to its payload record codec."""

    codec: RecordMappingCodec[M, Any]
    """Payload record codec for messages in this pubsub channel."""

    # ....................... #

    @property
    def model_type(self) -> type[M]:
        """Payload model type carried by :attr:`codec`."""

        return self.codec.model_type
