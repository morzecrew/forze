from typing import Any, final

import attrs

from forze.base.serialization import ModelCodec

from ..base import BaseSpec

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class StreamSpec[M](BaseSpec):
    """Specification binding a stream namespace to its payload record codec."""

    codec: ModelCodec[M, Any]
    """Payload record codec for messages in this stream."""

    # ....................... #

    @property
    def model_type(self) -> type[M]:
        """Payload model type carried by :attr:`codec`."""

        return self.codec.model_type
