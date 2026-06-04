from typing import final

import attrs

from forze.application.integrations.queue import QueueMessageCodec

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class SQSQueueCodec[M](QueueMessageCodec[M]):
    """SQS queue payload codec backed by a record-mapping codec."""
