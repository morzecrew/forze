from typing import final

import attrs

from ..base import MessageCodecSpec

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class StreamSpec[M](MessageCodecSpec[M]):
    """Specification binding a stream namespace to its payload record codec."""

    requires_transactions: bool = False
    """Demand native transport-level exactly-once (offset-log / commit sub-model).

    Fail-closed: when set, resolving a :class:`CommitStreamGroupQueryPort` whose
    backend does not report ``supports_transactions`` is rejected at resolve, so
    a transaction-dependent consumer is never silently wired onto a backend that
    only offers at-least-once + inbox dedup. Default ``False`` (the portable
    exactly-once-*effect* path). Ignored by the ack sub-model."""
