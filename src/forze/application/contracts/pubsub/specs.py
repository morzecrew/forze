from typing import final

import attrs
from pydantic import BaseModel

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PubSubSpec[M: BaseModel]:
    """Specification binding a pubsub namespace to its message model type."""

    namespace: str
    """Logical pubsub namespace (used as a topic prefix)."""

    model: type[M]
    """Pydantic model class for messages in this pubsub channel."""
