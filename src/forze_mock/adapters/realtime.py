"""In-memory recording :class:`RealtimePort` for tests, examples, and DST.

Records every emit so a test can assert *what* was pushed to *which logical
audience* without a running Socket.IO server. It records the logical
:class:`Audience` exactly as the handler expressed it — tenant scoping is a
transport-adapter concern, verified there, so the recorder stays tenant-agnostic
like the handlers it observes.
"""

from typing import final

import attrs
from pydantic import BaseModel

from forze.application.contracts.realtime import Audience, RealtimePort

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class RecordedEmit:
    """A single recorded :meth:`RecordingRealtimePort.emit` call."""

    audience: Audience
    """Logical target of the emit."""

    event: str
    """Client-facing event name."""

    payload: BaseModel
    """Event body passed by the caller (unserialized)."""


# ....................... #


@final
@attrs.define(slots=True)
class RecordingRealtimePort(RealtimePort):
    """Test double: a :class:`RealtimePort` that records instead of delivering.

    Every emit is appended to :attr:`emits` in call order.
    """

    emits: list[RecordedEmit] = attrs.field(factory=list, init=False)
    """All recorded emits, in call order."""

    # ....................... #

    async def emit(
        self,
        audience: Audience,
        event: str,
        payload: BaseModel,
    ) -> None:
        self.emits.append(
            RecordedEmit(audience=audience, event=event, payload=payload)
        )

    # ....................... #

    def events_for(self, audience: Audience) -> list[RecordedEmit]:
        """Return recorded emits targeting *audience*, in call order."""

        return [e for e in self.emits if e.audience == audience]

    # ....................... #

    def clear(self) -> None:
        """Forget every recorded emit."""

        self.emits.clear()
