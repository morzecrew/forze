from typing import final

import attrs

from forze.application.execution import Effect
from forze.base.primitives import ContextualBuffer

from .model import CreateOutboxEventCmd
from .service import OutboxService

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class FlushOutboxEffect[Args, R](Effect[Args, R]):
    """After-commit effect that flushes buffered outbox events to storage."""

    buf: ContextualBuffer[CreateOutboxEventCmd]
    """Buffer holding outbox event drafts accumulated during the request."""

    outbox: OutboxService
    """Service used to persist the buffered events."""

    # ....................... #

    async def __call__(self, args: Args, res: R) -> R:
        drafts = self.buf.pop()

        if drafts:
            await self.outbox.append_many(drafts)

        return res
