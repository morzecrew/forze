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
    buf: ContextualBuffer[CreateOutboxEventCmd]
    outbox: OutboxService

    # ....................... #

    async def __call__(self, args: Args, res: R) -> R:
        drafts = self.buf.pop()

        if drafts:
            await self.outbox.append_many(drafts)

        return res
