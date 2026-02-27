import attrs

from forze.application.execution import Middleware, NextCall
from forze.base.primitives.buffer import ContextualBuffer

from .model import CreateOutboxEventCmd

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class OutboxBufferMiddleware[Args, R](Middleware[Args, R]):
    buf: ContextualBuffer[CreateOutboxEventCmd]

    # ....................... #

    async def __call__(self, next: NextCall[Args, R], args: Args) -> R:
        with self.buf.scope():
            return await next(args)
