import attrs

from forze.application.execution import Middleware, NextCall
from forze.base.primitives.buffer import ContextualBuffer

from .model import CreateOutboxEventCmd

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class OutboxBufferMiddleware[Args, R](Middleware[Args, R]):
    """Middleware that scopes a :class:`ContextualBuffer` for outbox event drafts.

    Opens a buffer scope before the inner call and closes it on exit, so
    outbox events accumulated during the request are isolated per-call.
    """

    buf: ContextualBuffer[CreateOutboxEventCmd]
    """Buffer scoped by this middleware."""

    # ....................... #

    async def __call__(self, next: NextCall[Args, R], args: Args) -> R:
        with self.buf.scope():
            return await next(args)
