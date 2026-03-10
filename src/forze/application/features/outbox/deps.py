from typing import Any

from forze.application.contracts.deps import DepKey
from forze.application.execution import ExecutionContext
from forze.base.primitives import ContextualBuffer

from .effects import FlushOutboxEffect
from .middlewares import OutboxBufferMiddleware
from .model import CreateOutboxEventCmd
from .service import OutboxService
from .spec import OutboxSpec

# ----------------------- #

OutboxServiceDepKey = DepKey[OutboxService]("outbox_service")
"""Key used to register the :class:`OutboxService` implementation."""

OutboxBufferDepKey = DepKey[ContextualBuffer[CreateOutboxEventCmd]]("outbox_buffer")
"""Key used to register the :class:`ContextualBuffer` implementation related to the outbox."""

OutboxBuffer = ContextualBuffer[CreateOutboxEventCmd]()
"""Singleton instance of the :class:`ContextualBuffer` for the outbox."""

# ....................... #
#! TODO: review factories below


def build_outbox_service(ctx: ExecutionContext, spec: OutboxSpec) -> OutboxService:
    """Build an :class:`OutboxService` from the execution context and outbox spec."""

    d = ctx.doc_write(spec)

    return OutboxService(doc=d)


# ....................... #


def build_outbox_buffer_middleware(
    ctx: ExecutionContext,
) -> OutboxBufferMiddleware[Any, Any]:
    """Build an :class:`OutboxBufferMiddleware` from the execution context."""

    buf = ctx.dep(OutboxBufferDepKey)

    return OutboxBufferMiddleware(buf=buf)


# ....................... #


def build_flush_outbox_effect(ctx: ExecutionContext) -> FlushOutboxEffect[Any, Any]:
    """Build a :class:`FlushOutboxEffect` from the execution context."""

    buf = ctx.dep(OutboxBufferDepKey)
    outbox = ctx.dep(OutboxServiceDepKey)

    return FlushOutboxEffect(buf=buf, outbox=outbox)
