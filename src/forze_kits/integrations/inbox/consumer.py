"""Consumer-side dedup: process a message exactly-once via the inbox."""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from forze.application.contracts.inbox import InboxSpec
from forze.application.execution.context import ExecutionContext
from forze.base.exceptions import exc
from forze.base.primitives import StrKey

# ----------------------- #


async def process_with_inbox[M](
    ctx: ExecutionContext,
    message: M,
    *,
    inbox_spec: InboxSpec,
    handler: Callable[[M], Awaitable[None]],
    tx_route: StrKey,
    message_id: Callable[[M], str] | None = None,
) -> bool:
    """Process *message* exactly-once, deduping on a message id.

    Opens a transaction on *tx_route*, marks the message processed via the inbox,
    and runs *handler* in the **same transaction** — so the dedup mark and the
    handler's writes commit atomically. A redelivered message (already marked in a
    prior committed transaction) is skipped.

    The dedup id defaults to ``message.key or message.id`` (outbox relay sets
    ``key`` to the integration ``event_id``); pass *message_id* to override.

    :returns: ``True`` if the message was processed, ``False`` if skipped as a duplicate.
    """

    if message_id is not None:
        dedup_id: str | None = message_id(message)

    else:
        dedup_id = getattr(message, "key", None) or getattr(message, "id", None)

    if not dedup_id:
        raise exc.precondition(
            "Cannot deduplicate message: no key or id; pass a message_id extractor",
        )

    async with ctx.tx_ctx.scope(tx_route):
        port = ctx.inbox(inbox_spec)

        if not await port.mark_if_unseen(str(inbox_spec.name), dedup_id):
            return False

        await handler(message)
        return True
