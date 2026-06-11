"""Consumer-side dedup: process a message exactly-once via the inbox."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from contextlib import ExitStack
from uuid import UUID

from forze.application.contracts.envelope import (
    HEADER_CORRELATION_ID,
    HEADER_EVENT_ID,
    HEADER_TENANT_ID,
)
from forze.application.contracts.inbox import InboxSpec
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution.context import ExecutionContext
from forze.application.execution.context.invocation import InvocationMetadata
from forze.base.exceptions import exc
from forze.base.primitives import StrKey, uuid7

# ----------------------- #


def _parse_uuid(value: object) -> UUID | None:
    """Best-effort UUID parse: malformed header values are ignored."""

    if not isinstance(value, str) or not value:
        return None

    try:
        return UUID(value)
    except ValueError:
        return None


# ....................... #


def _message_headers(message: object) -> Mapping[str, str]:
    """Return the message's transport headers, tolerating header-less types."""

    headers = getattr(message, "headers", None)

    if isinstance(headers, Mapping):
        return headers  # pyright: ignore[reportUnknownVariableType]

    return {}


# ....................... #


async def process_with_inbox[M](
    ctx: ExecutionContext,
    message: M,
    *,
    inbox_spec: InboxSpec,
    handler: Callable[[M], Awaitable[None]],
    tx_route: StrKey,
    message_id: Callable[[M], str] | None = None,
    bind_tenant_from_headers: bool = False,
) -> bool:
    """Process *message* exactly-once, deduping on a message id.

    Opens a transaction on *tx_route*, marks the message processed via the inbox,
    and runs *handler* in the **same transaction** — so the dedup mark and the
    handler's writes commit atomically. A redelivered message (already marked in a
    prior committed transaction) is skipped.

    The dedup id defaults to ``message.key or message.id`` (outbox relay sets
    ``key`` to the integration ``event_id``); pass *message_id* to override.

    **Envelope rebinding** — when the message carries the well-known envelope
    headers (see :mod:`forze.application.contracts.envelope`, written by the
    outbox relay), invocation metadata is bound around the dedup mark and the
    handler so tracing survives the broker hop:

    - ``correlation_id`` — the ``forze_correlation_id`` header value: the
      handler runs under the *originating* correlation id.
    - ``causation_id`` — the consumed event's id (``forze_event_id`` header,
      falling back to ``message.key``): the consumed event *causes* the
      handler's effects, extending the standard causation chain.
    - ``execution_id`` — kept from the already-bound consumer metadata when
      present (processing a message is its own execution), otherwise a fresh
      id.

    Without a parseable correlation header the binding is skipped entirely
    (current behavior). Malformed UUID header values are ignored.

    :param bind_tenant_from_headers: **Opt-in** (default ``False``): when
        ``True`` and the ``forze_tenant_id`` header carries a valid UUID, the
        tenant identity is bound for the handler scope. Opt-in because headers
        are untrusted input — within a deployment they are written by the
        application's own relay, but any producer with broker access could
        forge them; only enable this for brokers where every producer is
        trusted to assert tenancy.
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

    headers = _message_headers(message)
    correlation_id = _parse_uuid(headers.get(HEADER_CORRELATION_ID))

    with ExitStack() as stack:
        if correlation_id is not None:
            causation_id = _parse_uuid(headers.get(HEADER_EVENT_ID)) or _parse_uuid(
                getattr(message, "key", None)
            )
            current = ctx.inv_ctx.get_metadata()
            metadata = InvocationMetadata(
                execution_id=current.execution_id if current is not None else uuid7(),
                correlation_id=correlation_id,
                causation_id=causation_id,
            )
            stack.enter_context(ctx.inv_ctx.bind_metadata(metadata=metadata))

        if bind_tenant_from_headers:
            tenant_id = _parse_uuid(headers.get(HEADER_TENANT_ID))

            if tenant_id is not None:
                stack.enter_context(
                    ctx.inv_ctx.bind_identity(
                        authn=ctx.inv_ctx.get_authn(),
                        tenant=TenantIdentity(tenant_id=tenant_id),
                    )
                )

        async with ctx.tx_ctx.scope(tx_route):
            port = ctx.inbox(inbox_spec)

            if not await port.mark_if_unseen(str(inbox_spec.name), dedup_id):
                return False

            await handler(message)
            return True

    # Unreachable: the ExitStack body always returns. Keeps type checkers
    # convinced every path returns a bool.
    return False  # pragma: no cover
