"""Consumer-side dedup: process a message exactly-once via the inbox."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from contextlib import ExitStack
from uuid import UUID

from forze.application.contracts.envelope import (
    HEADER_CORRELATION_ID,
    HEADER_EVENT_ID,
    HEADER_HLC,
    HEADER_TENANT_ID,
    HEADER_TRACEPARENT,
)
from forze.application.contracts.inbox import InboxSpec
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution.context import ExecutionContext
from forze.application.execution.context.invocation import InvocationMetadata
from forze.base.exceptions import CoreException, exc
from forze.base.primitives import HlcTimestamp, HybridLogicalClock, StrKey, uuid7

# ----------------------- #


def _merge_inbound_hlc(
    headers: Mapping[str, object],
    clock: HybridLogicalClock,
) -> None:
    """Advance this node's clock past a consumed event's HLC (best-effort).

    So an event produced *in reaction* to this one causally follows it. A
    malformed or absent header is ignored — ordering is best-effort.
    """

    raw = headers.get(HEADER_HLC)

    if not isinstance(raw, str) or not raw:
        return

    try:
        clock.update(HlcTimestamp.parse(raw))

    except CoreException:
        return


# ----------------------- #


def _parse_uuid(value: object) -> UUID | None:
    """Best-effort UUID parse: malformed header values are ignored."""

    if not isinstance(value, str) or not value:
        return None

    try:
        return UUID(value)

    except ValueError:
        return None


# ----------------------- #


def _attach_trace_context(headers: Mapping[str, object], stack: ExitStack) -> None:
    """Attach the publish-side W3C trace context (from ``HEADER_TRACEPARENT``) for the handler's scope.

    So the consume span links to the publishing operation's span — the async outbox→broker→inbox hop
    becomes one distributed trace. Registered on *stack* (detaches on exit). No-op when the header is
    absent/malformed. OpenTelemetry is imported lazily so a consumer that never carries a traceparent
    keeps an OTel-free import path.
    """

    raw = headers.get(HEADER_TRACEPARENT)

    if not isinstance(raw, str) or not raw:
        return

    from opentelemetry import context as otel_context

    from forze.application.execution.tracing.propagation import (
        context_from_traceparent,
    )

    token = otel_context.attach(context_from_traceparent(raw))
    stack.callback(otel_context.detach, token)


# ....................... #


def _message_headers(message: object) -> Mapping[str, str]:
    """Return the message's transport headers, tolerating header-less types."""

    headers = getattr(message, "headers", None)

    return (
        headers if isinstance(headers, Mapping) else {}
    )  # pyright: ignore[reportUnknownVariableType]


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

    **Dedup id priority**: an explicit *message_id* extractor wins; otherwise
    the ``forze_event_id`` header (written by the outbox relay), then
    ``message.key``, then ``message.id``. The header outranks ``key`` because
    the relay publishes ``key`` as the staged *ordering key* when one is set —
    two **different** events of the same aggregate then share a ``key`` and
    must not dedupe each other, while a redelivery of the **same** event keeps
    its ``forze_event_id`` and is skipped.

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

    headers = _message_headers(message)
    header_event_id = headers.get(HEADER_EVENT_ID)

    if message_id is not None:
        dedup_id: str | None = message_id(message)

    else:
        dedup_id = (
            header_event_id
            or getattr(message, "key", None)
            or getattr(message, "id", None)
        )

    if not dedup_id:
        raise exc.precondition(
            "Cannot deduplicate message: no event-id header, key, or id; "
            "pass a message_id extractor",
        )

    correlation_id = _parse_uuid(headers.get(HEADER_CORRELATION_ID))

    with ExitStack() as stack:
        # Outermost: link the handler's spans to the publishing operation's span (one distributed
        # trace across the broker hop), wrapping the dedup mark, tx scope, and handler below.
        _attach_trace_context(headers, stack)

        if correlation_id is not None:
            causation_id = _parse_uuid(header_event_id) or _parse_uuid(
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

            # Exactly-once holds only if the dedup mark commits atomically with the
            # handler's writes — i.e. the inbox store runs on the same client this scope
            # opened the transaction on. Wiring the inbox and ``tx_route`` to different
            # pools would silently break that; fail closed instead.
            ctx.tx_ctx.assert_enlisted(port, what=f"Inbox route {inbox_spec.name!r}")

            if not await port.mark_if_unseen(str(inbox_spec.name), dedup_id):
                return False

            # Only a genuinely new message advances this node's clock — a
            # replayed/duplicate one must not (it would let forged or repeated
            # headers skew causality).
            _merge_inbound_hlc(headers, ctx.outbox_clock)

            await handler(message)
            return True

    # Unreachable: the ExitStack body always returns. Keeps type checkers
    # convinced every path returns a bool.
    return False  # pragma: no cover
