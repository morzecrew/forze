"""Publish realtime signals onto messaging — the application-facing surface.

Two disciplines, made explicit at the call site (one method each):

- :meth:`RealtimePublisher.publish` — **ephemeral, at-most-once**: append the
  signal directly to the realtime stream, fire-and-forget. Lost if no connection
  is currently joined (typing indicators, presence, live cursors).
- :meth:`RealtimePublisher.stage` — **durable, at-least-once**: stage the signal
  to the outbox in the current transaction; the relay appends it to the same
  stream after commit (must-arrive-while-online signals).

The application stays on the messaging side — it never reaches a connection. The
tenant rides in the message headers (ephemeral) or the relayed claim headers
(durable); the gateway scopes the room from it. Publishing is a side effect, so
it is refused from a read-only (``QUERY``) operation.
"""

from typing import final

import attrs
from pydantic import BaseModel

from forze.application.contracts.envelope import HEADER_TENANT_ID
from forze.application.contracts.outbox import OutboxSpec
from forze.application.contracts.realtime import Audience, RealtimeEvent, RealtimeSignal
from forze.application.contracts.stream import StreamCommandDepKey, StreamSpec
from forze.application.execution import ExecutionContext
from forze.base.exceptions import exc

# ----------------------- #


def _require_writable(ctx: ExecutionContext) -> None:
    """Refuse a realtime publish from a read-only (``QUERY``) operation.

    Stream/pubsub command ports are not guarded by the kernel's read-only check
    (they have no convenience accessor), so the realtime surface enforces it
    itself: pushing to clients is a side effect a query must not perform.
    """

    if ctx.inv_ctx.is_read_only():
        raise exc.precondition(
            "Cannot publish a realtime signal from a read-only (QUERY) operation"
        )


# ....................... #


def _partition_key(audience: Audience) -> str:
    """Per-audience ordering/partition key so a topic's signals stay in order."""

    return f"{audience.kind.value}:{audience.name}"


# ....................... #


def _tenant_headers(ctx: ExecutionContext) -> dict[str, str]:
    """Carry the ambient tenant on the message, mirroring the outbox relay."""

    tenant = ctx.inv_ctx.get_tenant()

    return {HEADER_TENANT_ID: str(tenant.tenant_id)} if tenant is not None else {}


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class RealtimePublisher:
    """Publish realtime signals onto a stream (ephemeral) or the outbox (durable)."""

    stream_spec: StreamSpec[RealtimeSignal]
    """The realtime stream signals are appended to."""

    outbox_spec: OutboxSpec[RealtimeSignal] | None = None
    """Outbox route for durable signals; required by :meth:`stage`."""

    # ....................... #

    async def publish[Payload: BaseModel](
        self,
        ctx: ExecutionContext,
        audience: Audience,
        event: RealtimeEvent[Payload],
        payload: Payload,
    ) -> str:
        """Ephemeral (at-most-once): append the signal to the stream, fire-and-forget.

        :returns: The broker-assigned message id.
        """

        _require_writable(ctx)

        signal = RealtimeSignal.for_event(audience, event, payload)
        command = ctx.deps.resolve_configurable(
            ctx,
            StreamCommandDepKey,
            self.stream_spec,
            route=self.stream_spec.name,
        )

        return await command.append(
            str(self.stream_spec.name),
            signal,
            type=event.name,
            key=_partition_key(audience),
            headers=_tenant_headers(ctx),
        )

    # ....................... #

    async def stage[Payload: BaseModel](
        self,
        ctx: ExecutionContext,
        audience: Audience,
        event: RealtimeEvent[Payload],
        payload: Payload,
    ) -> None:
        """Durable (at-least-once): stage the signal to the outbox in the current tx.

        The relay appends it to the stream after commit; flushing happens with the
        transaction (or call ``ctx.outbox.command(spec).flush()``).
        """

        if self.outbox_spec is None:
            raise exc.configuration(
                "RealtimePublisher.stage requires an outbox_spec"
            )

        _require_writable(ctx)

        signal = RealtimeSignal.for_event(audience, event, payload)

        await ctx.outbox.command(self.outbox_spec).stage(
            event.name,
            signal,
            ordering_key=_partition_key(audience),
        )
