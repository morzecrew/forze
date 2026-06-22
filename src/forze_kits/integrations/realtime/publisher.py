"""Publish realtime signals onto messaging — the application-facing surface.

A :class:`RealtimePublisher` holds its **resolved ports** (the stream command
port, the optional durable outbox port) and a tenant provider, all materialized
once by :func:`build_realtime_publisher` at construction — call it from a handler
factory, the same way handlers inject `ctx.doc.command(spec)`. So the publish
methods take only their arguments (never a spec to resolve or the whole context),
and a misconfigured route fails when the handler is built, not on the first emit.

Two disciplines, made explicit at the call site (one method each):

- :meth:`RealtimePublisher.publish` — **ephemeral, at-most-once**: append directly
  to the realtime stream, fire-and-forget (typing, presence, live cursors).
- :meth:`RealtimePublisher.stage` — **durable, at-least-once**: stage to the outbox
  in the current transaction; the relay appends it to the stream after commit.

The application stays on the messaging side — it never reaches a connection. The
tenant rides in the message headers (ephemeral) or the relayed claim headers
(durable); the gateway scopes the room from it.
"""

from typing import final

import attrs
from pydantic import BaseModel

from forze.application.contracts.envelope import HEADER_TENANT_ID
from forze.application.contracts.outbox import OutboxCommandPort, OutboxSpec
from forze.application.contracts.realtime import Audience, RealtimeEvent, RealtimeSignal
from forze.application.contracts.stream import (
    StreamCommandDepKey,
    StreamCommandPort,
    StreamSpec,
)
from forze.application.contracts.tenancy import TenantProviderPort
from forze.application.execution import ExecutionContext
from forze.base.exceptions import exc

# ----------------------- #


def _partition_key(audience: Audience) -> str:
    """Per-audience ordering/partition key so a topic's signals stay in order."""

    return f"{audience.kind.value}:{audience.name}"


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class RealtimePublisher:
    """Publish realtime signals onto a stream (ephemeral) or the outbox (durable).

    Built with its resolved ports injected (see :func:`build_realtime_publisher`);
    the methods take only their arguments.
    """

    stream: StreamCommandPort[RealtimeSignal]
    """The resolved realtime stream command port."""

    stream_name: str
    """The stream name signals are appended to."""

    tenant_provider: TenantProviderPort
    """Reads the ambient tenant at emit time (carried in the message headers)."""

    outbox: OutboxCommandPort[RealtimeSignal] | None = None
    """The resolved durable outbox port; required by :meth:`stage`."""

    # ....................... #

    async def publish[Payload: BaseModel](
        self,
        audience: Audience,
        event: RealtimeEvent[Payload],
        payload: Payload,
    ) -> str:
        """Ephemeral (at-most-once): append the signal to the stream, fire-and-forget.

        :returns: The broker-assigned message id.
        """

        signal = RealtimeSignal.for_event(audience, event, payload)

        return await self.stream.append(
            self.stream_name,
            signal,
            type=event.name,
            key=_partition_key(audience),
            headers=self._tenant_headers(),
        )

    # ....................... #

    async def stage[Payload: BaseModel](
        self,
        audience: Audience,
        event: RealtimeEvent[Payload],
        payload: Payload,
    ) -> None:
        """Durable (at-least-once): stage the signal to the outbox in the current tx.

        The relay appends it to the stream after commit; flushing happens with the
        transaction (or call ``ctx.outbox.command(spec).flush()``).
        """

        if self.outbox is None:
            raise exc.configuration(
                "RealtimePublisher was built without an outbox; pass outbox_spec to "
                "build_realtime_publisher to enable durable .stage"
            )

        signal = RealtimeSignal.for_event(audience, event, payload)

        await self.outbox.stage(
            event.name,
            signal,
            ordering_key=_partition_key(audience),
        )

    # ....................... #

    def _tenant_headers(self) -> dict[str, str]:
        tenant = self.tenant_provider()

        return {HEADER_TENANT_ID: str(tenant.tenant_id)} if tenant is not None else {}


# ....................... #


def build_realtime_publisher(
    ctx: ExecutionContext,
    *,
    stream_spec: StreamSpec[RealtimeSignal],
    outbox_spec: OutboxSpec[RealtimeSignal] | None = None,
) -> RealtimePublisher:
    """Resolve the realtime ports and build a publisher — call from a handler factory.

    Materializes the dependencies at build time, so a missing route is caught when
    the handler is constructed, not on first emit. Publishing is a side effect, so
    this refuses to build in a read-only (``QUERY``) operation.
    """

    if ctx.inv_ctx.is_read_only():
        raise exc.precondition(
            "Cannot build a RealtimePublisher in a read-only (QUERY) operation"
        )

    stream = ctx.deps.resolve_configurable(
        ctx, StreamCommandDepKey, stream_spec, route=stream_spec.name
    )
    outbox = ctx.outbox.command(outbox_spec) if outbox_spec is not None else None

    return RealtimePublisher(
        stream=stream,
        stream_name=str(stream_spec.name),
        tenant_provider=ctx.inv_ctx.get_tenant,
        outbox=outbox,
    )
