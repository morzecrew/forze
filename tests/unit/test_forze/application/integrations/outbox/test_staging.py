"""Unit tests for :class:`~forze.application.integrations.outbox.OutboxStaging`."""

from __future__ import annotations

from uuid import uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.outbox import OutboxSpec, StagedOutboxEntry
from forze.application.execution import DepsRegistry, ExecutionContext
from forze.application.execution.outbox import InvocationOutboxEnricher
from forze.application.integrations.outbox import OutboxStaging
from forze.base.serialization import PydanticModelCodec


class _Payload(BaseModel):
    value: str


@pytest.mark.asyncio
async def test_flush_delegates_buffered_rows() -> None:
    flushed: list[StagedOutboxEntry] = []

    async def _flush(rows: list[StagedOutboxEntry]) -> int:
        flushed.extend(rows)
        return len(rows)

    spec = OutboxSpec(
        name="events",
        codec=PydanticModelCodec(_Payload),
    )
    ctx = ExecutionContext(deps=DepsRegistry().freeze().resolve())
    coord = OutboxStaging(
        staging=ctx.outbox_staging,
        spec=spec,
        enricher=InvocationOutboxEnricher(inv=ctx.inv_ctx),
        flush_rows=_flush,
    )

    await coord.stage("demo.created", _Payload(value="a"))
    count = await coord.flush()

    assert count == 1
    assert len(flushed) == 1
    assert flushed[0].event.event_type == "demo.created"
    assert ctx.outbox_staging.flushed is True


@pytest.mark.asyncio
async def test_cannot_stage_after_flush() -> None:
    spec = OutboxSpec(
        name="events",
        codec=PydanticModelCodec(_Payload),
    )
    ctx = ExecutionContext(deps=DepsRegistry().freeze().resolve())

    async def _flush(_rows: list[StagedOutboxEntry]) -> int:
        return 0

    coord = OutboxStaging(
        staging=ctx.outbox_staging,
        spec=spec,
        enricher=InvocationOutboxEnricher(inv=ctx.inv_ctx),
        flush_rows=_flush,
    )
    await coord.flush()

    with pytest.raises(Exception, match="after flush"):
        await coord.stage("x", _Payload(value="y"))
