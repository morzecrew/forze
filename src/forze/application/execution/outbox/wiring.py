"""Compose outbox command ports at the execution boundary."""

from collections.abc import Sequence
from typing import Protocol, runtime_checkable

from pydantic import BaseModel

from forze.application.contracts.outbox import OutboxSpec, StagedOutboxEntry
from forze.application.execution.context import ExecutionContext
from forze.application.integrations.outbox import OutboxStaging, StagingOutboxCommand
from forze.application.integrations.outbox.staging import FlushRowsFn

from .enrichment import InvocationOutboxEnricher
from collections.abc import Awaitable

# ----------------------- #


@runtime_checkable
class OutboxRowPersistPort(Protocol):
    """Narrow store surface used when wiring flush into staging."""

    def persist_rows(self, rows: Sequence[StagedOutboxEntry]) -> Awaitable[int]:
        """Insert staged rows; return count of new rows."""
        ...


# ....................... #


def build_staging_outbox_command[M: BaseModel](
    ctx: ExecutionContext,
    spec: OutboxSpec[M],
    *,
    flush_rows: FlushRowsFn,
) -> StagingOutboxCommand[M]:
    """Wire :class:`StagingOutboxCommand` for *spec* using *ctx* invocation state."""

    staging = OutboxStaging(
        staging=ctx.outbox_staging,
        spec=spec,
        enricher=InvocationOutboxEnricher(inv=ctx.inv_ctx),
        flush_rows=flush_rows,
    )
    return StagingOutboxCommand(spec=spec, staging=staging)


# ....................... #


def build_staging_outbox_command_for_store[M: BaseModel](
    ctx: ExecutionContext,
    spec: OutboxSpec[M],
    store: OutboxRowPersistPort,
) -> StagingOutboxCommand[M]:
    """Wire command port that flushes via *store.persist_rows*."""

    return build_staging_outbox_command(ctx, spec, flush_rows=store.persist_rows)
