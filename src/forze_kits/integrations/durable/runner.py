"""Durable-function runner: enqueue, execute-in-process, and recover abandoned runs."""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING, final
from uuid import UUID

import attrs

from forze.application.contracts.durable.function import (
    DurableRunContext,
    DurableRunRecord,
    DurableRunStatus,
    bind_durable_run,
    reset_durable_run,
)
from forze.base.exceptions import CoreException

from ._resolve import resolve_durable_run_store
from .registry import DurableFunctionRegistry

if TYPE_CHECKING:
    from forze.application.execution.context import ExecutionContext
    from forze.base.primitives import JsonDict

# ----------------------- #

_FORWARD_INCOMPLETE_CODE = "saga.forward_incomplete"
"""A saga that committed at its pivot but could not complete forward — a distinct terminal
state from an ordinary failure (no compensation happened; manual completion is required)."""


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class DurableFunctionRunner:
    """Drive durable functions over a :class:`DurableRunStorePort` and the step journal.

    ``enqueue`` records a run; ``run_now`` records and executes it in-process; ``recover``
    re-claims abandoned runs (crashed mid-flight) and re-invokes them — completed steps
    replay from the journal, so each step effect applies exactly once across a crash.
    """

    registry: DurableFunctionRegistry
    """Name → durable-function body (must contain a run's ``name`` to execute/recover it)."""

    lease_for: timedelta = timedelta(minutes=5)
    """How long a claim leases a run before the recovery scanner may reclaim it."""

    # ....................... #

    async def enqueue(
        self,
        ctx: ExecutionContext,
        name: str,
        input_json: JsonDict | None = None,
        *,
        idempotency_key: str | None = None,
        tenant_id: UUID | None = None,
    ) -> DurableRunRecord:
        """Record a new ``PENDING`` run (idempotency-key re-submits converge on one run)."""

        store = resolve_durable_run_store(ctx)

        return await store.enqueue(
            name,
            input_json=input_json,
            idempotency_key=idempotency_key,
            tenant_id=tenant_id,
        )

    # ....................... #

    async def run_now(
        self,
        ctx: ExecutionContext,
        name: str,
        input_json: JsonDict | None = None,
        *,
        idempotency_key: str | None = None,
        tenant_id: UUID | None = None,
    ) -> DurableRunRecord:
        """Enqueue and execute a run in-process; return its final record.

        An idempotent re-submit that already completed returns immediately; a run already
        claimed elsewhere is returned as-is (not double-executed).
        """

        store = resolve_durable_run_store(ctx)
        record = await store.enqueue(
            name,
            input_json=input_json,
            idempotency_key=idempotency_key,
            tenant_id=tenant_id,
        )

        if record.status is DurableRunStatus.COMPLETED:
            return record

        claimed = await store.begin(record.run_id, lease_for=self.lease_for)

        if claimed is None:
            return await store.load(record.run_id) or record

        await self._execute(ctx, claimed, reraise=True)

        return await store.load(claimed.run_id) or claimed

    # ....................... #

    async def recover(
        self,
        ctx: ExecutionContext,
        *,
        limit: int = 10,
    ) -> int:
        """Claim up to *limit* abandoned runs and re-invoke them; return the count claimed.

        A body failure during recovery is recorded on the run and swallowed (the scanner
        keeps draining), never propagated.
        """

        store = resolve_durable_run_store(ctx)
        claimed = await store.claim_abandoned(limit=limit, lease_for=self.lease_for)

        for record in claimed:
            await self._execute(ctx, record, reraise=False)

        return len(claimed)

    # ....................... #

    async def _execute(
        self,
        ctx: ExecutionContext,
        record: DurableRunRecord,
        *,
        reraise: bool,
    ) -> None:
        store = resolve_durable_run_store(ctx)
        handler = self.registry.get(record.name)

        token = bind_durable_run(
            DurableRunContext(
                run_id=record.run_id,
                name=record.name,
                attempt=record.attempts,
            )
        )

        try:
            output = await handler(ctx, record.input_json)

        except CoreException as error:
            # A pivot-committed saga that could not complete forward is a distinct terminal
            # state — never compensated, must be finished by hand — not an ordinary failure.
            if (error.code or "") == _FORWARD_INCOMPLETE_CODE:
                await store.mark_forward_incomplete(record.run_id, error=str(error))
            else:
                await store.fail(record.run_id, error=str(error))

            if reraise:
                raise

            return

        except Exception as error:  # noqa: BLE001 — record then optionally re-raise
            await store.fail(record.run_id, error=str(error))

            if reraise:
                raise

            return

        finally:
            reset_durable_run(token)

        await store.complete(record.run_id, output_json=output)
