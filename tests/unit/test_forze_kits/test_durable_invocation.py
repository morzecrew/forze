"""Every self-hosted durable attempt runs with invocation metadata of its own (RFC 0079 §5.3, Q4).

# covers: DurableFunctionRunner._execute

An attempt binds surface ``durable``, a fresh execution id, the run id as the correlation id,
and the in-process caller's execution id as the causation. The execution id owns the
idempotency claims the attempt takes, so a claim must begin and end within one attempt.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any
from uuid import UUID

import attrs
from structlog.contextvars import get_contextvars

from forze.application.contracts.durable.function import (
    DurableRunStatus,
    current_durable_run,
)
from forze.application.contracts.idempotency import IdempotencyRecord, IdempotencySpec
from forze.application.execution import ExecutionContext, InvocationMetadata
from forze.application.execution.context.invocation import (
    CORR_ID_KEY,
    EXEC_ID_KEY,
    SURFACE_KEY,
)
from forze.application.execution.outbox import InvocationOutboxEnricher
from forze.base.exceptions import CoreException, ExceptionKind
from forze.base.primitives import utcnow, uuid7
from forze.domain.models import BaseDTO
from forze_kits.integrations.durable import (
    DurableFunctionRegistry,
    DurableFunctionRunner,
    resolve_durable_run_store,
    resolve_durable_step,
)
from forze_mock import MockDepsModule, MockState
from tests.support.execution_context import context_from_modules

# ----------------------- #

_IDEMPOTENCY = IdempotencySpec(name="charges")
_OP, _KEY, _HASH = "charge", "k-1", "h-1"


class _Shipped(BaseDTO):
    n: int


def _context(state: MockState | None = None) -> ExecutionContext:
    return context_from_modules(MockDepsModule(state=state or MockState()))


def _recording(seen: list[dict[str, Any]]) -> DurableFunctionRegistry:
    registry = DurableFunctionRegistry()

    async def body(ctx: ExecutionContext, _input: dict | None) -> dict:
        enricher = InvocationOutboxEnricher(inv=ctx.inv_ctx, clock=ctx.outbox_clock)
        seen.append(
            {
                "metadata": ctx.inv_ctx.get_metadata(),
                "logs": dict(get_contextvars()),
                "event": enricher.enrich("shipped", _Shipped(n=1)),
            }
        )
        return {}

    registry.register("fn", body)
    return registry


def _expire_lease(state: MockState, run_id: str) -> None:
    state.durable_runs[run_id]["leased_until"] = utcnow() - timedelta(hours=1)


def _expire_claims(state: MockState) -> None:
    for key, (status, payload_hash, entry) in list(state.idempotency.items()):
        expired = attrs.evolve(entry, expires_at=utcnow() - timedelta(seconds=1))
        state.idempotency[key] = (status, payload_hash, expired)


async def _until(event: asyncio.Event) -> None:
    await asyncio.wait_for(event.wait(), timeout=5)


# ....................... #


class TestAttemptMetadata:
    async def test_a_run_binds_surface_ids_and_logs_them(self) -> None:
        ctx = _context()
        seen: list[dict[str, Any]] = []
        runner = DurableFunctionRunner(registry=_recording(seen))

        record = await runner.run_now(ctx, "fn")

        ((attempt,),) = [seen]
        metadata = attempt["metadata"]
        assert isinstance(metadata, InvocationMetadata)
        assert metadata.surface == "durable"
        assert metadata.correlation_id == UUID(record.run_id)
        assert metadata.causation_id is None
        # Log lines and published events carry the attempt's ids.
        assert attempt["logs"][SURFACE_KEY] == "durable"
        assert attempt["logs"][EXEC_ID_KEY] == str(metadata.execution_id)
        assert attempt["logs"][CORR_ID_KEY] == record.run_id
        assert attempt["event"].execution_id == metadata.execution_id
        assert attempt["event"].correlation_id == metadata.correlation_id
        assert ctx.inv_ctx.get_metadata() is None

    async def test_run_now_inside_an_invocation_records_it_as_the_cause(self) -> None:
        ctx = _context()
        seen: list[dict[str, Any]] = []
        runner = DurableFunctionRunner(registry=_recording(seen))
        caller = InvocationMetadata(execution_id=uuid7(), correlation_id=uuid7(), surface="http")

        with ctx.inv_ctx.bind_metadata(metadata=caller):
            record = await runner.run_now(ctx, "fn")

            assert ctx.inv_ctx.get_metadata() is caller

        metadata = seen[0]["metadata"]
        # The run names itself; the request is reached through the causation id.
        assert metadata.surface == "durable"
        assert metadata.execution_id != caller.execution_id
        assert metadata.correlation_id == UUID(record.run_id)
        assert metadata.causation_id == caller.execution_id


# ....................... #


class TestClaimsAcrossAttempts:
    """RFC 0079 §10 Q4: a claim must begin and end within one attempt."""

    async def test_a_claim_begun_in_one_attempt_is_refused_at_the_next_attempts_commit(
        self,
    ) -> None:
        state = MockState()
        ctx = _context(state)
        claimed = asyncio.Event()
        attempts: list[InvocationMetadata | None] = []
        outcome: dict[str, BaseException | None] = {}
        registry = DurableFunctionRegistry()

        async def body(ctx: ExecutionContext, _input: dict | None) -> dict:
            port = ctx.idempotency(_IDEMPOTENCY)
            attempts.append(ctx.inv_ctx.get_metadata())

            async def claim() -> dict:
                await port.begin(_OP, _KEY, _HASH)
                return {}

            # Memoized: the next attempt replays it and takes no claim of its own.
            await resolve_durable_step(ctx).run("claim", claim)

            if current_durable_run().attempt == 1:  # type: ignore[union-attr]
                claimed.set()
                await asyncio.Event().wait()  # the worker dies here

            try:
                await port.commit(_OP, _KEY, _HASH, IdempotencyRecord(result=b"r"))
                outcome["commit"] = None

            except CoreException as error:
                outcome["commit"] = error

            return {}

        registry.register("fn", body)
        runner = DurableFunctionRunner(registry=registry)
        store = resolve_durable_run_store(ctx)

        first = asyncio.create_task(runner.run_now(ctx, "fn"))
        await _until(claimed)
        first.cancel()
        await asyncio.gather(first, return_exceptions=True)

        (run_id,) = state.durable_runs
        _expire_lease(state, run_id)
        assert await runner.recover(ctx) == 1

        error = outcome["commit"]
        assert isinstance(error, CoreException)
        assert error.kind is ExceptionKind.CONFLICT
        # Each attempt is its own invocation, and every attempt of the run shares its id.
        first_attempt, second_attempt = attempts
        assert first_attempt is not None and second_attempt is not None
        assert first_attempt.execution_id != second_attempt.execution_id
        assert first_attempt.correlation_id == second_attempt.correlation_id == UUID(run_id)
        reloaded = await store.load(run_id)
        assert reloaded is not None and reloaded.status is DurableRunStatus.COMPLETED

    async def test_a_superseded_attempts_late_commit_does_not_replace_its_successors_claim(
        self,
    ) -> None:
        state = MockState()
        ctx = _context(state)
        first_claimed, second_claimed = asyncio.Event(), asyncio.Event()
        release_first, release_second = asyncio.Event(), asyncio.Event()
        commits: dict[int, BaseException | None] = {}
        registry = DurableFunctionRegistry()

        async def body(ctx: ExecutionContext, _input: dict | None) -> dict:
            attempt = current_durable_run().attempt  # type: ignore[union-attr]
            port = ctx.idempotency(_IDEMPOTENCY)

            async def charge() -> dict:
                # Begun and committed within one step, so within one attempt.
                await port.begin(_OP, _KEY, _HASH)
                (first_claimed if attempt == 1 else second_claimed).set()
                await _until(release_first if attempt == 1 else release_second)

                try:
                    await port.commit(
                        _OP, _KEY, _HASH, IdempotencyRecord(result=f"r{attempt}".encode())
                    )
                    commits[attempt] = None

                except CoreException as error:
                    commits[attempt] = error

                return {}

            await resolve_durable_step(ctx).run("charge", charge)
            return {}

        registry.register("fn", body)
        runner = DurableFunctionRunner(registry=registry)

        first = asyncio.create_task(runner.run_now(ctx, "fn"))
        await _until(first_claimed)

        # The first attempt overran its lease and its claim's window; recovery takes over.
        (run_id,) = state.durable_runs
        _expire_lease(state, run_id)
        _expire_claims(state)
        second = asyncio.create_task(runner.recover(ctx))
        await _until(second_claimed)

        release_first.set()
        await asyncio.gather(first, return_exceptions=True)
        release_second.set()
        assert await second == 1

        late = commits[1]
        assert isinstance(late, CoreException)
        assert late.kind is ExceptionKind.CONFLICT
        assert commits[2] is None
        # The successor's record is the one kept.
        port = ctx.idempotency(_IDEMPOTENCY)
        replay = await port.begin(_OP, _KEY, _HASH)
        assert replay is not None and replay.result == b"r2"
