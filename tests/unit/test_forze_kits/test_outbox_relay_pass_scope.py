"""Unit tests for the relay's pass scope, its budget and the publish timeout.

# covers: forze_kits.integrations.outbox._relay_core.relay_outbox_claims
# covers: forze.application.contracts.outbox.outbox_pass_scope
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, AsyncIterator, Iterator
from contextlib import asynccontextmanager, contextmanager, suppress
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import MagicMock, PropertyMock, patch
from uuid import uuid4

import attrs
import pytest
from pydantic import BaseModel

from forze.application.contracts.deps import implements
from forze.application.contracts.outbox import (
    OutboxClaim,
    OutboxDeps,
    OutboxPassBudget,
    OutboxQueryPort,
    OutboxSpec,
    OutboxStatus,
    TransactionalOutboxPasses,
    outbox_pass_scope,
)
from forze.application.execution import DepsRegistry, ExecutionRuntime
from forze.application.execution.context import ExecutionContext
from forze.base.exceptions import CoreException, exc
from forze.base.serialization import PydanticModelCodec
from forze_kits.integrations.outbox import OutboxRelay
from forze_kits.integrations.outbox._relay_core import relay_outbox_claims
from forze_mock import MockDepsModule, MockStateDepKey
from forze_mock.adapters import MockState
from forze_mock.adapters.outbox import MockOutboxRow, MockOutboxStore

# ----------------------- #


class _Payload(BaseModel):
    n: int


_T0 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
_DECRYPT = "forze_kits.integrations.outbox._relay_core.decrypt_outbox_payload"
_SPEC: OutboxSpec[_Payload] = OutboxSpec(name="events", codec=PydanticModelCodec(_Payload))


def _seed(state: MockState, count: int) -> list[MockOutboxRow]:
    rows = [
        MockOutboxRow(
            id=uuid4(),
            outbox_route="events",
            event_id=uuid4(),
            event_type="job.requested",
            payload={"n": index},
            status=OutboxStatus.PENDING,
            tenant_id=None,
            execution_id=None,
            correlation_id=None,
            causation_id=None,
            occurred_at=_T0,
            created_at=_T0 + timedelta(microseconds=index),
        )
        for index in range(count)
    ]
    state.outbox_rows.setdefault("events", []).extend(rows)
    return rows


@asynccontextmanager
async def _context() -> AsyncIterator[tuple[ExecutionContext, MockState]]:
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())

    async with runtime.scope():
        ctx = runtime.get_context()
        yield ctx, ctx.deps.provide(MockStateDepKey)


def _budget_scope(deadline_in: float, entered: list[str] | None = None) -> Any:
    """A pass scope that yields a budget ending *deadline_in* seconds after it is entered."""

    @asynccontextmanager
    async def scope(_self: Any) -> AsyncGenerator[OutboxPassBudget]:
        if entered is not None:
            entered.append("enter")

        yield OutboxPassBudget(deadline=asyncio.get_running_loop().time() + deadline_in)

        if entered is not None:
            entered.append("exit")

    return scope


@contextmanager
def _scoped(scope: Any) -> Iterator[None]:
    """Give the mock store the pass extension, with *scope* as its pass scope."""

    with (
        patch.object(MockOutboxStore, "pass_scope", scope, create=True),
        patch.object(
            MockOutboxStore,
            "transactional_passes",
            new_callable=PropertyMock,
            return_value=True,
            create=True,
        ),
    ):
        yield


async def _relay(ctx: ExecutionContext, publish_one: Any, **kwargs: Any) -> Any:
    return await relay_outbox_claims(
        ctx,
        outbox_spec=_SPEC,
        publish_one=publish_one,
        reclaim_stale_after=None,
        **kwargs,
    )


async def _publish_ok(claim: OutboxClaim, payload: Any) -> None:
    del claim, payload


@attrs.define(slots=True)
class _OldSurfaceStore:
    """A store written against the query port before the pass capability: structural only."""

    inner: MockOutboxStore[Any]

    @property
    def spec(self) -> OutboxSpec[Any]:
        return self.inner.spec

    def claim_pending(self, *, limit: int | None = None) -> Any:
        return self.inner.claim_pending(limit=limit)

    def mark_published(self, ids: Any) -> Any:
        return self.inner.mark_published(ids)

    def mark_failed(self, ids: Any, *, error: str | None = None) -> Any:
        return self.inner.mark_failed(ids, error=error)

    def mark_retry(self, ids: Any, **kwargs: Any) -> Any:
        return self.inner.mark_retry(ids, **kwargs)

    def reclaim_stale_processing(self, *, older_than: Any) -> Any:
        return self.inner.reclaim_stale_processing(older_than=older_than)

    def requeue_failed(self, ids: Any) -> Any:
        return self.inner.requeue_failed(ids)


# ....................... #


class TestOldSurfaceStore:
    async def test_a_store_without_the_pass_capability_is_a_query_port_and_relays(
        self,
    ) -> None:
        async with _context() as (ctx, state):
            rows = _seed(state, 2)
            store = _OldSurfaceStore(inner=ctx.outbox.query(_SPEC))  # type: ignore[arg-type]

            with patch.object(OutboxDeps, "query", lambda _self, _spec: store):
                result = await _relay(ctx, _publish_ok)

        assert isinstance(store, OutboxQueryPort)
        assert result.published == 2
        assert [row.status for row in rows] == [OutboxStatus.PUBLISHED] * 2


# ....................... #


class TestRelayPassScope:
    async def test_a_store_without_the_extension_gets_a_scope_that_does_nothing(self) -> None:
        async with _context() as (ctx, _state):
            store = ctx.outbox.query(_SPEC)

            async with outbox_pass_scope(store) as budget:
                assert budget is None

        assert not implements(store, TransactionalOutboxPasses)
        assert not implements(MagicMock(spec=OutboxQueryPort), TransactionalOutboxPasses)

    async def test_one_pass_enters_the_scope_once_around_its_claim_and_marks(self) -> None:
        entered: list[str] = []
        seen_inside: list[bool] = []

        async def publish(claim: OutboxClaim, payload: Any) -> None:
            seen_inside.append(entered == ["enter"])

        async with _context() as (ctx, state):
            _seed(state, 3)

            with _scoped(_budget_scope(60.0, entered)):
                result = await _relay(ctx, publish)

        assert entered == ["enter", "exit"]
        assert seen_inside == [True, True, True]
        assert result.published == 3


class TestRelayPassScopeDoubles:
    async def test_a_test_doubles_stand_in_budget_bounds_nothing(self) -> None:
        # A bare MagicMock has every attribute, and its pass_scope() yields a MagicMock.
        double = MagicMock()

        async with _context() as (ctx, state):
            _seed(state, 2)

            with _scoped(lambda _self: double.pass_scope()):
                result = await _relay(ctx, _publish_ok)

        assert result.published == 2


class TestRelayPassBudget:
    async def test_a_spent_budget_leaves_every_claimed_row_unchanged(self) -> None:
        calls: list[OutboxClaim] = []

        async def publish(claim: OutboxClaim, payload: Any) -> None:
            calls.append(claim)

        async with _context() as (ctx, state):
            rows = _seed(state, 3)

            with _scoped(_budget_scope(-1.0)):
                result = await _relay(ctx, publish)

        assert calls == []
        assert (result.claimed, result.published, result.retried, result.failed) == (3, 0, 0, 0)
        assert all(row.attempts == 0 and row.last_error is None for row in rows)

    async def test_the_deadline_cuts_the_publish_in_flight_and_stops_the_pass(self) -> None:
        async def publish(claim: OutboxClaim, payload: Any) -> None:
            if payload.n == 1:
                await asyncio.sleep(30)  # a destination slower than the pass allows

        async with _context() as (ctx, state):
            rows = _seed(state, 3)

            with _scoped(_budget_scope(0.3)):
                result = await asyncio.wait_for(_relay(ctx, publish), timeout=10)

        assert (result.published, result.retried, result.failed) == (1, 1, 0)
        first, cut, unreached = rows
        assert first.status == OutboxStatus.PUBLISHED
        # The cut publish is a transient failure: it keeps its attempt and backoff.
        assert cut.attempts == 1 and cut.available_at is not None
        assert cut.last_error is not None and cut.last_error.startswith("publish timed out")
        # The row after the deadline was never touched.
        assert unreached.attempts == 0 and unreached.last_error is None


class TestRelayDecodeBound:
    async def test_a_decode_still_running_at_the_deadline_is_cut_and_its_row_left(
        self,
    ) -> None:
        # A decrypt waiting on a slow KMS must not hold the pass's session idle past the
        # deadline: the server would cut the session off and the whole pass would roll back.
        calls: list[OutboxClaim] = []

        async def publish(claim: OutboxClaim, payload: Any) -> None:
            calls.append(claim)

        async def slow_decrypt(_cipher: Any, payload: Any, **_: Any) -> Any:
            if payload["n"] == 1:
                await asyncio.sleep(30)

            return payload

        async with _context() as (ctx, state):
            rows = _seed(state, 3)

            with (
                _scoped(_budget_scope(0.3)),
                patch(_DECRYPT, slow_decrypt),
            ):
                result = await asyncio.wait_for(_relay(ctx, publish), timeout=10)

        assert [claim.id for claim in calls] == [rows[0].id]
        assert (result.published, result.retried, result.failed) == (1, 0, 0)
        assert all(row.attempts == 0 and row.last_error is None for row in rows[1:])

    async def test_a_decode_that_turns_its_cut_into_an_error_is_not_parked(self) -> None:
        # Some clients answer a cancelled request with an error of their own; the row is
        # still sound and must wait for the next pass, not be parked as poison.
        async def converting_decrypt(_cipher: Any, payload: Any, **_: Any) -> Any:
            if payload["n"] == 1:
                try:
                    await asyncio.sleep(30)

                except asyncio.CancelledError:
                    raise exc.internal("KMS request cancelled") from None

            return payload

        async with _context() as (ctx, state):
            rows = _seed(state, 3)

            with (
                _scoped(_budget_scope(0.3)),
                patch(_DECRYPT, converting_decrypt),
            ):
                result = await asyncio.wait_for(_relay(ctx, _publish_ok), timeout=10)

        assert (result.published, result.failed) == (1, 0)
        assert rows[1].status != OutboxStatus.FAILED and rows[1].last_error is None

    async def test_a_decode_that_ignores_its_cut_leaves_its_row(self) -> None:
        # A decode that swallows the cancellation returns after the deadline: the row must
        # still not be published past it.
        calls: list[OutboxClaim] = []

        async def publish(claim: OutboxClaim, payload: Any) -> None:
            calls.append(claim)

        async def stubborn_decrypt(_cipher: Any, payload: Any, **_: Any) -> Any:
            if payload["n"] == 1:
                with suppress(asyncio.CancelledError):
                    await asyncio.sleep(30)

            return payload

        async with _context() as (ctx, state):
            rows = _seed(state, 3)

            with (
                _scoped(_budget_scope(0.3)),
                patch(_DECRYPT, stubborn_decrypt),
            ):
                result = await asyncio.wait_for(_relay(ctx, publish), timeout=10)

        assert [claim.id for claim in calls] == [rows[0].id]
        assert (result.published, result.retried) == (1, 0)
        assert all(row.attempts == 0 for row in rows[1:])


class TestRelayPublishUntil:
    async def test_it_ends_publishing_before_a_later_budget_does(self) -> None:
        calls: list[OutboxClaim] = []

        async def publish(claim: OutboxClaim, payload: Any) -> None:
            calls.append(claim)

        async with _context() as (ctx, state):
            rows = _seed(state, 2)

            with _scoped(_budget_scope(60.0)):
                result = await _relay(
                    ctx, publish, publish_until=asyncio.get_running_loop().time() - 1
                )

        assert calls == []
        assert (result.claimed, result.published) == (2, 0)
        assert all(row.attempts == 0 for row in rows)


class TestRelayPublishTimeout:
    async def test_a_slow_publish_is_retried_and_the_rest_publish(self) -> None:
        async def publish(claim: OutboxClaim, payload: Any) -> None:
            if payload.n == 0:
                await asyncio.sleep(30)

        async with _context() as (ctx, state):
            rows = _seed(state, 3)
            result = await asyncio.wait_for(
                _relay(ctx, publish, publish_timeout=timedelta(milliseconds=50)),
                timeout=10,
            )

        assert (result.published, result.retried) == (2, 1)
        assert rows[0].attempts == 1
        assert rows[0].last_error == "publish timed out after 0.050s"
        assert [row.status for row in rows[1:]] == [OutboxStatus.PUBLISHED] * 2

    async def test_a_slow_publish_at_its_last_attempt_is_parked(self) -> None:
        async def publish(claim: OutboxClaim, payload: Any) -> None:
            await asyncio.sleep(30)

        async with _context() as (ctx, state):
            [row] = _seed(state, 1)
            result = await asyncio.wait_for(
                _relay(ctx, publish, max_attempts=1, publish_timeout=timedelta(milliseconds=20)),
                timeout=10,
            )

        assert result.failed == 1
        assert row.status == OutboxStatus.FAILED

    @pytest.mark.parametrize("timeout", [timedelta(0), timedelta(seconds=-1)])
    def test_a_timeout_that_is_not_positive_is_refused(self, timeout: timedelta) -> None:
        with pytest.raises(CoreException, match="publish_timeout"):
            OutboxRelay(outbox_spec=_SPEC, publish_timeout=timeout)

    async def test_a_publish_error_keeps_its_own_message(self) -> None:
        async def publish(claim: OutboxClaim, payload: Any) -> None:
            raise RuntimeError("broker down")

        async with _context() as (ctx, state):
            [row] = _seed(state, 1)
            await _relay(ctx, publish, publish_timeout=timedelta(seconds=5))

        assert row.last_error == "broker down"
