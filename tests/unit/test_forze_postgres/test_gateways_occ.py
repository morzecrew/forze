"""Unit tests for the transaction-aware Postgres OCC retry decorator."""

import attrs
import pytest
from psycopg import errors

from forze.application.contracts.resilience import ResilienceExecutorPort
from forze.application.execution.resilience import default_resilience_executor
from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze_postgres.kernel.gateways._occ import postgres_occ_retry

# ----------------------- #


def _client_side_conflict() -> CoreException:
    """A conflict the gateway detected itself (zero rows matched): no server error."""

    return exc.concurrency("Failed to update record")


def _server_side_conflict() -> CoreException:
    """A conflict mapped from a psycopg server error (the transaction is aborted)."""

    error = exc.concurrency("Transaction serialization failure. Please retry.")
    error.__cause__ = errors.SerializationFailure(
        "could not serialize access due to concurrent update"
    )

    return error


# ....................... #


class _FakeClient:
    def __init__(self, *, in_transaction: bool) -> None:
        self._in_transaction = in_transaction

    def is_in_transaction(self) -> bool:
        return self._in_transaction


@attrs.define(slots=True, kw_only=True)
class _Boundary:
    """Minimal structural host for the decorator: a client and a resilience executor."""

    client: _FakeClient
    resilience: ResilienceExecutorPort = attrs.field(factory=default_resilience_executor)

    calls: int = 0
    failures: list[CoreException] = attrs.field(factory=list)
    result: str = "ok"

    @postgres_occ_retry
    async def write(self) -> str:
        self.calls += 1

        if self.failures:
            raise self.failures.pop(0)

        return self.result


# ....................... #


class TestOutsideTransaction:
    @pytest.mark.asyncio
    async def test_retries_client_side_conflict(self) -> None:
        boundary = _Boundary(
            client=_FakeClient(in_transaction=False),
            failures=[_client_side_conflict()],
        )

        assert await boundary.write() == "ok"
        assert boundary.calls == 2

    @pytest.mark.asyncio
    async def test_retries_server_side_conflict(self) -> None:
        """Outside a transaction the method's own transaction rolled back, so
        even server-reported conflicts are retried on a clean slate."""

        boundary = _Boundary(
            client=_FakeClient(in_transaction=False),
            failures=[_server_side_conflict()],
        )

        assert await boundary.write() == "ok"
        assert boundary.calls == 2

    @pytest.mark.asyncio
    async def test_exhausts_attempts_then_raises(self) -> None:
        boundary = _Boundary(
            client=_FakeClient(in_transaction=False),
            failures=[_client_side_conflict() for _ in range(5)],
        )

        with pytest.raises(CoreException, match="Failed to update record"):
            await boundary.write()

        assert boundary.calls == 3


# ....................... #


class TestInsideCallerTransaction:
    @pytest.mark.asyncio
    async def test_retries_client_side_conflict(self) -> None:
        """A zero-rows stale-revision miss leaves the transaction healthy; the
        in-place retry re-reads committed state and can succeed."""

        boundary = _Boundary(
            client=_FakeClient(in_transaction=True),
            failures=[_client_side_conflict()],
        )

        assert await boundary.write() == "ok"
        assert boundary.calls == 2

    @pytest.mark.asyncio
    async def test_server_side_conflict_runs_once_and_surfaces_original(self) -> None:
        """A server-reported conflict aborted the transaction: no re-attempt, and
        the original ``concurrency`` error surfaces for the scope owner."""

        original = _server_side_conflict()
        boundary = _Boundary(
            client=_FakeClient(in_transaction=True),
            failures=[original],
        )

        with pytest.raises(CoreException) as excinfo:
            await boundary.write()

        assert excinfo.value is original
        assert excinfo.value.kind is ExceptionKind.CONCURRENCY
        assert boundary.calls == 1

    @pytest.mark.asyncio
    async def test_server_error_nested_in_context_chain_is_detected(self) -> None:
        """The psycopg error may sit anywhere in the cause/context chain."""

        inner = exc.concurrency("Deadlock detected. Please retry.")
        inner.__context__ = errors.DeadlockDetected("deadlock detected")
        wrapper = exc.concurrency("wrapped")
        wrapper.__cause__ = inner

        boundary = _Boundary(
            client=_FakeClient(in_transaction=True),
            failures=[wrapper],
        )

        with pytest.raises(CoreException) as excinfo:
            await boundary.write()

        assert excinfo.value is wrapper
        assert boundary.calls == 1

    @pytest.mark.asyncio
    async def test_non_concurrency_error_propagates_without_retry(self) -> None:
        boundary = _Boundary(
            client=_FakeClient(in_transaction=True),
            failures=[exc.not_found("Record not found")],
        )

        with pytest.raises(CoreException, match="Record not found"):
            await boundary.write()

        assert boundary.calls == 1
