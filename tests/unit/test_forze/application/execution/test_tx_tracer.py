"""Unit tests for :class:`TxTracer` and transaction scope observation."""

from __future__ import annotations

import attrs
import pytest

from forze.application.execution.context.transaction import TransactionContext
from forze.application.execution.tracing import (
    NOOP_TX_TRACER,
    RecordingRuntimeTracer,
    runtime_tracer_from_flag,
    tx_tracer_from_runtime,
)
from forze_mock.adapters import MockTxManagerAdapter

# ----------------------- #


@attrs.define(slots=True)
class RecordingTxTracer:
    """In-memory transaction tracer for unit tests."""

    events: list[tuple[str, str, int]] = attrs.field(factory=list)
    exit_outcomes: list[bool] = attrs.field(factory=list)
    """The ``committed`` flag recorded for each scope exit (commit vs rollback)."""

    @property
    def enabled(self) -> bool:
        return True

    def on_scope_enter(self, *, route: str, depth: int, tx_id: int | None = None) -> None:
        del tx_id
        self.events.append(("enter", route, depth))

    def on_scope_exit(
        self, *, route: str, depth: int, tx_id: int | None = None, committed: bool = True
    ) -> None:
        del tx_id
        self.events.append(("exit", route, depth))
        self.exit_outcomes.append(committed)


def _mock_tx_resolver(_route: str) -> MockTxManagerAdapter:
    return MockTxManagerAdapter()


class TestTxTracerFromRuntime:
    def test_disabled_runtime_returns_noop(self) -> None:
        tracer = tx_tracer_from_runtime(runtime_tracer_from_flag(False))

        assert tracer is NOOP_TX_TRACER
        assert not tracer.enabled

    def test_enabled_runtime_forwards_tx_events(self) -> None:
        runtime = RecordingRuntimeTracer()
        tracer = tx_tracer_from_runtime(runtime)

        tracer.on_scope_enter(route="mock", depth=1)
        tracer.on_scope_exit(route="mock", depth=1)

        trace = runtime.snapshot()
        assert trace is not None
        assert any(e.domain == "tx" and e.op == "enter" for e in trace.events)
        assert any(e.domain == "tx" and e.op == "exit" for e in trace.events)


class TestTransactionContextTxTracer:
    @pytest.mark.asyncio
    async def test_injected_tracer_receives_root_scope_events(self) -> None:
        recording = RecordingTxTracer()
        tx = TransactionContext()
        tx.lock(_mock_tx_resolver, tx_tracer=recording)

        async with tx.scope("mock"):
            pass

        assert recording.events == [
            ("enter", "mock", 1),
            ("exit", "mock", 1),
        ]

    @pytest.mark.asyncio
    async def test_nested_scope_does_not_emit_tx_events(self) -> None:
        recording = RecordingTxTracer()
        tx = TransactionContext()
        tx.lock(_mock_tx_resolver, tx_tracer=recording)

        async with tx.scope("mock"), tx.scope("mock"):
            pass

        assert recording.events == [
            ("enter", "mock", 1),
            ("exit", "mock", 1),
        ]

    @pytest.mark.asyncio
    async def test_clean_exit_records_a_commit(self) -> None:
        recording = RecordingTxTracer()
        tx = TransactionContext()
        tx.lock(_mock_tx_resolver, tx_tracer=recording)

        async with tx.scope("mock"):
            pass

        assert recording.exit_outcomes == [True]  # committed

    @pytest.mark.asyncio
    async def test_a_rollback_records_a_non_commit(self) -> None:
        # The exit fires from a finally on rollback too, but is marked not-committed so a rolled-back
        # scope is never read as a commit (the per-commit oracle relies on this).
        recording = RecordingTxTracer()
        tx = TransactionContext()
        tx.lock(_mock_tx_resolver, tx_tracer=recording)

        with pytest.raises(RuntimeError):
            async with tx.scope("mock"):
                raise RuntimeError("boom")

        assert recording.events == [("enter", "mock", 1), ("exit", "mock", 1)]
        assert recording.exit_outcomes == [False]  # rollback

    @pytest.mark.asyncio
    async def test_default_lock_uses_noop_tracer(self) -> None:
        tx = TransactionContext()
        tx.lock(_mock_tx_resolver)

        async with tx.scope("mock"):
            pass

        assert tx._tx_tracer is NOOP_TX_TRACER
