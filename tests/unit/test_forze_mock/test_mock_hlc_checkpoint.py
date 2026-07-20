"""In-memory HLC high-water-mark adapter and startup restart recovery."""

from __future__ import annotations

import pytest

from forze.application.execution.lifecycle.builtin import (
    hlc_checkpoint_recovery_lifecycle_step,
)
from forze.base.primitives import HlcTimestamp
from forze_mock import MockDepsModule
from forze_mock.adapters.hlc_checkpoint import MockHlcCheckpointAdapter
from forze_mock.state import MockState
from tests.support.execution_context import context_from_modules

# ----------------------- #


class TestMockHlcCheckpointAdapter:
    async def test_load_is_none_when_empty(self) -> None:
        assert await MockHlcCheckpointAdapter(state=MockState()).load() is None

    async def test_advance_then_load_roundtrips(self) -> None:
        adapter = MockHlcCheckpointAdapter(state=MockState())
        await adapter.advance(HlcTimestamp(9_000, 3))

        assert await adapter.load() == HlcTimestamp(9_000, 3)

    async def test_advance_is_monotonic(self) -> None:
        adapter = MockHlcCheckpointAdapter(state=MockState())
        await adapter.advance(HlcTimestamp(9_000, 3))
        await adapter.advance(HlcTimestamp(5_000, 0))  # older physical → ignored
        await adapter.advance(HlcTimestamp(9_000, 3))  # equal → ignored

        assert await adapter.load() == HlcTimestamp(9_000, 3)

    async def test_load_returns_max_across_node_keys(self) -> None:
        state = MockState()
        await MockHlcCheckpointAdapter(state=state, node_key="a").advance(
            HlcTimestamp(1_000, 0)
        )
        await MockHlcCheckpointAdapter(state=state, node_key="b").advance(
            HlcTimestamp(2_000, 9)
        )

        # A restart resumes above the whole deployment's emissions, not just this node's.
        loaded = await MockHlcCheckpointAdapter(state=state, node_key="a").load()
        assert loaded == HlcTimestamp(2_000, 9)

    async def test_advance_reverts_on_transaction_rollback(self) -> None:
        # Co-located fidelity: a rolled-back business transaction reverts the mark too, so
        # it never advances for rows that did not commit.
        state = MockState()
        ctx = context_from_modules(MockDepsModule(state=state, hlc_checkpoint=True))
        adapter = MockHlcCheckpointAdapter(state=state)

        class _Boom(Exception): ...

        with pytest.raises(_Boom):
            async with ctx.tx_ctx.scope("default"):
                await adapter.advance(HlcTimestamp(7_000, 0))
                raise _Boom

        assert await adapter.load() is None  # reverted with the transaction


# ....................... #


class TestRestartRecovery:
    async def test_recovery_resumes_clock_above_persisted_mark(self) -> None:
        # A prior process persisted a high-water mark; a fresh runtime (clock at (0, 0))
        # seeds above it at startup so it cannot re-issue below a prior emission.
        state = MockState()
        state.hlc_checkpoint["default"] = HlcTimestamp(9_000, 4).pack()

        ctx = context_from_modules(MockDepsModule(state=state, hlc_checkpoint=True))
        assert ctx.outbox_clock.last == HlcTimestamp(0, 0)  # fresh, pre-recovery

        await hlc_checkpoint_recovery_lifecycle_step().startup(ctx)

        assert ctx.outbox_clock.last == HlcTimestamp(9_000, 4)

    async def test_recovery_is_a_noop_when_nothing_persisted(self) -> None:
        ctx = context_from_modules(MockDepsModule(hlc_checkpoint=True))

        await hlc_checkpoint_recovery_lifecycle_step().startup(ctx)

        assert ctx.outbox_clock.last == HlcTimestamp(0, 0)

    async def test_recovery_is_a_noop_when_no_checkpoint_wired(self) -> None:
        # Default module: no checkpoint dep registered → recovery is a safe no-op.
        ctx = context_from_modules(MockDepsModule())

        await hlc_checkpoint_recovery_lifecycle_step().startup(ctx)

        assert ctx.outbox_clock.last == HlcTimestamp(0, 0)
