"""Startup restart recovery over the in-memory high-water mark.

The adapter's own contract — monotonic max, reads across node keys, atomic with the
business transaction — lives in the shared battery
(``tests/unit/test_forze_mock/test_mock_hlc_checkpoint_conformance.py``), which holds every
engine to it. What is left here is the lifecycle step above it: resuming a *clock* from a
persisted mark, which is the mock's own concern and has no engine counterpart.
"""

from __future__ import annotations

from forze.application.execution.lifecycle.builtin import (
    hlc_checkpoint_recovery_lifecycle_step,
)
from forze.base.primitives import HlcTimestamp
from forze_mock import MockDepsModule
from forze_mock.state import MockState
from tests.support.execution_context import context_from_modules

# ----------------------- #


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
