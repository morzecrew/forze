"""In-memory durable function step memo adapter."""

from __future__ import annotations

from typing import Awaitable, Callable, TypeVar, cast, final

import attrs

from forze.application.contracts.durable.function import (
    DurableFunctionStepPort,
    current_durable_run,
)
from forze.application.execution.tracing import record
from forze_mock.state import MockState

# ----------------------- #

T = TypeVar("T")


@final
@attrs.define(slots=True, kw_only=True)
class MockDurableFunctionStepAdapter(DurableFunctionStepPort):
    """Memoize step results in :attr:`MockState.durable_step_memo`.

    Keys the memo by the ambient :class:`DurableRunContext` run id when one is bound (the
    durable-function runner / saga executor path), falling back to :attr:`run_id` for direct
    single-run use — so it journals per run like the Postgres adapter.

    Emits a ``durable`` step event (``executed`` on the first run, ``replayed`` from the memo)
    into the runtime trace, so a deterministic-simulation oracle can assert a completed step
    replays from the memo instead of re-executing across a crash. The recorded **result** is
    exactly-once; a body may still run more than once if a worker is reclaimed / crashes
    mid-body before the result is journaled, so keep step bodies idempotent.
    """

    state: MockState
    run_id: str = "default"

    async def run[T](
        self,
        step_id: str,
        fn: Callable[[], Awaitable[T]],
    ) -> T:
        run = current_durable_run()
        run_id = run.run_id if run is not None else self.run_id
        key = f"{run_id}:{step_id}"

        with self.state.lock:
            memo = self.state.durable_step_memo
            if key in memo:
                record(
                    domain="durable",
                    op="step",
                    route=run_id,
                    key=step_id,
                    outcome="replayed",
                )
                return cast(T, memo[key])

        result = await fn()

        with self.state.lock:
            # First-write-wins convergence, matching the Postgres ``ON CONFLICT DO NOTHING``:
            # if a concurrent execution journaled this step first, discard our result and
            # return the winner's so every caller agrees on one value. (The body still ran —
            # an at-least-once effect under a reclaimed lease; keep step bodies idempotent.)
            memo = self.state.durable_step_memo
            if key not in memo:
                memo[key] = result  # type: ignore[arg-type]
            winner = memo[key]

        record(
            domain="durable",
            op="step",
            route=run_id,
            key=step_id,
            outcome="executed",
        )

        return cast("T", winner)
