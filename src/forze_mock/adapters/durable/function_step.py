"""In-memory durable function step memo adapter."""

from __future__ import annotations

from typing import Awaitable, Callable, TypeVar, cast, final

import attrs

from forze.application.contracts.durable.function import (
    DurableFunctionStepPort,
    current_durable_run,
)
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
                return cast(T, memo[key])

        result = await fn()

        with self.state.lock:
            self.state.durable_step_memo[key] = result  # type: ignore[arg-type]

        return result
