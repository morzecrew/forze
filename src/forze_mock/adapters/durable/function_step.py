"""In-memory durable function step memo adapter."""

from __future__ import annotations

from typing import Awaitable, Callable, TypeVar, cast, final

import attrs

from forze.application.contracts.durable.function import DurableFunctionStepPort
from forze_mock.state import MockState

# ----------------------- #

T = TypeVar("T")


@final
@attrs.define(slots=True, kw_only=True)
class MockDurableFunctionStepAdapter(DurableFunctionStepPort):
    """Memoize step results in :attr:`MockState.durable_step_memo`."""

    state: MockState
    run_id: str = "default"

    async def run[T](
        self,
        step_id: str,
        fn: Callable[[], Awaitable[T]],
    ) -> T:
        key = f"{self.run_id}:{step_id}"

        with self.state.lock:
            memo = self.state.durable_step_memo
            if key in memo:
                return cast(T, memo[key])

        result = await fn()

        with self.state.lock:
            self.state.durable_step_memo[key] = result  # type: ignore[arg-type]

        return result
