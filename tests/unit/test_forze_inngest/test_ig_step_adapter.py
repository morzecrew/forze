from collections.abc import Awaitable, Callable
from typing import TypeVar
from unittest.mock import AsyncMock

import pytest

from forze_inngest.adapters.step import (
    InngestStepAdapter,
    bind_inngest_step,
    require_inngest_step,
    reset_inngest_step,
)

T = TypeVar("T")


class _FakeStep:
    def __init__(self) -> None:
        self.run = AsyncMock(side_effect=self._run)

    async def _run(
        self,
        step_id: str,
        fn: Callable[[], Awaitable[T]],
    ) -> T:
        return await fn()


@pytest.mark.asyncio
async def test_step_adapter_delegates_to_bound_step() -> None:
    fake = _FakeStep()
    token = bind_inngest_step(fake)

    try:
        adapter = InngestStepAdapter()

        async def _body() -> str:
            return "ok"

        result = await adapter.run("step-a", _body)

    finally:
        reset_inngest_step(token)

    assert result == "ok"
    fake.run.assert_awaited_once()


def test_require_inngest_step_raises_outside_run() -> None:
    from tests.support.exceptions import assert_precondition

    with pytest.raises(Exception) as err:
        require_inngest_step()

    assert_precondition(err.value)
