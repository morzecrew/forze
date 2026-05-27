"""Unit tests for DurableFunctionStepPort."""

import pytest

from forze.application.contracts.durable.function import (
    DurableFunctionStepDepKey,
    DurableFunctionStepPort,
)


class _StubStepPort(DurableFunctionStepPort):
    async def run[T](self, step_id: str, fn) -> T:
        assert step_id
        return await fn()


class TestDurableFunctionStepPorts:
    def test_runtime_checkable(self) -> None:
        assert isinstance(_StubStepPort(), DurableFunctionStepPort)

    def test_dep_key_name(self) -> None:
        assert DurableFunctionStepDepKey.name == "durable_function_step"


@pytest.mark.asyncio
async def test_run_delegates_to_callable() -> None:
    port = _StubStepPort()

    async def _work() -> int:
        return 42

    assert await port.run("step-a", _work) == 42
