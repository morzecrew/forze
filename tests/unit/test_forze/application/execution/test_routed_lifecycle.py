"""Tests for routed_client_lifecycle_step."""

import pytest

from forze.application.execution.context import ExecutionContext
from forze.application.execution.lifecycle import LifecyclePlan
from forze.application.execution.lifecycle.builtin import routed_client_lifecycle_step
from forze_mock import MockDepsModule, MockState

# ----------------------- #


class _MockRoutedClient:
    def __init__(self) -> None:
        self.startup_calls = 0
        self.close_calls = 0

    async def startup(self) -> None:
        self.startup_calls += 1

    async def close(self) -> None:
        self.close_calls += 1


@pytest.fixture
def ctx() -> ExecutionContext:
    return ExecutionContext(deps=MockDepsModule(state=MockState())())


class TestRoutedClientLifecycleStep:
    @pytest.mark.asyncio
    async def test_startup_and_shutdown_invoke_client(
        self, ctx: ExecutionContext
    ) -> None:
        client = _MockRoutedClient()
        plan = LifecyclePlan.from_steps(
            routed_client_lifecycle_step("routed", client=client),
        )

        await plan.startup(ctx)
        await plan.shutdown(ctx)

        assert client.startup_calls == 1
        assert client.close_calls == 1
