"""Tests for registry-backed durable function runners."""

from __future__ import annotations

import attrs
import pytest
from pydantic import BaseModel

from forze.application.contracts.durable.function import (
    DurableFunctionCronTrigger,
    DurableFunctionInvokeSpec,
    DurableFunctionSpec,
)
from forze.application.contracts.execution import Handler
from forze.application.execution.operations.run import (
    handler_for_registry_operation,
    run_durable_function,
)
from forze.application.execution.operations.registry import OperationRegistry
from forze.base.exceptions import CoreException

# ----------------------- #


class _CronArgs(BaseModel):
    tick: int = 0


@attrs.define(slots=True)
class _EchoHandler(Handler[_CronArgs, str]):
    """Minimal handler for registry resolution tests."""

    async def __call__(self, args: _CronArgs) -> str:
        return f"tick:{args.tick}"


def _echo_factory(_ctx: object) -> _EchoHandler:
    return _EchoHandler()


def _frozen_echo_registry() -> object:
    return (
        OperationRegistry(handlers={"jobs.echo": _echo_factory})
        .bind("jobs.echo")
        .finish()
        .freeze()
    )


def _cron_spec(*, operation: str | None = "jobs.echo") -> DurableFunctionSpec[_CronArgs, str]:
    return DurableFunctionSpec(
        name="echo-cron",
        operation=operation,
        run=DurableFunctionInvokeSpec(args_type=_CronArgs, return_type=str),
        triggers=(DurableFunctionCronTrigger(expression="0 * * * *"),),
    )


class TestRunDurableFunction:
    @pytest.mark.asyncio
    async def test_runs_resolved_operation(self, traced_ctx) -> None:
        spec = _cron_spec()
        registry = _frozen_echo_registry()

        result = await run_durable_function(
            spec,
            registry,
            traced_ctx,
            _CronArgs(tick=3),
        )

        assert result == "tick:3"

    @pytest.mark.asyncio
    async def test_requires_operation_on_spec(self, traced_ctx) -> None:
        spec = _cron_spec(operation=None)
        registry = _frozen_echo_registry()

        with pytest.raises(CoreException, match="operation is required"):
            await run_durable_function(spec, registry, traced_ctx, _CronArgs())


class TestHandlerForRegistryOperation:
    def test_factory_resolves_like_registry(self, traced_ctx) -> None:
        registry = _frozen_echo_registry()
        factory = handler_for_registry_operation(registry, "jobs.echo")

        handler = factory(traced_ctx)
        direct = registry.resolve("jobs.echo", traced_ctx)

        assert type(handler) is type(direct)
