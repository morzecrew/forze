"""The self-hosted durable→registry bridge.

# covers: forze_kits.integrations.durable.operation_bridge

``DurableFunctionSpec.operation`` had auto-bridging only on the Inngest tier; the
self-hosted twin validates the run's stored JSON into the spec's args type and
dispatches through ``run_operation`` — plans and hooks apply, no bypass — so every
self-hosted app stops hand-writing the same body.
"""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from forze.application.contracts.durable.function import (
    DurableFunctionCronTrigger,
    DurableFunctionInvokeSpec,
    DurableFunctionSpec,
)
from forze.application.execution.operations import OperationRegistry
from forze.base.exceptions import CoreException
from forze_kits.integrations.durable import (
    DurableFunctionRegistry,
    operation_durable_handler,
    register_operation_functions,
)
from forze_mock import MockDepsModule
from tests.support.execution_context import context_from_modules

# ----------------------- #


class _Args(BaseModel):
    order_id: str


class _Out(BaseModel):
    handled: str


async def _handle(args: _Args) -> _Out:
    return _Out(handled=args.order_id)


def _operations():  # type: ignore[no-untyped-def]
    return OperationRegistry().set_handler("sync.run", lambda _ctx: _handle).freeze()


def _spec(operation: str | None = "sync.run") -> DurableFunctionSpec[_Args, _Out]:
    return DurableFunctionSpec(
        name="sync",
        run=DurableFunctionInvokeSpec(args_type=_Args),
        operation=operation,
        triggers=(DurableFunctionCronTrigger(expression="0 * * * *"),),
    )


# ....................... #


@pytest.mark.asyncio
async def test_bridge_validates_input_and_dispatches_the_operation() -> None:
    handler = operation_durable_handler(_spec(), _operations())
    ctx = context_from_modules(MockDepsModule())

    output = await handler(ctx, {"order_id": "o-1"})

    assert output == {"handled": "o-1"}  # BaseModel output lands as JSON


@pytest.mark.asyncio
async def test_malformed_stored_input_is_a_clean_precondition() -> None:
    handler = operation_durable_handler(_spec(), _operations())
    ctx = context_from_modules(MockDepsModule())

    with pytest.raises(CoreException) as caught:
        await handler(ctx, {"order_id": 123456})  # wrong type in the stored record

    assert caught.value.code == "durable_input_invalid"


def test_spec_without_operation_is_refused() -> None:
    with pytest.raises(CoreException, match="declares no operation"):
        operation_durable_handler(_spec(operation=None), _operations())


@pytest.mark.asyncio
async def test_register_operation_functions_registers_each_body() -> None:
    registry = DurableFunctionRegistry()
    register_operation_functions(registry, [_spec()], operations=_operations())

    assert registry.registered("sync")

    ctx = context_from_modules(MockDepsModule())
    output = await registry.get("sync")(ctx, {"order_id": "o-2"})
    assert output == {"handled": "o-2"}
