"""InngestFunctionConfig forwards function-level controls (retries / idempotency / …) into the
SDK's ``create_function``; an unset config leaves the SDK defaults untouched."""

from typing import Any

import inngest
import pytest
from pydantic import BaseModel

from forze.application.contracts.durable.function import (
    DurableFunctionEventTrigger,
    DurableFunctionInvokeSpec,
    DurableFunctionSpec,
)
from forze.application.execution import Deps
from forze_inngest import (
    InngestClient,
    InngestFunctionBinding,
    InngestFunctionConfig,
    register_functions,
)
from tests.support.execution_context import context_from_deps

pytestmark = pytest.mark.unit


class _In(BaseModel):
    value: str


class _Out(BaseModel):
    ok: bool = True


async def _handler(_args: _In) -> _Out:
    return _Out()


def _register(config: InngestFunctionConfig | None) -> Any:
    client = InngestClient(app_id="forze-cfg-test")
    spec = DurableFunctionSpec(
        name="on-x",
        run=DurableFunctionInvokeSpec(args_type=_In, return_type=_Out),
        triggers=(DurableFunctionEventTrigger(event="app/x"),),
    )
    binding = InngestFunctionBinding(
        spec=spec, handler_factory=lambda _c: _handler, config=config
    )
    fns = register_functions(
        client, [binding], ctx_factory=lambda: context_from_deps(Deps.plain({}))
    )
    return fns[0]


def test_function_config_forwarded_to_sdk() -> None:
    fn = _register(InngestFunctionConfig(retries=5, idempotency="event.data.id"))
    opts = fn._opts  # pyright: ignore[reportPrivateUsage]
    assert opts.retries == 5
    assert opts.idempotency == "event.data.id"


def test_no_config_leaves_sdk_defaults() -> None:
    fn = _register(None)
    opts = fn._opts  # pyright: ignore[reportPrivateUsage]
    assert opts.retries is None
    assert opts.idempotency is None
