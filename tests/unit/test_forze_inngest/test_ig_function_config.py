"""InngestFunctionConfig forwards function-level controls (retries / idempotency / …) into the
SDK's ``create_function``; an unset config leaves the SDK defaults untouched."""

from datetime import timedelta
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


def test_all_function_config_fields_forwarded_to_sdk() -> None:
    # Every field must reach ``sdk.create_function`` — a dropped/renamed forward would silently
    # ignore the control, so pin each one through ``fn._opts``.
    config = InngestFunctionConfig(
        retries=5,
        idempotency="event.data.id",
        concurrency=[inngest.Concurrency(limit=2)],
        rate_limit=inngest.RateLimit(limit=5, period=timedelta(seconds=60)),
        throttle=inngest.Throttle(limit=4, period=timedelta(seconds=30)),
        priority=inngest.Priority(run="event.data.p"),
        debounce=inngest.Debounce(period=timedelta(seconds=10)),
        batch_events=inngest.Batch(max_size=10, timeout=timedelta(seconds=5)),
        timeouts=inngest.Timeouts(start=timedelta(seconds=7)),
        singleton=inngest.Singleton(key="event.data.id", mode="skip"),
        cancel=[inngest.Cancel(event="app/cancel")],
    )

    opts = _register(config)._opts  # pyright: ignore[reportPrivateUsage]

    assert opts.retries == 5
    assert opts.idempotency == "event.data.id"
    assert opts.concurrency == [inngest.Concurrency(limit=2)]
    assert opts.rate_limit == inngest.RateLimit(limit=5, period=timedelta(seconds=60))
    assert opts.throttle == inngest.Throttle(limit=4, period=timedelta(seconds=30))
    assert opts.priority == inngest.Priority(run="event.data.p")
    assert opts.debounce == inngest.Debounce(period=timedelta(seconds=10))
    assert opts.batch_events == inngest.Batch(
        max_size=10, timeout=timedelta(seconds=5)
    )
    assert opts.timeouts == inngest.Timeouts(start=timedelta(seconds=7))
    assert opts.singleton == inngest.Singleton(key="event.data.id", mode="skip")
    assert opts.cancel == [inngest.Cancel(event="app/cancel")]


def test_no_config_leaves_sdk_defaults() -> None:
    fn = _register(None)
    opts = fn._opts  # pyright: ignore[reportPrivateUsage]
    assert opts.retries is None
    assert opts.idempotency is None
