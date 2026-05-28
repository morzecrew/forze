from pydantic import BaseModel

from forze.application.contracts.durable.function import (
    DurableFunctionEventTrigger,
    DurableFunctionInvokeSpec,
    DurableFunctionSpec,
)
from forze.application.execution import Deps, ExecutionContext
from forze_inngest import InngestClient, InngestFunctionBinding, register_functions

from tests.unit.test_forze_inngest.helpers import RecordingInngestClient


class _In(BaseModel):
    value: str


class _Out(BaseModel):
    ok: bool = True


def test_register_functions_returns_sdk_functions() -> None:
    client = InngestClient(app_id="forze-register-test")
    spec = DurableFunctionSpec(
        name="on-test",
        run=DurableFunctionInvokeSpec(args_type=_In, return_type=_Out),
        triggers=(DurableFunctionEventTrigger(event="app/test"),),
    )

    async def _handler(_args: _In) -> _Out:
        return _Out(ok=True)

    binding = InngestFunctionBinding(
        spec=spec,
        handler_factory=lambda _ctx: _handler,
    )

    deps = Deps.plain({})
    functions = register_functions(
        client,
        [binding],
        ctx_factory=lambda: ExecutionContext(deps=deps),
    )

    assert len(functions) == 1


def test_recording_client_native_used_for_registration() -> None:
    from forze.application.contracts.durable.function import DurableFunctionCronTrigger

    recording = RecordingInngestClient()

    spec = DurableFunctionSpec(
        name="cron-fn",
        run=DurableFunctionInvokeSpec(args_type=_In, return_type=_Out),
        triggers=(DurableFunctionCronTrigger(expression="0 * * * *"),),
    )

    binding = InngestFunctionBinding(
        spec=spec,
        handler_factory=lambda _ctx: (lambda _a: _Out()),
    )

    fns = register_functions(
        recording,
        [binding],
        ctx_factory=lambda: ExecutionContext(deps=Deps.plain({})),
    )

    assert len(fns) == 1
