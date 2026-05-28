import attrs
import pytest
from pydantic import BaseModel

from forze.application.contracts.durable.function import (
    DurableFunctionCronTrigger,
    DurableFunctionEventTrigger,
    DurableFunctionInvokeSpec,
    DurableFunctionSpec,
)
from forze.application.contracts.execution import Handler
from forze.application.execution import Deps, ExecutionContext
from forze.application.execution.registry import OperationRegistry
from forze.base.exceptions import CoreException
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


@attrs.define(slots=True)
class _RegistryHandler(Handler[_In, _Out]):
    async def __call__(self, _args: _In) -> _Out:
        return _Out(ok=True)


def _registry_handler_factory(_ctx: object) -> _RegistryHandler:
    return _RegistryHandler()


def test_register_functions_with_spec_operation_and_registry() -> None:
    client = InngestClient(app_id="forze-registry-op-test")
    spec = DurableFunctionSpec(
        name="cron-scan",
        operation="jobs.scan",
        run=DurableFunctionInvokeSpec(args_type=_In, return_type=_Out),
        triggers=(DurableFunctionCronTrigger(expression="0 */3 * * *"),),
    )

    frozen = (
        OperationRegistry(handlers={"jobs.scan": _registry_handler_factory})
        .bind("jobs.scan")
        .finish()
        .freeze()
    )

    binding = InngestFunctionBinding(spec=spec)

    functions = register_functions(
        client,
        [binding],
        ctx_factory=lambda: ExecutionContext(deps=Deps.plain({})),
        registry=frozen,
    )

    assert len(functions) == 1


def test_binding_rejects_operation_and_handler_factory() -> None:
    spec = DurableFunctionSpec(
        name="conflict",
        operation="jobs.scan",
        run=DurableFunctionInvokeSpec(args_type=_In, return_type=_Out),
        triggers=(DurableFunctionEventTrigger(event="app/test"),),
    )

    with pytest.raises(CoreException, match="handler_factory"):
        InngestFunctionBinding(
            spec=spec,
            handler_factory=lambda _ctx: (lambda _a: _Out()),
        )


def test_binding_requires_handler_factory_without_operation() -> None:
    spec = DurableFunctionSpec(
        name="custom",
        run=DurableFunctionInvokeSpec(args_type=_In, return_type=_Out),
        triggers=(DurableFunctionEventTrigger(event="app/test"),),
    )

    with pytest.raises(CoreException, match="handler_factory"):
        InngestFunctionBinding(spec=spec)
