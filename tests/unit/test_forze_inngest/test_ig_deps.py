from unittest.mock import Mock

from forze.application.contracts.durable.function import (
    DurableFunctionEventCommandDepKey,
    DurableFunctionStepDepKey,
)
from forze.application.execution import Deps
from forze_inngest.adapters import InngestEventCommandAdapter, InngestStepAdapter
from forze_inngest.execution.deps import InngestClientDepKey, InngestDepsModule
from forze_inngest.execution.deps.configs import InngestEventConfig
from forze_inngest.execution.deps.deps import ConfigurableInngestEventCommand
from forze_inngest.kernel.platform import InngestClientPort


def test_inngest_deps_module_registers_keys() -> None:
    client = Mock(spec=InngestClientPort)
    module = InngestDepsModule(
        client=client,
        events={
            "app/test": InngestEventConfig(include_execution_context=False),
        },
    )

    deps = module()

    assert deps.exists(InngestClientDepKey)
    assert deps.exists(DurableFunctionEventCommandDepKey, route="app/test")
    assert deps.exists(DurableFunctionStepDepKey)


def test_configurable_event_command_builds_adapter() -> None:
    client = Mock(spec=InngestClientPort)
    deps = Deps.plain({InngestClientDepKey: client})
    from forze.application.execution import ExecutionContext

    ctx = ExecutionContext(deps=deps)
    from forze.application.contracts.durable.function import DurableFunctionEventSpec
    from forze.base.serialization import PydanticRecordMappingCodec
    from pydantic import BaseModel

    class _Payload(BaseModel):
        n: int

    spec = DurableFunctionEventSpec(
        name="app/test",
        codec=PydanticRecordMappingCodec(model_type=_Payload),
    )

    factory = ConfigurableInngestEventCommand(
        config=InngestEventConfig(include_execution_context=False),
    )
    adapter = factory(ctx, spec)

    assert isinstance(adapter, InngestEventCommandAdapter)
    assert adapter.client is client

    module_deps = InngestDepsModule(client=client)()
    step_port = module_deps.provide(DurableFunctionStepDepKey)
    step = step_port(ctx)
    assert isinstance(step, InngestStepAdapter)
