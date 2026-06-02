from unittest.mock import Mock

import pytest

from forze.application.contracts.durable.function import (
    DurableFunctionEventCommandDepKey,
    DurableFunctionStepDepKey,
)
from tests.support.execution_context import context_from_deps, context_from_modules, frozen_deps_from_deps
from forze.application.execution import Deps
from forze_inngest.adapters import InngestEventCommandAdapter, InngestStepAdapter
from forze_inngest.execution.deps import (
    ConfigurableInngestEventCommand,
    InngestClientDepKey,
    InngestDepsModule,
    InngestEventConfig,
)
from forze_inngest.kernel.client import InngestClientPort


def test_rejects_mapping_config() -> None:
    with pytest.raises(TypeError, match="InngestEventConfig"):
        ConfigurableInngestEventCommand(config={"include_execution_context": False})


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

    ctx = context_from_deps(deps)
    from forze.application.contracts.durable.function import DurableFunctionEventSpec
    from forze.base.serialization import PydanticModelCodec
    from pydantic import BaseModel

    class _Payload(BaseModel):
        n: int

    spec = DurableFunctionEventSpec(
        name="app/test",
        codec=PydanticModelCodec(model_type=_Payload),
    )

    factory = ConfigurableInngestEventCommand(
        config=InngestEventConfig(include_execution_context=False),
    )
    adapter = factory(ctx, spec)

    assert isinstance(adapter, InngestEventCommandAdapter)
    assert adapter.client is client

    step_ctx = context_from_deps(InngestDepsModule(client=client)())
    step_port = step_ctx.deps.provide(DurableFunctionStepDepKey)
    step = step_port(step_ctx)
    assert isinstance(step, InngestStepAdapter)
