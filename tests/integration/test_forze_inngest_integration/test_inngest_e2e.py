"""End-to-end integration tests for ``forze_inngest`` against the Dev Server."""

from __future__ import annotations

import inngest
import pytest
from pydantic import BaseModel

from forze.application.contracts.durable.function import (
    DurableFunctionEventCommandDepKey,
    DurableFunctionEventSpec,
    DurableFunctionEventTrigger,
    DurableFunctionInvokeSpec,
    DurableFunctionSpec,
    DurableFunctionStepDepKey,
)
from forze.application.execution import ExecutionContext, InvocationMetadata
from forze.base.primitives import uuid7
from forze.base.serialization import PydanticRecordMappingCodec
from forze_inngest import InngestFunctionBinding

from ._harness import start_forze_inngest_app, wait_for_outcome
from .inngest_dev_server import InngestDevTarget

# ----------------------- #

_EVENT_NAME = "it/forze.inngest.e2e"


class _Payload(BaseModel):
    value: str


class _FnIn(BaseModel):
    value: str


class _FnOut(BaseModel):
    ok: bool = True


class _CorrelationOut(BaseModel):
    correlation_id: str


# ....................... #


def _event_spec() -> DurableFunctionEventSpec[_Payload]:
    return DurableFunctionEventSpec(
        name=_EVENT_NAME,
        codec=PydanticRecordMappingCodec(model_type=_Payload),
    )


def _function_spec() -> DurableFunctionSpec[_FnIn, _FnOut]:
    return DurableFunctionSpec(
        name="it-forze-inngest-fn",
        run=DurableFunctionInvokeSpec(args_type=_FnIn, return_type=_FnOut),
        triggers=(DurableFunctionEventTrigger(event=_EVENT_NAME),),
    )


# ....................... #


@pytest.mark.integration
@pytest.mark.asyncio
async def test_registered_function_runs_on_event(
    inngest_dev_env: InngestDevTarget,
) -> None:
    """``register_functions`` + dev server invoke a Forze handler."""

    outcomes: list[_FnIn] = []
    spec = _function_spec()

    async def _handler(args: _FnIn) -> _FnOut:
        outcomes.append(args)
        return _FnOut(ok=True)

    binding = InngestFunctionBinding(
        spec=spec,
        handler_factory=lambda _ctx: _handler,
    )

    harness = start_forze_inngest_app(
        inngest_dev_env,
        bindings=[binding],
        app_id="forze-it-register",
    )

    try:
        await harness.client.send(
            inngest.Event(name=_EVENT_NAME, data={"value": "from-event"}),
        )
        result = await wait_for_outcome(outcomes)

    finally:
        harness.stop()

    assert result.value == "from-event"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_inngest_event_command_adapter_triggers_function(
    inngest_dev_env: InngestDevTarget,
) -> None:
    """``InngestEventCommandAdapter`` (via deps) emits an event that runs the function."""

    outcomes: list[_FnIn] = []
    event_spec = _event_spec()
    fn_spec = _function_spec()

    async def _handler(args: _FnIn) -> _FnOut:
        outcomes.append(args)
        return _FnOut()

    binding = InngestFunctionBinding(
        spec=fn_spec,
        handler_factory=lambda _ctx: _handler,
    )

    harness = start_forze_inngest_app(
        inngest_dev_env,
        bindings=[binding],
        events={event_spec.name: {}},
        app_id="forze-it-emit",
    )

    try:
        ctx = harness.ctx_factory()
        metadata = InvocationMetadata(
            execution_id=uuid7(),
            correlation_id=uuid7(),
        )

        with ctx.inv_ctx.bind_metadata(metadata=metadata):
            events = ctx.deps.resolve_configurable(
                ctx,
                DurableFunctionEventCommandDepKey,
                event_spec,
                route=event_spec.name,
            )
            await events.send(_Payload(value="via-adapter"))

        result = await wait_for_outcome(outcomes)

    finally:
        harness.stop()

    assert result.value == "via-adapter"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_execution_context_envelope_restored_in_function(
    inngest_dev_env: InngestDevTarget,
) -> None:
    """``_forze`` envelope on event data restores invocation metadata in the worker."""

    outcomes: list[_CorrelationOut] = []
    correlation = uuid7()
    event_spec = _event_spec()
    fn_spec = DurableFunctionSpec(
        name="it-forze-inngest-envelope",
        run=DurableFunctionInvokeSpec(
            args_type=_FnIn,
            return_type=_CorrelationOut,
        ),
        triggers=(DurableFunctionEventTrigger(event=_EVENT_NAME),),
    )

    async def _handler(ctx: ExecutionContext, args: _FnIn) -> _CorrelationOut:
        meta = ctx.inv_ctx.get_metadata()
        assert meta is not None
        outcomes.append(_CorrelationOut(correlation_id=str(meta.correlation_id)))
        return outcomes[-1]

    def _factory(ctx: ExecutionContext):
        async def _run(args: _FnIn) -> _CorrelationOut:
            return await _handler(ctx, args)

        return _run

    binding = InngestFunctionBinding(
        spec=fn_spec,
        handler_factory=_factory,
    )

    harness = start_forze_inngest_app(
        inngest_dev_env,
        bindings=[binding],
        events={event_spec.name: {}},
        app_id="forze-it-envelope",
    )

    try:
        ctx = harness.ctx_factory()
        metadata = InvocationMetadata(
            execution_id=uuid7(),
            correlation_id=correlation,
        )

        with ctx.inv_ctx.bind_metadata(metadata=metadata):
            events = ctx.deps.resolve_configurable(
                ctx,
                DurableFunctionEventCommandDepKey,
                event_spec,
                route=event_spec.name,
            )
            await events.send(_Payload(value="envelope"))

        out = await wait_for_outcome(outcomes)

    finally:
        harness.stop()

    assert out.correlation_id == str(correlation)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_durable_function_step_port_inside_handler(
    inngest_dev_env: InngestDevTarget,
) -> None:
    """Handlers resolve ``DurableFunctionStepDepKey`` and run memoized substeps."""

    outcomes: list[str] = []
    spec = DurableFunctionSpec(
        name="it-forze-inngest-step",
        run=DurableFunctionInvokeSpec(args_type=_FnIn, return_type=_FnOut),
        triggers=(DurableFunctionEventTrigger(event=_EVENT_NAME),),
    )

    async def _handler(ctx: ExecutionContext, args: _FnIn) -> _FnOut:
        step = ctx.deps.provide(DurableFunctionStepDepKey)(ctx)

        async def _substep() -> str:
            return f"step:{args.value}"

        label = await step.run("substep", _substep)
        outcomes.append(label)
        return _FnOut(ok=True)

    def _factory(ctx: ExecutionContext):
        async def _run(args: _FnIn) -> _FnOut:
            return await _handler(ctx, args)

        return _run

    binding = InngestFunctionBinding(
        spec=spec,
        handler_factory=_factory,
    )

    harness = start_forze_inngest_app(
        inngest_dev_env,
        bindings=[binding],
        app_id="forze-it-step",
    )

    try:
        await harness.client.send(
            inngest.Event(name=_EVENT_NAME, data={"value": "memo"}),
        )
        label = await wait_for_outcome(outcomes)

    finally:
        harness.stop()

    assert label == "step:memo"
