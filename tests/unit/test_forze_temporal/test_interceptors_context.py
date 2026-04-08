"""Unit tests for :mod:`forze_temporal.interceptors.context`."""

from unittest.mock import AsyncMock, MagicMock

import pytest

pytest.importorskip("temporalio")

from temporalio.client import OutboundInterceptor
from temporalio.worker import (
    ActivityInboundInterceptor,
    ExecuteActivityInput,
    WorkflowInboundInterceptor,
    WorkflowInterceptorClassInput,
)

from forze.application.execution import CallContext, ExecutionContext, PrincipalContext
from forze.application.execution.deps import Deps
from forze.base.primitives import uuid7
from forze_temporal.interceptors.codecs import TemporalContextCodec
from forze_temporal.interceptors.context import (
    ActivityContextInboundInterceptor,
    ClientContextOutboundInterceptor,
    ExecutionContextInterceptor,
    WorkflowContextInboundInterceptor,
    WorkflowContextOutboundInterceptor,
)

_EXEC_HEADER = "Forze-Execution-ID"
_CORR_HEADER = "Forze-Correlation-ID"
_TENANT_HEADER = "Forze-Tenant-ID"
_ACTOR_HEADER = "Forze-Actor-ID"


def _exec_ctx() -> ExecutionContext:
    return ExecutionContext(deps=Deps.plain({}))


class TestExecutionContextInterceptorChains:
    """``ExecutionContextInterceptor`` composes client and activity interceptors."""

    @pytest.mark.asyncio
    async def test_intercept_client_returns_outbound_that_injects_headers(self) -> None:
        ctx = _exec_ctx()
        eid = uuid7()
        cid = uuid7()
        tenant = uuid7()
        actor = uuid7()
        with ctx.bind_call(
            call=CallContext(execution_id=eid, correlation_id=cid, causation_id=None),
            principal=PrincipalContext(tenant_id=tenant, actor_id=actor),
        ):
            eci = ExecutionContextInterceptor(ctx_dep=lambda: ctx)
            inner = MagicMock(spec=OutboundInterceptor)
            inner.start_workflow = AsyncMock(return_value="handle")

            chained = eci.intercept_client(inner)
            assert isinstance(chained, ClientContextOutboundInterceptor)

            inp = MagicMock()
            inp.headers = {}

            out = await chained.start_workflow(inp)

            assert out == "handle"
            inner.start_workflow.assert_awaited_once()
            passed = inner.start_workflow.await_args.args[0]
            assert bytes(passed.headers[_EXEC_HEADER].data) == str(eid).encode("utf-8")
            assert bytes(passed.headers[_CORR_HEADER].data) == str(cid).encode("utf-8")
            assert bytes(passed.headers[_TENANT_HEADER].data) == str(tenant).encode("utf-8")
            assert bytes(passed.headers[_ACTOR_HEADER].data) == str(actor).encode("utf-8")

    @pytest.mark.asyncio
    async def test_intercept_activity_binds_call_from_headers(self) -> None:
        ctx = _exec_ctx()
        codec = TemporalContextCodec()
        eid = uuid7()
        cid = uuid7()
        headers = codec.encode(
            call=CallContext(execution_id=eid, correlation_id=cid, causation_id=None),
        )

        captured: list[CallContext | None] = []

        async def inner_exec(_inp: ExecuteActivityInput) -> str:
            captured.append(ctx.get_call_ctx())
            return "ok"

        inner = MagicMock(spec=ActivityInboundInterceptor)
        inner.execute_activity = inner_exec

        eci = ExecutionContextInterceptor(ctx_dep=lambda: ctx)
        act_chain = eci.intercept_activity(inner)
        assert isinstance(act_chain, ActivityContextInboundInterceptor)

        inp = MagicMock(spec=ExecuteActivityInput)
        inp.headers = headers

        result = await act_chain.execute_activity(inp)

        assert result == "ok"
        assert captured[0] is not None
        assert captured[0].correlation_id == cid


class TestWorkflowInterceptorClassFactory:
    """Behavior of :meth:`ExecutionContextInterceptor.workflow_interceptor_class`."""

    def test_returned_class_is_constructible_with_sdk_pattern(self) -> None:
        """Temporal builds inbound interceptors as ``cls(next_inbound)`` (see SDK)."""

        ctx = _exec_ctx()
        eci = ExecutionContextInterceptor(ctx_dep=lambda: ctx)
        wf_input = WorkflowInterceptorClassInput(unsafe_extern_functions={})

        cls_ref = eci.workflow_interceptor_class(wf_input)
        assert cls_ref is not None

        next_inbound = MagicMock(spec=WorkflowInboundInterceptor)
        bound = cls_ref(next_inbound)

        assert isinstance(bound, WorkflowContextInboundInterceptor)
        assert bound.next is next_inbound
        assert bound.ctx_dep is eci.ctx_dep
        assert isinstance(bound.codec, TemporalContextCodec)


class TestWorkflowContextOutboundInterceptor:
    """Outbound workflow interceptor injects context into commands."""

    def test_start_activity_injects_headers(self) -> None:
        ctx = _exec_ctx()
        eid = uuid7()
        cid = uuid7()
        with ctx.bind_call(
            call=CallContext(execution_id=eid, correlation_id=cid, causation_id=None),
        ):
            out_next = MagicMock()
            wco = WorkflowContextOutboundInterceptor(
                next=out_next,
                ctx_dep=lambda: ctx,
            )

            inp = MagicMock()
            inp.headers = {}
            wco.start_activity(inp)

            out_next.start_activity.assert_called_once_with(inp)
            assert bytes(inp.headers[_EXEC_HEADER].data) == str(eid).encode("utf-8")
