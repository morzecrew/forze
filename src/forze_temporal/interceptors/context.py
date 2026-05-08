from forze_temporal._compat import require_temporal

require_temporal()

# ....................... #

from typing import Any, Awaitable, Callable, Mapping, Protocol

import attrs
from temporalio.api.common.v1 import Payload
from temporalio.client import Interceptor as ClientInterceptor
from temporalio.client import (
    OutboundInterceptor,
    QueryWorkflowInput,
    SignalWorkflowInput,
    StartWorkflowInput,
    StartWorkflowUpdateInput,
    StartWorkflowUpdateWithStartInput,
)
from temporalio.worker import (
    ActivityInboundInterceptor,
    ContinueAsNewInput,
    ExecuteActivityInput,
    ExecuteWorkflowInput,
    HandleQueryInput,
    HandleSignalInput,
    HandleUpdateInput,
    SignalChildWorkflowInput,
    SignalExternalWorkflowInput,
    StartActivityInput,
    StartChildWorkflowInput,
    StartLocalActivityInput,
    WorkflowInboundInterceptor,
    WorkflowInterceptorClassInput,
    WorkflowOutboundInterceptor,
)
from temporalio.worker import Interceptor as WorkerInterceptor

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import CallContext, ExecutionContext

from .codecs import TemporalContextBinder, TemporalContextCodec

# ----------------------- #


@attrs.define(slots=True)
class ExecutionContextInterceptor(ClientInterceptor, WorkerInterceptor):
    ctx_dep: Callable[[], ExecutionContext] = attrs.field(
        kw_only=True,
        on_setattr=attrs.setters.frozen,
    )
    """The dependency to resolve the execution context."""

    # ....................... #

    def intercept_client(self, next: OutboundInterceptor) -> OutboundInterceptor:
        """Intercept the client outbound interceptor."""

        return ClientContextOutboundInterceptor(next=next, ctx_dep=self.ctx_dep)

    # ....................... #

    def intercept_activity(
        self,
        next: ActivityInboundInterceptor,
    ) -> ActivityInboundInterceptor:
        """Intercept the activity inbound interceptor."""

        return ActivityContextInboundInterceptor(next=next, ctx_dep=self.ctx_dep)

    # ....................... #

    def workflow_interceptor_class(
        self,
        input: WorkflowInterceptorClassInput,
    ) -> type[WorkflowInboundInterceptor]:
        """Intercept the workflow inbound interceptor class."""

        outer_ctx_dep = self.ctx_dep

        @attrs.define(slots=True, frozen=True)
        class BoundWorkflowInterceptor(WorkflowContextInboundInterceptor):
            ctx_dep: Callable[[], ExecutionContext] = attrs.field(
                default=outer_ctx_dep, init=False
            )

        return BoundWorkflowInterceptor


# ....................... #


class InputWithHeaders(Protocol):
    headers: Mapping[str, Payload]


# ....................... #


@attrs.define(slots=True, frozen=True)
class BaseContextInterceptor:
    ctx_dep: Callable[[], ExecutionContext] = attrs.field(kw_only=True)

    # Non initable fields
    codec: TemporalContextCodec = attrs.field(
        factory=TemporalContextCodec,
        init=False,
    )

    binder: TemporalContextBinder = attrs.field(
        factory=TemporalContextBinder,
        init=False,
    )

    # ....................... #

    def inject_headers(self, input: InputWithHeaders) -> None:
        ctx = self.ctx_dep()
        context_headers = self.codec.encode(
            call=ctx.get_call_ctx(),
            identity=ctx.get_authn_identity(),
        )
        headers = dict(input.headers or {})

        for k, v in context_headers.items():
            headers[k] = v

        input.headers = headers

    # ....................... #

    def bind_headers(
        self,
        headers: Mapping[str, Payload],
    ) -> tuple[CallContext, AuthnIdentity | None, TenantIdentity | None]:
        decoded = self.codec.decode(headers)

        return self.binder.bind(decoded)

    # ....................... #

    async def bind_and_call(
        self,
        headers: Mapping[str, Payload],
        next: Callable[[], Awaitable[Any]],
    ) -> Any:
        ctx = self.ctx_dep()
        call_ctx, identity, tenant = self.bind_headers(headers)

        with ctx.bind_call(call=call_ctx, identity=identity, tenancy=tenant):
            return await next()

    # ....................... #

    def bind_and_call_sync(
        self,
        headers: Mapping[str, Payload],
        next: Callable[[], Any],
    ) -> Any:
        ctx = self.ctx_dep()
        call_ctx, identity, tenant = self.bind_headers(headers)

        with ctx.bind_call(call=call_ctx, identity=identity, tenancy=tenant):
            return next()


# ....................... #


@attrs.define(slots=True, frozen=True)
class ClientContextOutboundInterceptor(OutboundInterceptor, BaseContextInterceptor):
    next: OutboundInterceptor

    # ....................... #

    async def start_workflow(self, input: StartWorkflowInput) -> Any:
        self.inject_headers(input)

        return await self.next.start_workflow(input)

    # ....................... #

    async def signal_workflow(self, input: SignalWorkflowInput) -> Any:
        self.inject_headers(input)

        return await self.next.signal_workflow(input)

    # ....................... #

    async def query_workflow(self, input: QueryWorkflowInput) -> Any:
        self.inject_headers(input)

        return await self.next.query_workflow(input)

    # ....................... #

    async def start_workflow_update(self, input: StartWorkflowUpdateInput) -> Any:
        self.inject_headers(input)

        return await self.next.start_workflow_update(input)

    # ....................... #

    async def start_update_with_start_workflow(
        self,
        input: StartWorkflowUpdateWithStartInput,
    ) -> Any:
        self.inject_headers(input.start_workflow_input)
        self.inject_headers(input.update_workflow_input)

        return await self.next.start_update_with_start_workflow(input)


# ....................... #


@attrs.define(slots=True, frozen=True)
class WorkflowContextInboundInterceptor(
    WorkflowInboundInterceptor,
    BaseContextInterceptor,
):
    next: WorkflowInboundInterceptor

    # ....................... #

    def init(self, outbound: WorkflowOutboundInterceptor) -> None:
        wrapped = WorkflowContextOutboundInterceptor(
            next=outbound,
            ctx_dep=self.ctx_dep,
        )

        return self.next.init(wrapped)

    # ....................... #

    async def execute_workflow(self, input: ExecuteWorkflowInput) -> Any:
        return await self.bind_and_call(
            dict(input.headers),
            lambda: self.next.execute_workflow(input),
        )

    # ....................... #

    async def handle_signal(self, input: HandleSignalInput) -> Any:
        return await self.bind_and_call(
            dict(input.headers),
            lambda: self.next.handle_signal(input),
        )

    # ....................... #

    async def handle_query(self, input: HandleQueryInput) -> Any:
        return await self.bind_and_call(
            dict(input.headers),
            lambda: self.next.handle_query(input),
        )

    # ....................... #

    async def handle_update_handler(self, input: HandleUpdateInput) -> Any:
        return await self.bind_and_call(
            dict(input.headers),
            lambda: self.next.handle_update_handler(input),
        )

    # ....................... #

    def handle_update_validator(self, input: HandleUpdateInput) -> None:
        return self.bind_and_call_sync(
            dict(input.headers),
            lambda: self.next.handle_update_validator(input),
        )


# ....................... #


@attrs.define(slots=True, frozen=True)
class WorkflowContextOutboundInterceptor(
    WorkflowOutboundInterceptor,
    BaseContextInterceptor,
):
    next: WorkflowOutboundInterceptor

    # ....................... #

    def start_activity(self, input: StartActivityInput) -> Any:
        self.inject_headers(input)

        return self.next.start_activity(input)

    # ....................... #

    def start_local_activity(self, input: StartLocalActivityInput) -> Any:
        self.inject_headers(input)

        return self.next.start_local_activity(input)

    # ....................... #

    async def start_child_workflow(self, input: StartChildWorkflowInput) -> Any:
        self.inject_headers(input)

        return await self.next.start_child_workflow(input)

    # ....................... #

    async def signal_child_workflow(self, input: SignalChildWorkflowInput) -> Any:
        self.inject_headers(input)

        return await self.next.signal_child_workflow(input)

    # ....................... #

    async def signal_external_workflow(
        self,
        input: SignalExternalWorkflowInput,
    ) -> Any:
        self.inject_headers(input)

        return await self.next.signal_external_workflow(input)

    # ....................... #

    def continue_as_new(self, input: ContinueAsNewInput) -> Any:
        self.inject_headers(input)

        return self.next.continue_as_new(input)


# ....................... #


@attrs.define(slots=True, frozen=True)
class ActivityContextInboundInterceptor(
    ActivityInboundInterceptor,
    BaseContextInterceptor,
):
    next: ActivityInboundInterceptor

    # ....................... #

    async def execute_activity(self, input: ExecuteActivityInput) -> Any:
        return await self.bind_and_call(
            dict(input.headers),
            lambda: self.next.execute_activity(input),
        )
