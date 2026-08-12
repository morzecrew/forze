from forze_temporal._compat import require_temporal

require_temporal()

# ....................... #

import asyncio
from collections.abc import AsyncGenerator, Awaitable, Callable, Mapping
from contextlib import asynccontextmanager, suppress
from datetime import timedelta
from typing import Any, Protocol

import attrs
from temporalio import activity
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
from forze.application.execution import ExecutionContext, InvocationMetadata
from forze.base.primitives import bind_time_source

from ._logger import logger
from .clock import TemporalWorkflowTimeSource
from .codecs import TemporalContextBinder, TemporalContextCodec

# ----------------------- #

_WORKFLOW_CLOCK = TemporalWorkflowTimeSource()
"""Replay-deterministic time source bound for the duration of workflow execution."""

_HEARTBEAT_INTERVAL_DIVISOR = 3
"""Heartbeats go out at ``heartbeat_timeout`` divided by this.

Three, so two consecutive heartbeats can be lost — to a paused event loop, a slow
frontend call, the SDK's own throttling — before the server calls the activity dead.
"""


def heartbeat_interval(
    heartbeat_timeout: timedelta | None,
    *,
    is_local: bool,
) -> float | None:
    """Seconds between auto-heartbeats, or ``None`` when there is nothing to keep alive.

    Two cases produce ``None`` rather than a default interval. An activity with **no**
    ``heartbeat_timeout`` has no liveness deadline to miss, so heartbeating it only adds
    RPCs. A **local** activity has no server-side record to beat against:
    ``activity.heartbeat()`` returns normally there but the beat lands nowhere, and the
    SDK core logs a failed heartbeat for each one — so a pump would be noise standing in
    for a liveness guarantee that does not exist.
    """

    if is_local or heartbeat_timeout is None:
        return None

    seconds = heartbeat_timeout.total_seconds() / _HEARTBEAT_INTERVAL_DIVISOR

    return seconds if seconds > 0 else None


@asynccontextmanager
async def _heartbeating(interval: float) -> AsyncGenerator[None]:
    """Beat every *interval* seconds for the duration of the block.

    Nothing the pump does can change what the activity returns. Its own failures are
    logged and dropped — inside the loop, so one failed beat does not silently end the
    pump, and again while tearing it down, because an exception raised out of this
    ``finally`` would *replace* the activity's result or its exception with a bookkeeping
    error the caller has no way to interpret.
    """

    async def _pump() -> None:
        while True:
            await asyncio.sleep(interval)

            try:
                activity.heartbeat()

            except Exception:
                # A beat can fail on its own (a completed activity's context, a closing
                # loop). Keep beating: the next one may well land, and giving up quietly
                # would let the activity time out for a reason nobody can see.
                logger.warning("Temporal auto-heartbeat failed", exc_info=True)

    task = asyncio.create_task(_pump(), name="temporal-auto-heartbeat")

    try:
        yield

    finally:
        task.cancel()

        # Both, deliberately: ``await task`` re-raises the pump's cancellation (the
        # normal path) and anything else it stored, and neither may outrank what the
        # activity itself returned or raised.
        with suppress(asyncio.CancelledError, Exception):
            await task


@attrs.define(slots=True)
class ExecutionContextInterceptor(ClientInterceptor, WorkerInterceptor):
    ctx_dep: Callable[[], ExecutionContext] = attrs.field(
        kw_only=True,
        on_setattr=attrs.setters.frozen,
    )
    """The dependency to resolve the execution context."""

    auto_heartbeat: bool = attrs.field(
        default=False,
        kw_only=True,
        on_setattr=attrs.setters.frozen,
    )
    """Beat for every activity that declares a ``heartbeat_timeout``, opt-in.

    Covers the common case — "this activity is alive, stop killing it" — for work with no
    incremental state to report. Off by default, and that default is a judgement, not
    caution: an automatic heartbeat says *the process is alive*, which is not the same
    claim as *the activity is making progress*. Turn it on and a wedged activity keeps its
    lease until ``start_to_close`` instead of being rescheduled at ``heartbeat_timeout``.

    An activity that has incremental state to report should call
    ``activity.heartbeat(details)`` itself — details are authoring-surface territory and
    the framework puts nothing between the author and the SDK there.
    """

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

        return ActivityContextInboundInterceptor(
            next=next,
            ctx_dep=self.ctx_dep,
            auto_heartbeat=self.auto_heartbeat,
        )

    # ....................... #

    def workflow_interceptor_class(
        self,
        input: WorkflowInterceptorClassInput,
    ) -> type[WorkflowInboundInterceptor]:
        """Intercept the workflow inbound interceptor class."""

        outer_ctx_dep = self.ctx_dep

        @attrs.define(slots=True, frozen=True)
        class BoundWorkflowInterceptor(WorkflowContextInboundInterceptor):
            ctx_dep: Callable[[], ExecutionContext] = attrs.field(default=outer_ctx_dep, init=False)

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
            metadata=ctx.inv_ctx.get_metadata(),
            authn=ctx.inv_ctx.get_authn(),
            tenant=ctx.inv_ctx.get_tenant(),
        )
        headers = dict(input.headers or {})

        for k, v in context_headers.items():
            headers[k] = v

        input.headers = headers

    # ....................... #

    def bind_headers(
        self,
        headers: Mapping[str, Payload],
    ) -> tuple[InvocationMetadata, AuthnIdentity | None, TenantIdentity | None]:
        decoded = self.codec.decode(headers)

        return self.binder.bind(decoded)

    # ....................... #

    async def bind_and_call(
        self,
        headers: Mapping[str, Payload],
        next: Callable[[], Awaitable[Any]],
    ) -> Any:
        ctx = self.ctx_dep()
        metadata, authn, tenant = self.bind_headers(headers)

        with ctx.inv_ctx.bind(metadata=metadata, authn=authn, tenant=tenant):
            return await next()

    # ....................... #

    def bind_and_call_sync(
        self,
        headers: Mapping[str, Payload],
        next: Callable[[], Any],
    ) -> Any:
        ctx = self.ctx_dep()
        metadata, authn, tenant = self.bind_headers(headers)

        with ctx.inv_ctx.bind(metadata=metadata, authn=authn, tenant=tenant):
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

    async def bind_and_call(
        self,
        headers: Mapping[str, Payload],
        next: Callable[[], Awaitable[Any]],
    ) -> Any:
        # Bind Temporal's replay-safe clock for the workflow scope so all utcnow()/
        # uuid7() reads (domain stamping, adapters) reproduce deterministically.
        with bind_time_source(_WORKFLOW_CLOCK):
            return await super().bind_and_call(headers, next)

    # ....................... #

    def bind_and_call_sync(
        self,
        headers: Mapping[str, Payload],
        next: Callable[[], Any],
    ) -> Any:
        with bind_time_source(_WORKFLOW_CLOCK):
            return super().bind_and_call_sync(headers, next)

    # ....................... #

    async def execute_workflow(self, input: ExecuteWorkflowInput) -> Any:
        return await self.bind_and_call(
            input.headers,
            lambda: self.next.execute_workflow(input),
        )

    # ....................... #

    async def handle_signal(self, input: HandleSignalInput) -> Any:
        return await self.bind_and_call(
            input.headers,
            lambda: self.next.handle_signal(input),
        )

    # ....................... #

    async def handle_query(self, input: HandleQueryInput) -> Any:
        return await self.bind_and_call(
            input.headers,
            lambda: self.next.handle_query(input),
        )

    # ....................... #

    async def handle_update_handler(self, input: HandleUpdateInput) -> Any:
        return await self.bind_and_call(
            input.headers,
            lambda: self.next.handle_update_handler(input),
        )

    # ....................... #

    def handle_update_validator(self, input: HandleUpdateInput) -> None:
        return self.bind_and_call_sync(
            input.headers,
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

    auto_heartbeat: bool = False
    """Whether to keep an activity with a ``heartbeat_timeout`` alive while it runs."""

    # ....................... #

    async def execute_activity(self, input: ExecuteActivityInput) -> Any:
        async def run() -> Any:
            return await self.bind_and_call(
                input.headers,
                lambda: self.next.execute_activity(input),
            )

        if not self.auto_heartbeat:
            return await run()

        info = activity.info()
        interval = heartbeat_interval(info.heartbeat_timeout, is_local=info.is_local)

        if interval is None:
            return await run()

        async with _heartbeating(interval):
            return await run()
