"""Behavioral tests for declarative port-level resilience policy wrapping."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from datetime import timedelta
from typing import Any

import pytest
from pydantic import BaseModel

from forze.application.contracts.document import DocumentQueryDepKey, DocumentSpec
from forze.application.contracts.interception import (
    PortCall,
    PortNext,
    StreamPortNext,
)
from forze.application.contracts.queue import (
    QueueCommandDepKey,
    QueueQueryDepKey,
    QueueSpec,
)
from forze.application.contracts.resilience import (
    CircuitBreakerStrategy,
    PortPolicy,
    RateLimitStrategy,
    ResilienceExecutorDepKey,
    ResiliencePolicy,
    ResiliencePortPoliciesDepKey,
)
from forze.application.execution import Deps, DepsRegistry, ExecutionContext
from forze.application.execution.interception import wrap_intercepted
from forze.application.execution.resilience import InProcessResilienceExecutor
from forze.application.execution.resilience.port_policy import (
    ResiliencePortProxy,
    wrap_port_policy,
)
from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze.base.serialization import PydanticModelCodec
from forze.domain.models import CreateDocumentCmd, Document, ReadDocument
from forze_mock import MockDepsModule, MockState

# ----------------------- #


async def _no_sleep(_delay: float) -> None:
    return None


class _Clock:
    """Manually advanced monotonic clock (frozen unless moved)."""

    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


class _Msg(BaseModel):
    text: str


def _policies(permits: int = 1) -> dict[str, ResiliencePolicy]:
    return {
        "port_rl": ResiliencePolicy(
            name="port_rl",
            strategies=(RateLimitStrategy(permits=permits, per=timedelta(seconds=1)),),
        ),
        "port_cb": ResiliencePolicy(
            name="port_cb",
            strategies=(
                CircuitBreakerStrategy(
                    failure_ratio=1.0,
                    sampling_window=timedelta(seconds=100),
                    min_throughput=1,
                    break_duration=timedelta(seconds=10),
                    half_open_max_calls=1,
                ),
            ),
        ),
    }


def _build_ctx(
    *,
    port_policies: tuple[PortPolicy, ...] = (),
    permits: int = 1,
    clock: _Clock | None = None,
    tracing: bool = False,
) -> ExecutionContext:
    """MockDepsModule ports + a real executor + the port-policy table."""

    base = MockDepsModule(state=MockState())()
    plain: dict[Any, Any] = dict(base.plain_deps)
    plain[ResilienceExecutorDepKey] = InProcessResilienceExecutor(
        policies=_policies(permits),
        sleep=_no_sleep,
        clock=clock or _Clock(),
    )

    if port_policies:
        plain[ResiliencePortPoliciesDepKey] = {pp.key: pp for pp in port_policies}

    registry = DepsRegistry.from_deps(Deps.plain(plain))

    if tracing:
        registry = registry.with_tracing(runtime=True)

    return ExecutionContext(deps=registry.freeze().resolve())


def _doc_spec(name: str = "alpha") -> DocumentSpec:
    return DocumentSpec(
        name=name,
        read=ReadDocument,
        write={
            "domain": Document,
            "create_cmd": CreateDocumentCmd,
            "update_cmd": CreateDocumentCmd,
        },
    )


def _queue_spec(name: str = "jobs") -> QueueSpec[_Msg]:
    return QueueSpec(name=name, codec=PydanticModelCodec(model_type=_Msg))


def _assert_throttled(ei: pytest.ExceptionInfo[CoreException]) -> None:
    assert ei.value.kind is ExceptionKind.THROTTLED
    assert ei.value.code == "rate_limited"


# ----------------------- #


class _StubPort:
    """Port double covering every attribute shape the proxy must classify."""

    tag = "stub"  # non-callable attribute

    def __init__(self) -> None:
        self.calls = 0

    async def fetch(self, value: int) -> int:
        self.calls += 1
        return value * 2

    async def push(self, value: int) -> int:
        self.calls += 1
        return value

    def plain(self) -> str:
        return "sync"

    async def stream(self, n: int) -> AsyncGenerator[int]:
        for i in range(n):
            yield i

    async def broken_stream(self, n: int) -> AsyncGenerator[int]:
        for i in range(n):
            yield i
        raise exc.infrastructure("stream died mid-iteration")

    async def _internal(self) -> str:
        return "private"


class _PassthroughStreamInterceptor:
    """Interceptor double standing in for a real interception layer inside the policy."""

    async def around(self, call: PortCall, nxt: PortNext) -> Any:
        return await nxt(call)

    async def around_stream(
        self, call: PortCall, nxt: StreamPortNext
    ) -> AsyncGenerator[Any]:
        async for item in nxt(call):
            yield item


# ....................... #


class TestProxyDirect:
    """Proxy-level classification with a stub port."""

    def _wrapped(self, *, methods: tuple[str, ...] | None = None) -> _StubPort:
        ctx = _build_ctx()
        stub = _StubPort()
        return wrap_port_policy(
            stub,
            ctx=ctx,
            port_policy=PortPolicy(
                key=DocumentQueryDepKey,
                policy="port_rl",
                methods=methods,
            ),
            resolved_route=None,
        )

    async def test_coroutine_methods_throttle_after_burst(self) -> None:
        port = self._wrapped()

        assert await port.fetch(2) == 4

        with pytest.raises(CoreException) as ei:
            await port.fetch(2)

        _assert_throttled(ei)

    async def test_all_coroutine_methods_share_the_policy_bucket(self) -> None:
        # methods=None wraps every public coroutine method under one policy:
        # fetch consumes the token, push gets throttled.
        port = self._wrapped()

        assert await port.fetch(2) == 4

        with pytest.raises(CoreException):
            await port.push(1)

    async def test_async_generator_method_not_rate_limited(self) -> None:
        port = self._wrapped()

        # Exhaust the bucket, then stream anyway: streams get only the
        # breaker from a policy — the rate limit never gates them.
        assert await port.fetch(2) == 4
        assert [i async for i in port.stream(3)] == [0, 1, 2]

    async def test_sync_method_and_attributes_pass_through(self) -> None:
        port = self._wrapped()

        assert await port.fetch(2) == 4  # exhaust the bucket
        assert port.plain() == "sync"
        assert port.tag == "stub"

    async def test_private_methods_pass_through(self) -> None:
        port = self._wrapped()

        assert await port.fetch(2) == 4  # exhaust the bucket
        assert await port._internal() == "private"

    async def test_explicit_methods_narrow_wrapping(self) -> None:
        port = self._wrapped(methods=("fetch",))

        assert await port.fetch(2) == 4

        with pytest.raises(CoreException):
            await port.fetch(2)

        # push is outside the tuple: unwrapped, unthrottled.
        assert await port.push(1) == 1


# ....................... #


class TestStreamBreaker:
    """Streams share the port's breaker: gated at acquisition, outcomes recorded."""

    def _wrapped(self, *, methods: tuple[str, ...] | None = None) -> _StubPort:
        ctx = _build_ctx()
        stub = _StubPort()
        return wrap_port_policy(
            stub,
            ctx=ctx,
            port_policy=PortPolicy(
                key=DocumentQueryDepKey,
                policy="port_cb",
                methods=methods,
            ),
            resolved_route=None,
        )

    async def test_mid_stream_failure_opens_breaker_for_unary_sibling(self) -> None:
        port = self._wrapped()

        with pytest.raises(CoreException) as ei:
            async for _ in port.broken_stream(2):
                pass

        assert ei.value.kind is ExceptionKind.INFRASTRUCTURE

        # The stream failure fed the shared breaker: the unary sibling under
        # the same (policy, route) is fast-failed without reaching the port.
        calls_before = port.calls

        with pytest.raises(CoreException, match="Circuit breaker open"):
            await port.fetch(1)

        assert port.calls == calls_before

    async def test_open_breaker_rejects_new_stream(self) -> None:
        port = self._wrapped()

        with pytest.raises(CoreException):
            async for _ in port.broken_stream(0):
                pass

        # Opening a fresh stream against the known-dead backend is rejected
        # exactly like a unary call.
        with pytest.raises(CoreException, match="Circuit breaker open"):
            async for _ in port.stream(3):
                pass

    async def test_early_close_is_not_a_failure(self) -> None:
        port = self._wrapped()

        agen = port.stream(3)
        assert await anext(agen) == 0
        await agen.aclose()

        # Abandoning the stream cleanly recorded a success: unary calls under
        # the same breaker are still admitted.
        assert await port.fetch(2) == 4

    async def test_methods_narrowing_applies_to_streams(self) -> None:
        port = self._wrapped(methods=("fetch",))

        # broken_stream is outside the tuple: unwrapped, so its failure never
        # reaches the breaker and fetch stays admitted.
        with pytest.raises(CoreException):
            async for _ in port.broken_stream(0):
                pass

        assert await port.fetch(2) == 4

    def _wrapped_over_interception(self) -> _StubPort:
        # Mirror the real resolution order: interception innermost, the
        # resilience policy outermost.
        ctx = _build_ctx()
        intercepted = wrap_intercepted(
            _StubPort(),
            interceptors=(_PassthroughStreamInterceptor(),),
            surface=None,
            route=None,
        )
        return wrap_port_policy(
            intercepted,
            ctx=ctx,
            port_policy=PortPolicy(key=DocumentQueryDepKey, policy="port_cb"),
            resolved_route=None,
        )

    async def test_mid_stream_failure_recorded_through_interception(self) -> None:
        port = self._wrapped_over_interception()

        with pytest.raises(CoreException):
            async for _ in port.broken_stream(1):
                pass

        # The failure surfaced through the interception wrapper still lands in
        # the resilience layer's breaker.
        with pytest.raises(CoreException, match="Circuit breaker open"):
            await port.fetch(1)

    async def test_early_close_through_interception_is_not_a_failure(self) -> None:
        port = self._wrapped_over_interception()

        agen = port.stream(3)
        assert await anext(agen) == 0
        await agen.aclose()

        assert await port.fetch(2) == 4


# ....................... #


class TestDocumentFamily:
    """Wrapping through ctx resolution for the document query family."""

    async def test_query_port_throttles_after_burst(self) -> None:
        ctx = _build_ctx(
            port_policies=(PortPolicy(key=DocumentQueryDepKey, policy="port_rl"),),
        )
        port = ctx.document.query(_doc_spec())

        assert list(await port.get_many([])) == []

        with pytest.raises(CoreException) as ei:
            await port.get_many([])

        _assert_throttled(ei)

    async def test_route_defaults_to_spec_name(self) -> None:
        # No explicit PortPolicy.route: state is keyed by the route the port
        # resolved under (spec.name), so distinct specs get distinct buckets.
        ctx = _build_ctx(
            port_policies=(PortPolicy(key=DocumentQueryDepKey, policy="port_rl"),),
        )

        alpha = ctx.document.query(_doc_spec("alpha"))
        assert list(await alpha.get_many([])) == []

        with pytest.raises(CoreException) as ei:
            await alpha.get_many([])

        assert ei.value.details == {"policy": "port_rl", "route": "alpha"}

        beta = ctx.document.query(_doc_spec("beta"))
        assert list(await beta.get_many([])) == []

    async def test_explicit_route_shares_one_bucket(self) -> None:
        ctx = _build_ctx(
            port_policies=(
                PortPolicy(
                    key=DocumentQueryDepKey,
                    policy="port_rl",
                    route="shared",
                ),
            ),
        )

        alpha = ctx.document.query(_doc_spec("alpha"))
        assert list(await alpha.get_many([])) == []

        # A different spec route drains the same explicit-route bucket.
        beta = ctx.document.query(_doc_spec("beta"))

        with pytest.raises(CoreException) as ei:
            await beta.get_many([])

        assert ei.value.details == {"policy": "port_rl", "route": "shared"}

    async def test_wrapped_instance_is_cached_per_scope(self) -> None:
        ctx = _build_ctx(
            port_policies=(PortPolicy(key=DocumentQueryDepKey, policy="port_rl"),),
        )

        # Structurally equal specs hit the spec-equality port cache: the
        # *wrapped* instance is what got cached.
        first = ctx.document.query(_doc_spec())
        second = ctx.document.query(_doc_spec())

        assert isinstance(first, ResiliencePortProxy)
        assert first is second

        # And the bucket persists across resolutions (same proxy, same state).
        assert list(await first.get_many([])) == []

        with pytest.raises(CoreException):
            await second.get_many([])

    async def test_unlisted_dep_key_not_wrapped(self) -> None:
        ctx = _build_ctx(
            port_policies=(PortPolicy(key=QueueCommandDepKey, policy="port_rl"),),
        )
        port = ctx.document.query(_doc_spec())

        assert not isinstance(port, ResiliencePortProxy)


# ....................... #


class TestQueueFamily:
    """Wrapping through ctx resolution for the queue family (second family)."""

    async def test_command_port_throttles_after_burst(self) -> None:
        ctx = _build_ctx(
            port_policies=(PortPolicy(key=QueueCommandDepKey, policy="port_rl"),),
        )
        spec = _queue_spec()
        port = ctx.deps.resolve_configurable(
            ctx,
            QueueCommandDepKey,
            spec,
            route=spec.name,
        )

        assert await port.enqueue("jobs", _Msg(text="one")) is not None

        with pytest.raises(CoreException) as ei:
            await port.enqueue("jobs", _Msg(text="two"))

        _assert_throttled(ei)
        assert ei.value.details == {"policy": "port_rl", "route": "jobs"}

    async def test_consume_async_generator_not_rate_limited_and_works(self) -> None:
        ctx = _build_ctx(
            port_policies=(PortPolicy(key=QueueQueryDepKey, policy="port_rl"),),
            permits=1,
        )
        spec = _queue_spec()

        # Enqueue through the unwrapped command port.
        command = ctx.deps.resolve_configurable(
            ctx,
            QueueCommandDepKey,
            spec,
            route=spec.name,
        )
        await command.enqueue("jobs", _Msg(text="hello"))

        query = ctx.deps.resolve_configurable(
            ctx,
            QueueQueryDepKey,
            spec,
            route=spec.name,
        )

        # Drain the bucket via the wrapped coroutine method.
        await query.ack("jobs", [])

        with pytest.raises(CoreException):
            await query.ack("jobs", [])

        # consume is an async-generator method: wrapped for the breaker only,
        # so the exhausted rate-limit bucket does not gate it.
        agen = query.consume("jobs", timeout=timedelta(milliseconds=50))
        received = None

        async for message in agen:
            received = message
            break

        await agen.aclose()

        assert received is not None
        assert received.payload.text == "hello"

    async def test_methods_narrowing_through_ctx(self) -> None:
        ctx = _build_ctx(
            port_policies=(
                PortPolicy(
                    key=QueueCommandDepKey,
                    policy="port_rl",
                    methods=("enqueue",),
                ),
            ),
        )
        spec = _queue_spec()
        port = ctx.deps.resolve_configurable(
            ctx,
            QueueCommandDepKey,
            spec,
            route=spec.name,
        )

        assert await port.enqueue("jobs", _Msg(text="one")) is not None

        with pytest.raises(CoreException):
            await port.enqueue("jobs", _Msg(text="two"))

        # enqueue_many is outside the tuple: unwrapped, unthrottled.
        assert await port.enqueue_many("jobs", [_Msg(text="three")]) is not None


# ....................... #


class TestProxyTracingComposition:
    """Policy wraps OUTSIDE tracing: traces record only real invocations."""

    async def test_rejection_emits_resilience_event_not_port_event(self) -> None:
        ctx = _build_ctx(
            port_policies=(PortPolicy(key=DocumentQueryDepKey, policy="port_rl"),),
            tracing=True,
        )
        port = ctx.document.query(_doc_spec())

        assert list(await port.get_many([])) == []

        with pytest.raises(CoreException):
            await port.get_many([])

        trace = ctx.deps.runtime_trace()
        assert trace is not None

        port_calls = [e for e in trace.events if e.op == "get_many"]
        resilience_ops = {e.op for e in trace.events if e.domain == "resilience"}

        # One successful call -> exactly one traced port invocation; the
        # rejected call records no phantom port event, only the executor's
        # own rejection event.
        assert len(port_calls) == 1
        assert "rate_limit_reject" in resilience_ops
