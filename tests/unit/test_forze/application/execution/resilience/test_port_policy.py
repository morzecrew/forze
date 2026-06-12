"""Behavioral tests for declarative port-level resilience policy wrapping."""

from __future__ import annotations

from datetime import timedelta
from typing import Any, AsyncGenerator

import pytest
from pydantic import BaseModel

from forze.application.contracts.document import DocumentQueryDepKey, DocumentSpec
from forze.application.contracts.queue import (
    QueueCommandDepKey,
    QueueQueryDepKey,
    QueueSpec,
)
from forze.application.contracts.resilience import (
    PortPolicy,
    RateLimitStrategy,
    ResilienceExecutorDepKey,
    ResiliencePolicy,
    ResiliencePortPoliciesDepKey,
)
from forze.application.execution import Deps, DepsRegistry, ExecutionContext
from forze.application.execution.resilience import InProcessResilienceExecutor
from forze.application.execution.resilience.port_policy import (
    ResiliencePortProxy,
    wrap_port_policy,
)
from forze.base.exceptions import CoreException, ExceptionKind
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
            strategies=(
                RateLimitStrategy(permits=permits, per=timedelta(seconds=1)),
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

    async def _internal(self) -> str:
        return "private"


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

    async def test_async_generator_method_not_wrapped(self) -> None:
        port = self._wrapped()

        # Exhaust the bucket, then stream anyway: async-gen methods are never
        # run through the policy (a stream cannot run inside one run() call).
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

    async def test_consume_async_generator_not_wrapped_and_works(self) -> None:
        ctx = _build_ctx(
            port_policies=(
                PortPolicy(key=QueueQueryDepKey, policy="port_rl"),
            ),
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

        # consume is an async-generator method: never wrapped, still streams.
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
