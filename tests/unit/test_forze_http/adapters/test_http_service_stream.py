"""`HttpServiceAdapter.stream`: a streamed call is still a call through the declared service.

The point of streaming through the port rather than a bare client is that nothing about the
governance changes: the same wiring gate, the same request (credential, deadline budget,
trace context, egress tag), the same errors before the first chunk, and the same breaker
under a port policy — with retry kept away from a body the caller has started reading.
"""

import asyncio
from collections.abc import AsyncIterator, Callable
from contextlib import aclosing
from datetime import timedelta
from typing import Any
from uuid import UUID

import httpx
import pytest
from pydantic import BaseModel

from forze.application.contracts.envelope import HTTP_HEADER_DEADLINE_BUDGET
from forze.application.contracts.http import (
    RESPONSE_ERROR_DETAIL,
    HttpOperationSpec,
    HttpServiceDepKey,
    HttpServiceSpec,
)
from forze.application.contracts.resilience import (
    BackoffStrategy,
    CircuitBreakerStrategy,
    PortPolicy,
    ResilienceExecutorDepKey,
    ResiliencePolicy,
    RetryStrategy,
)
from forze.application.contracts.secrets import SecretRef
from forze.application.execution.context import bind_deadline
from forze.application.execution.resilience import InProcessResilienceExecutor
from forze.application.execution.resilience.port_policy import wrap_port_policy
from forze.base.exceptions import CoreException, ExceptionKind
from forze.testing import context_from_deps, context_from_modules
from forze_http import HttpDepsModule
from forze_http.adapters.http_service import HttpServiceAdapter
from forze_http.execution.deps.configs import HttpAuthConfig, HttpServiceConfig
from forze_http.kernel.client import HttpClient, RoutedHttpClient
from forze_http.kernel.client.credentials import credential_auth_headers
from forze_http.kernel.client.routing_credentials import HttpRoutingCredentials

pytestmark = pytest.mark.unit

# ----------------------- #

TENANT_ID = UUID("00000000-0000-0000-0000-000000000001")


class Prompt(BaseModel):
    text: str


class Reply(BaseModel):
    text: str = ""


class ProviderError(BaseModel):
    error: str


def _spec(*, error_type: type[BaseModel] | None = None) -> HttpServiceSpec:
    return HttpServiceSpec(
        name="model",
        operations={
            "complete": HttpOperationSpec(
                name="complete",
                method="POST",
                path="/v1/complete",
                args_type=Prompt,
                return_type=Reply,
                error_type=error_type,
            ),
        },
    )


class _Body(httpx.AsyncByteStream):
    """A response body sent chunk by chunk, that can stall or fail after its chunks and records
    its close."""

    def __init__(
        self,
        *chunks: bytes,
        fail: Exception | None = None,
        stall: asyncio.Event | None = None,
    ) -> None:
        self.chunks = chunks
        self.fail = fail
        self.stall = stall
        self.closed = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for chunk in self.chunks:
            yield chunk

        if self.stall is not None:
            await self.stall.wait()

        if self.fail is not None:
            raise self.fail

    async def aclose(self) -> None:
        self.closed = True


async def _adapter(
    handler: Callable[[httpx.Request], httpx.Response],
    *,
    spec: HttpServiceSpec | None = None,
    **config: Any,
) -> tuple[HttpServiceAdapter, HttpClient]:
    client = HttpClient()
    await client.initialize(transport=httpx.MockTransport(handler))

    adapter = HttpServiceAdapter(
        client=client,
        config=HttpServiceConfig(base_url="https://api.example.com", **config),
        spec=spec or _spec(),
    )

    return adapter, client


# ....................... #


class TestTheRequestIsInvokes:
    async def test_stream_sends_what_invoke_sends(self) -> None:
        seen: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            return httpx.Response(200, json={"text": "hi"})

        adapter, client = await _adapter(
            handler,
            auth=HttpAuthConfig(kind="bearer", token="tok"),
            default_headers={"X-Client": "forze"},
        )

        try:
            await adapter.invoke("complete", Prompt(text="q"))
            chunks = [chunk async for chunk in adapter.stream("complete", Prompt(text="q"))]

        finally:
            await client.close()

        invoked, streamed = seen

        assert b"".join(chunks) == b'{"text":"hi"}'
        assert streamed.method == invoked.method == "POST"
        assert streamed.url == invoked.url
        assert streamed.content == invoked.content
        assert streamed.headers["authorization"] == "Bearer tok"
        assert streamed.headers["x-client"] == "forze"
        assert streamed.extensions["timeout"] == invoked.extensions["timeout"]

    async def test_the_deadline_budget_travels(self) -> None:
        seen: list[str | None] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request.headers.get(HTTP_HEADER_DEADLINE_BUDGET))
            return httpx.Response(200, content=b"x")

        adapter, client = await _adapter(handler)

        try:
            with bind_deadline(5.0):
                [_ async for _ in adapter.stream("complete", Prompt(text="q"))]

        finally:
            await client.close()

        assert seen[0] is not None
        assert 0.0 < float(seen[0]) <= 5.0

    async def test_a_sensitive_route_marks_the_span_and_carries_the_trace(self) -> None:
        from opentelemetry.sdk.trace import TracerProvider

        seen: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            return httpx.Response(200, content=b"x")

        adapter, client = await _adapter(
            handler, egress_sensitive=True, acknowledge_data_egress=True
        )
        tracer = TracerProvider().get_tracer("test")

        try:
            with tracer.start_as_current_span("caller") as span:
                [_ async for _ in adapter.stream("complete", Prompt(text="q"))]

                attributes = dict(span.attributes or {})

        finally:
            await client.close()

        assert attributes.get("forze.egress.sensitive") is True
        assert "traceparent" in seen[0].headers


# ....................... #


class TestTheGateHoldsBeforeAnyRequest:
    async def test_an_undeclared_service_is_refused(self) -> None:
        seen: list[httpx.Request] = []
        client = HttpClient()
        await client.initialize(
            transport=httpx.MockTransport(lambda r: seen.append(r) or httpx.Response(200))
        )
        ctx = context_from_modules(
            HttpDepsModule(
                client=client,
                services={"other": HttpServiceConfig(base_url="https://api.example.com")},
            )
        )

        try:
            with pytest.raises(CoreException):
                [_ async for _ in ctx.http.service(_spec()).stream("complete", Prompt(text="q"))]

        finally:
            await client.close()

        assert seen == []

    def test_an_unacknowledged_sensitive_route_does_not_wire(self) -> None:
        with pytest.raises(CoreException) as raised:
            HttpServiceConfig(base_url="https://api.example.com", egress_sensitive=True)

        assert raised.value.code == "http_egress_unacknowledged"


# ....................... #


class TestErrorsArriveBeforeTheBody:
    @pytest.mark.parametrize("status", [302, 400, 401, 404, 429, 500])
    async def test_a_rejection_raises_what_invoke_raises(self, status: int) -> None:
        adapter, client = await _adapter(lambda _r: httpx.Response(status, content=b"no"))
        received: list[bytes] = []

        try:
            with pytest.raises(CoreException) as by_invoke:
                await adapter.invoke("complete", Prompt(text="q"))

            with pytest.raises(CoreException) as by_stream:
                async for chunk in adapter.stream("complete", Prompt(text="q")):
                    received.append(chunk)

        finally:
            await client.close()

        assert received == []
        assert by_stream.value.kind is by_invoke.value.kind
        assert by_stream.value.code == by_invoke.value.code
        assert by_stream.value.summary == by_invoke.value.summary

    async def test_a_declared_error_body_reaches_details(self) -> None:
        adapter, client = await _adapter(
            lambda _r: httpx.Response(400, json={"error": "overloaded", "trace": "t-1"}),
            spec=_spec(error_type=ProviderError),
        )

        try:
            with pytest.raises(CoreException) as raised:
                [_ async for _ in adapter.stream("complete", Prompt(text="q"))]

        finally:
            await client.close()

        assert (raised.value.details or {})[RESPONSE_ERROR_DETAIL] == {"error": "overloaded"}


# ....................... #


class TestLeavingEarly:
    async def test_closing_the_stream_closes_the_connection(self) -> None:
        body = _Body(b"one", b"two", b"three")
        adapter, client = await _adapter(lambda _r: httpx.Response(200, stream=body))

        try:
            async with aclosing(adapter.stream("complete", Prompt(text="q"))) as chunks:
                async for _chunk in chunks:
                    break

            assert body.closed is True

        finally:
            await client.close()


# ....................... #


def _policy_ctx(*strategies: Any) -> Any:
    from forze.application.execution import Deps

    async def _no_sleep(_delay: float) -> None:
        return None

    executor = InProcessResilienceExecutor(
        policies={"p": ResiliencePolicy(name="p", strategies=strategies)},
        sleep=_no_sleep,
    )

    return context_from_deps(Deps.plain({ResilienceExecutorDepKey: executor}))


class TestUnderAPortPolicy:
    async def test_a_failure_after_the_first_byte_is_not_retried(self) -> None:
        seen: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            return httpx.Response(200, stream=_Body(b"one", fail=httpx.ReadError("reset")))

        adapter, client = await _adapter(handler)
        retrying = RetryStrategy(
            max_attempts=3,
            backoff=BackoffStrategy(base=timedelta(milliseconds=1), max=timedelta(seconds=1)),
            retry_on=frozenset({ExceptionKind.INFRASTRUCTURE}),
        )
        port = wrap_port_policy(
            adapter,
            ctx=_policy_ctx(retrying),
            port_policy=PortPolicy(key=HttpServiceDepKey, policy="p", methods=("invoke", "stream")),
            resolved_route="model",
        )
        received: list[bytes] = []

        try:
            with pytest.raises(CoreException) as raised:
                async for chunk in port.stream("complete", Prompt(text="q")):
                    received.append(chunk)

            streamed = len(seen)

            # The same policy does retry a unary call, so the single request above is the
            # stream's own rule rather than a policy that never retries.
            with pytest.raises(CoreException):
                await port.invoke("complete", Prompt(text="q"))

        finally:
            await client.close()

        assert received == [b"one"]
        assert raised.value.kind is ExceptionKind.INFRASTRUCTURE
        assert streamed == 1
        assert len(seen) - streamed == 3

    async def test_the_breaker_records_a_failed_stream(self) -> None:
        seen: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            return httpx.Response(503)

        adapter, client = await _adapter(handler)
        breaker = CircuitBreakerStrategy(
            failure_ratio=1.0,
            sampling_window=timedelta(seconds=100),
            min_throughput=1,
            break_duration=timedelta(seconds=10),
        )
        port = wrap_port_policy(
            adapter,
            ctx=_policy_ctx(breaker),
            port_policy=PortPolicy(key=HttpServiceDepKey, policy="p"),
            resolved_route="model",
        )

        try:
            with pytest.raises(CoreException):
                [_ async for _ in port.stream("complete", Prompt(text="q"))]

            with pytest.raises(CoreException, match="Circuit breaker open"):
                [_ async for _ in port.stream("complete", Prompt(text="q"))]

        finally:
            await client.close()

        assert len(seen) == 1

    async def test_a_cancelled_consumer_closes_the_response_and_spares_the_breaker(
        self,
    ) -> None:
        stalled = _Body(b"one", stall=asyncio.Event())
        bodies = iter([stalled, _Body(b"again")])
        adapter, client = await _adapter(lambda _r: httpx.Response(200, stream=next(bodies)))
        # Trips on a single failure, so a cancellation counted as one would refuse the
        # next stream outright.
        breaker = CircuitBreakerStrategy(
            failure_ratio=1.0,
            sampling_window=timedelta(seconds=100),
            min_throughput=1,
            break_duration=timedelta(seconds=10),
        )
        port = wrap_port_policy(
            adapter,
            ctx=_policy_ctx(breaker),
            port_policy=PortPolicy(key=HttpServiceDepKey, policy="p"),
            resolved_route="model",
        )
        first = asyncio.Event()

        async def consume() -> None:
            async for _chunk in port.stream("complete", Prompt(text="q")):
                first.set()

        try:
            task = asyncio.create_task(consume())
            await asyncio.wait_for(first.wait(), 2)
            task.cancel()

            with pytest.raises(asyncio.CancelledError):
                await task

            closed = stalled.closed
            again = [chunk async for chunk in port.stream("complete", Prompt(text="q"))]

        finally:
            await client.close()

        assert closed is True
        assert again == [b"again"]


# ....................... #


class _SecretsStub:
    async def resolve_str(self, ref: SecretRef) -> str:
        return '{"base_url": "https://tenant.example.com", "bearer_token": "tok"}'

    async def exists(self, ref: SecretRef) -> bool:
        return True


class TestATenantRoutedStream:
    async def test_it_reaches_the_tenants_url_with_the_tenants_credential(self) -> None:
        seen: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            return httpx.Response(200, content=b"tenant")

        routed = RoutedHttpClient(
            secrets=_SecretsStub(),
            secret_ref_for_tenant={TENANT_ID: SecretRef(path="tenants/model")},
            tenant_provider=lambda: TENANT_ID,
        )

        async def initialize_client(tenant_id: UUID, creds: HttpRoutingCredentials) -> HttpClient:
            client = HttpClient()
            await client.initialize(
                creds.base_url,
                default_headers=credential_auth_headers(creds),
                transport=httpx.MockTransport(handler),
            )

            return client

        routed.initialize_client = initialize_client  # type: ignore[method-assign]
        await routed.startup()
        adapter = HttpServiceAdapter(
            client=routed, config=HttpServiceConfig(tenant_aware=True), spec=_spec()
        )

        try:
            async with asyncio.timeout(2):
                chunks = [chunk async for chunk in adapter.stream("complete", Prompt(text="q"))]

        finally:
            await routed.close()

        assert chunks == [b"tenant"]
        assert str(seen[0].url) == "https://tenant.example.com/v1/complete"
        assert seen[0].headers["authorization"] == "Bearer tok"
