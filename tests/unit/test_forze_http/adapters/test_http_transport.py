"""A vendor SDK's requests, sent through a declared HTTP service.

An SDK takes an ``httpx.AsyncClient`` and builds its own requests, so the service adapter
never sees them. :class:`HttpServiceTransport` stands in for that client's transport. These
tests pin that each request leaves the way a declared operation does — held to the service's
base URL, carrying its credential, deadline budget, trace context and egress mark — and that
the SDK still gets back the response the counterparty sent.
"""

import gzip
import tracemalloc
from collections.abc import AsyncIterator, Callable
from typing import Any
from uuid import UUID

import httpx
import pytest
from pydantic import BaseModel

from forze.application.contracts.deps import Deps
from forze.application.contracts.envelope import HTTP_HEADER_DEADLINE_BUDGET
from forze.application.contracts.http import HttpServiceDepKey
from forze.application.contracts.secrets import SecretRef
from forze.application.execution.context import bind_deadline
from forze.application.integrations.http import build_http_service_spec
from forze.application.integrations.http.descriptors import BaseHttpIntegration, async_http_op
from forze.base.exceptions import CoreException, ExceptionKind
from forze.testing import context_from_deps
from forze_http import (
    HttpAuthConfig,
    HttpClient,
    HttpConfig,
    HttpDepsModule,
    HttpServiceConfig,
    HttpServiceTransport,
    RoutedHttpClient,
    http_service_transport,
)
from forze_http.kernel.client.client import _is_under
from forze_http.kernel.client.credentials import credential_auth_headers
from forze_http.kernel.client.routing_credentials import HttpRoutingCredentials

pytestmark = pytest.mark.unit

# ----------------------- #

BASE = "https://api.example.com/v1"
TENANT_ID = UUID("00000000-0000-0000-0000-000000000001")

Handler = Callable[[httpx.Request], httpx.Response]


class _Answer(BaseModel):
    ok: bool


class _LlmClient(BaseHttpIntegration):
    complete = async_http_op(request=None, response=_Answer, method="POST", path="/complete")


SPEC = build_http_service_spec(_LlmClient, name="llm")


def _ok(seen: list[httpx.Request]) -> Handler:
    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={"ok": True})

    return handler


async def _transport(
    handler: Handler,
    *,
    max_response_bytes: int | None = None,
    **config: Any,
) -> tuple[HttpServiceTransport, HttpClient]:
    client = HttpClient()
    await client.initialize(
        config=HttpConfig(max_response_bytes=max_response_bytes),
        transport=httpx.MockTransport(handler),
    )
    service = HttpServiceConfig(base_url=BASE, **config)

    return HttpServiceTransport(client=client, config=service, spec=SPEC), client


async def _send(transport: HttpServiceTransport, url: str, **kwargs: Any) -> httpx.Response:
    """One request the way an SDK sends it: built and sent by its own ``AsyncClient``."""

    async with httpx.AsyncClient(transport=transport) as sdk:
        return await sdk.post(url, **kwargs)


class _Chunks(httpx.AsyncByteStream):
    """A body served as these chunks, unread until the client reads it."""

    def __init__(self, *chunks: bytes) -> None:
        self._chunks = chunks

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for chunk in self._chunks:
            yield chunk


# ....................... #


class TestHttpServiceTransport:
    async def test_the_service_credential_replaces_the_sdks(self) -> None:
        seen: list[httpx.Request] = []
        transport, client = await _transport(
            _ok(seen),
            auth=HttpAuthConfig(kind="bearer", token="governed-key"),
            default_headers={"X-Org": "acme"},
        )

        try:
            response = await _send(
                transport,
                f"{BASE}/chat",
                json={"q": "hi"},
                headers={"Authorization": "Bearer sdk-placeholder", "X-Sdk": "1"},
            )

        finally:
            await client.aclose()

        assert response.json() == {"ok": True}
        sent = seen[0]
        assert sent.headers["authorization"] == "Bearer governed-key"
        assert sent.headers["x-org"] == "acme"
        # Everything else the SDK set travels as it was.
        assert sent.headers["x-sdk"] == "1"
        assert sent.content == b'{"q":"hi"}'

    async def test_the_deadline_budget_travels(self) -> None:
        seen: list[httpx.Request] = []
        transport, client = await _transport(_ok(seen))

        try:
            await _send(transport, f"{BASE}/chat")

            with bind_deadline(5.0):
                await _send(transport, f"{BASE}/chat")

        finally:
            await client.aclose()

        assert HTTP_HEADER_DEADLINE_BUDGET not in seen[0].headers
        assert 0.0 < float(seen[1].headers[HTTP_HEADER_DEADLINE_BUDGET]) <= 5.0

    async def test_the_deadline_budget_respects_the_opt_out(self) -> None:
        seen: list[httpx.Request] = []
        transport, client = await _transport(_ok(seen), propagate_deadline=False)

        try:
            with bind_deadline(5.0):
                await _send(transport, f"{BASE}/chat")

        finally:
            await client.aclose()

        assert HTTP_HEADER_DEADLINE_BUDGET not in seen[0].headers

    async def test_a_spent_deadline_sends_nothing(self) -> None:
        seen: list[httpx.Request] = []
        transport, client = await _transport(_ok(seen))

        try:
            with bind_deadline(0.0), pytest.raises(CoreException) as raised:
                await _send(transport, f"{BASE}/chat")

        finally:
            await client.aclose()

        assert raised.value.kind is ExceptionKind.TIMEOUT
        assert raised.value.code == "deadline_exceeded"
        assert seen == []

    async def test_the_trace_continues_and_sensitive_egress_is_marked(self) -> None:
        from opentelemetry.sdk.trace import TracerProvider

        seen: list[httpx.Request] = []
        transport, client = await _transport(
            _ok(seen), egress_sensitive=True, acknowledge_data_egress=True
        )
        tracer = TracerProvider().get_tracer("test")

        try:
            with tracer.start_as_current_span("caller") as span:
                await _send(transport, f"{BASE}/chat")

        finally:
            await client.aclose()

        trace_id = f"{span.get_span_context().trace_id:032x}"
        assert trace_id in seen[0].headers["traceparent"]
        assert (span.attributes or {}).get("forze.egress.sensitive") is True  # type: ignore[attr-defined]

    async def test_an_ordinary_route_marks_nothing(self) -> None:
        from opentelemetry.sdk.trace import TracerProvider

        transport, client = await _transport(_ok([]))
        tracer = TracerProvider().get_tracer("test")

        try:
            with tracer.start_as_current_span("caller") as span:
                await _send(transport, f"{BASE}/chat")

        finally:
            await client.aclose()

        assert "forze.egress.sensitive" not in (span.attributes or {})  # type: ignore[attr-defined]

    @pytest.mark.parametrize(
        "url",
        [
            "https://evil.example.com/v1/chat",
            "https://api.example.com.evil.net/v1/chat",
            "http://api.example.com/v1/chat",
            "https://api.example.com:8443/v1/chat",
            "https://api.example.com/v2/chat",
            "https://api.example.com/v1-admin",
            "https://api.example.com/",
            "https://api.example.com/v1/../admin",
            "https://api.example.com/v1/%2e%2e/admin",
            "https://api.example.com/v1/..%2Fadmin",
            "https://api.example.com/v1/..%5Cadmin",
            "https://api.example.com/v1/..;/admin",
            "https://api.example.com/v1/chat;jsessionid=1",
            "https://api.example.com/v1/..%3B/admin",
            "https://api.example.com/v1/%252e%252e/admin",
            "https://user:pass@api.example.com/v1/chat",
            "https://api.example.com@evil.example.com/v1/chat",
        ],
    )
    async def test_a_url_off_the_route_is_refused_before_it_is_sent(self, url: str) -> None:
        seen: list[httpx.Request] = []
        transport, client = await _transport(
            _ok(seen), auth=HttpAuthConfig(kind="bearer", token="governed-key")
        )

        try:
            with pytest.raises(CoreException) as raised:
                await _send(transport, url)

        finally:
            await client.aclose()

        assert raised.value.kind is ExceptionKind.CONFIGURATION
        assert raised.value.code == "http_request_off_route"
        assert seen == []

    @pytest.mark.parametrize(
        "url",
        [
            "https://api.example.com/v1",
            "https://api.example.com/v1/",
            "https://API.example.com:443/v1/chat/completions?stream=true",
        ],
    )
    async def test_a_url_on_the_route_is_sent(self, url: str) -> None:
        seen: list[httpx.Request] = []
        transport, client = await _transport(_ok(seen))

        try:
            response = await _send(transport, url)

        finally:
            await client.aclose()

        assert response.status_code == 200
        assert len(seen) == 1

    async def test_a_redirect_off_the_route_is_refused(self) -> None:
        # Not followed by the pooled client: the SDK's client follows it, and the next hop
        # comes back through the transport, where it is held to the route again.
        seen: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            return httpx.Response(302, headers={"Location": "https://evil.example.com/steal"})

        transport, client = await _transport(
            handler, auth=HttpAuthConfig(kind="api_key", header_name="X-Api-Key", token="k")
        )

        try:
            async with httpx.AsyncClient(transport=transport, follow_redirects=True) as sdk:
                with pytest.raises(CoreException) as raised:
                    await sdk.get(f"{BASE}/chat")

        finally:
            await client.aclose()

        assert raised.value.code == "http_request_off_route"
        assert [str(r.url) for r in seen] == [f"{BASE}/chat"]

    async def test_a_rejection_comes_back_as_the_counterparty_sent_it(self) -> None:
        # The SDK owns status handling: a 429 is its to retry or raise, not forze's to map.
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(429, headers={"Retry-After": "3"}, json={"error": "slow"})

        transport, client = await _transport(handler)

        try:
            response = await _send(transport, f"{BASE}/chat")

        finally:
            await client.aclose()

        assert response.status_code == 429
        assert response.headers["retry-after"] == "3"
        assert response.json() == {"error": "slow"}

    async def test_a_declared_oversized_body_is_refused(self) -> None:
        transport, client = await _transport(
            lambda _r: httpx.Response(200, content=b"x" * 64), max_response_bytes=16
        )

        try:
            with pytest.raises(CoreException) as raised:
                await _send(transport, f"{BASE}/chat")

        finally:
            await client.aclose()

        assert raised.value.kind is ExceptionKind.INFRASTRUCTURE

    async def test_an_unsized_body_is_cut_off_at_the_cap(self) -> None:
        async def body() -> AsyncIterator[bytes]:
            for _ in range(8):
                yield b"x" * 8

        transport, client = await _transport(
            lambda _r: httpx.Response(200, content=body()), max_response_bytes=16
        )

        try:
            with pytest.raises(CoreException) as raised:
                await _send(transport, f"{BASE}/chat")

        finally:
            await client.aclose()

        assert raised.value.kind is ExceptionKind.INFRASTRUCTURE


# ....................... #


class TestHttpServiceTransportUnderAnSdk:
    """The transport inside an ``AsyncClient`` built the way a vendor SDK builds one."""

    async def test_an_sdk_client_streams_and_decodes_through_it(self) -> None:
        seen: list[httpx.Request] = []

        async def events() -> AsyncIterator[bytes]:
            yield b"data: one\n"
            yield b"\ndata: tw"
            yield b"o\n\n"

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)

            if request.url.path.endswith("/stream"):
                return httpx.Response(
                    200, headers={"Content-Type": "text/event-stream"}, content=events()
                )

            # Compressed on the wire: the SDK's client decodes it, so it must arrive undecoded.
            return httpx.Response(
                200,
                headers={"Content-Encoding": "gzip"},
                content=gzip.compress(b'{"ok": true}'),
            )

        # A cap, so the decoded-size count runs alongside the stream without holding it up.
        transport, client = await _transport(
            handler,
            max_response_bytes=4096,
            auth=HttpAuthConfig(kind="bearer", token="governed-key"),
        )

        try:
            async with httpx.AsyncClient(
                base_url=BASE,
                transport=transport,
                headers={"Authorization": "Bearer placeholder"},
            ) as sdk:
                answer = await sdk.post("/chat/completions", json={"stream": False})

                async with sdk.stream("POST", "/chat/stream", json={"stream": True}) as streamed:
                    lines = [line async for line in streamed.aiter_lines()]

            # Closing the SDK's client leaves the pooled client open for the next one.
            async with httpx.AsyncClient(base_url=BASE, transport=transport) as again:
                assert (await again.post("/chat/completions")).json() == {"ok": True}

        finally:
            await client.aclose()

        assert answer.json() == {"ok": True}
        # Lines cut across chunks come out whole: the SDK reads the stream, not a buffer.
        assert lines == ["data: one", "", "data: two", ""]
        assert {r.headers["authorization"] for r in seen} == {"Bearer governed-key"}
        # The SDK's own Accept-Encoding is replaced by the codings a capped client can bound.
        assert {r.headers["accept-encoding"] for r in seen} == {"gzip, deflate"}

    async def test_a_compression_bomb_never_reaches_the_sdks_decoder(self) -> None:
        # 10 MB of zeros in one 10 KB chunk, under a 100 KB cap counted on the wire. The SDK
        # decodes what it receives, so the cap has to count what the chunk decodes to.
        bomb = gzip.compress(bytes(10_000_000))

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, headers={"Content-Encoding": "gzip"}, stream=_Chunks(bomb))

        transport, client = await _transport(handler, max_response_bytes=100_000)
        tracemalloc.start()

        try:
            async with httpx.AsyncClient(base_url=BASE, transport=transport) as sdk:
                with pytest.raises(CoreException) as raised:
                    await sdk.post("/chat/completions")

            peak = tracemalloc.get_traced_memory()[1]

        finally:
            tracemalloc.stop()
            await client.aclose()

        assert raised.value.kind is ExceptionKind.INFRASTRUCTURE
        assert "max_response_bytes" in str(raised.value)
        assert peak < 3_000_000

    async def test_a_coding_the_cap_cannot_bound_is_refused(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, headers={"Content-Encoding": "br"}, stream=_Chunks(b"?"))

        transport, client = await _transport(handler, max_response_bytes=4096)

        try:
            async with httpx.AsyncClient(base_url=BASE, transport=transport) as sdk:
                with pytest.raises(CoreException) as raised:
                    await sdk.post("/chat/completions")

        finally:
            await client.aclose()

        assert "content coding" in str(raised.value)


# ....................... #


class _TenantSecrets:
    async def resolve_str(self, ref: SecretRef) -> str:
        return (
            '{"base_url": "https://tenant.example.com/api", '
            '"headers": {"X-Tenant": "abc"}, "bearer_token": "tenant-key"}'
        )

    async def exists(self, ref: SecretRef) -> bool:
        return True


class TestHttpServiceTransportForATenant:
    async def test_a_tenant_route_is_held_to_that_tenants_url_and_credential(self) -> None:
        seen: list[httpx.Request] = []
        mock = httpx.MockTransport(_ok(seen))

        routed = RoutedHttpClient(
            secrets=_TenantSecrets(),
            secret_ref_for_tenant={TENANT_ID: SecretRef(path="tenants/llm")},
            tenant_provider=lambda: TENANT_ID,
        )

        async def initialize_client(tenant_id: UUID, creds: HttpRoutingCredentials) -> HttpClient:
            made = HttpClient()
            await made.initialize(
                creds.base_url, default_headers=credential_auth_headers(creds), transport=mock
            )
            return made

        routed.initialize_client = initialize_client  # type: ignore[method-assign]
        await routed.startup()

        transport = HttpServiceTransport(
            client=routed, config=HttpServiceConfig(tenant_aware=True), spec=SPEC
        )

        try:
            response = await _send(
                transport,
                "https://tenant.example.com/api/chat",
                headers={"Authorization": "Bearer placeholder"},
            )

            with pytest.raises(CoreException) as raised:
                await _send(transport, "https://api.example.com/v1/chat")

        finally:
            await routed.close()

        assert response.status_code == 200
        assert [str(r.url) for r in seen] == ["https://tenant.example.com/api/chat"]
        assert seen[0].headers["authorization"] == "Bearer tenant-key"
        assert seen[0].headers["x-tenant"] == "abc"
        assert raised.value.code == "http_request_off_route"


# ....................... #


class TestHttpServiceTransportFromContext:
    """Built from the service's dep key, so only a declared, wired service yields one."""

    async def test_a_declared_service_yields_a_transport_over_its_route(self) -> None:
        seen: list[httpx.Request] = []
        client = HttpClient()
        await client.initialize(transport=httpx.MockTransport(_ok(seen)))
        config = HttpServiceConfig(base_url=BASE)
        ctx = context_from_deps(HttpDepsModule(client=client, services={"llm": config})())

        transport = http_service_transport(ctx, SPEC)

        try:
            await _send(transport, f"{BASE}/chat")

            with pytest.raises(CoreException):
                await _send(transport, "https://elsewhere.example.com/v1/chat")

        finally:
            await client.aclose()

        assert transport.config is config
        assert transport.client is client
        assert len(seen) == 1

    def test_an_undeclared_service_is_refused(self) -> None:
        other = HttpServiceConfig(base_url=BASE)
        ctx = context_from_deps(HttpDepsModule(client=HttpClient(), services={"other": other})())

        with pytest.raises(CoreException):
            http_service_transport(ctx, SPEC)

    def test_a_service_served_by_another_backend_is_refused(self) -> None:
        def foreign(ctx: Any, spec: Any) -> Any:
            raise AssertionError("never built")

        ctx = context_from_deps(Deps.routed({HttpServiceDepKey: {"llm": foreign}}))

        with pytest.raises(CoreException) as raised:
            http_service_transport(ctx, SPEC)

        assert raised.value.kind is ExceptionKind.CONFIGURATION

    def test_unacknowledged_sensitive_egress_never_reaches_a_transport(self) -> None:
        # The gate is the service config itself, so nothing downstream can be built from it.
        with pytest.raises(CoreException) as raised:
            HttpServiceConfig(base_url=BASE, egress_sensitive=True)

        assert raised.value.code == "http_egress_unacknowledged"


# ....................... #


class TestIsUnder:
    """The pin compared on raw strings, including shapes httpx would normalize first."""

    @pytest.mark.parametrize(
        ("url", "under"),
        [
            ("https://api.example.com/v1/x", True),
            ("https://api.example.com:443/v1", True),
            ("https://api.example.com/v1/../admin", False),
            ("https://api.example.com/v1/./x", False),
            ("https://api.example.com/v1x", False),
            ("https://api.example.com:notaport/v1", False),
            ("https://api.example.com/v1/..;/admin", False),
            ("https://api.example.com/v1/%252e%252e/admin", False),
            ("https://api.example.com/v1/%2525", False),
            ("HTTPS://API.EXAMPLE.COM/v1/x", True),
        ],
    )
    def test_compares_scheme_host_port_and_path_segments(self, url: str, under: bool) -> None:
        assert _is_under(url, "https://api.example.com/v1/") is under

    def test_a_root_base_admits_any_path_on_its_origin(self) -> None:
        assert _is_under("https://api.example.com/anything", "https://api.example.com")
        assert not _is_under("http://api.example.com/anything", "https://api.example.com")
