"""`response_decoding="raw"`: a non-JSON success body reaches the caller whole, as `RawBody`.

Only the success body changes. The request, the error mapping, the resilience policy, the
deadline budget and the response cap are the ones a JSON operation gets.
"""

import csv
import io
from collections.abc import Callable
from datetime import timedelta
from typing import Any

import httpx
import pytest
from pydantic import BaseModel, model_validator

from forze.application.contracts.envelope import HTTP_HEADER_DEADLINE_BUDGET
from forze.application.contracts.http import (
    RESPONSE_ERROR_DETAIL,
    HttpOperationSpec,
    HttpServiceDepKey,
    HttpServiceSpec,
    RawBody,
)
from forze.application.contracts.resilience import (
    BackoffStrategy,
    PortPolicy,
    ResilienceExecutorDepKey,
    ResiliencePolicy,
    RetryStrategy,
)
from forze.application.execution.context import bind_deadline
from forze.application.execution.resilience import InProcessResilienceExecutor
from forze.application.execution.resilience.port_policy import wrap_port_policy
from forze.application.integrations.http import (
    BaseHttpIntegration,
    async_http_op,
    build_http_service_spec,
)
from forze.base.exceptions import CoreException, ExceptionKind
from forze.testing import context_from_deps
from forze_http.adapters.http_service import HttpServiceAdapter
from forze_http.execution.deps.configs import HttpServiceConfig
from forze_http.kernel.client import HttpClient, HttpConfig

pytestmark = pytest.mark.unit

# ----------------------- #

CSV = "date,WALCL\n2026-01-07,6500000\n2026-01-14,6510000\n"


class SeriesArgs(BaseModel):
    id: str


class ProviderError(BaseModel):
    error: str


class CsvBody(RawBody):
    """A subclass that refuses anything but CSV."""

    @model_validator(mode="after")
    def _is_csv(self) -> "CsvBody":
        if self.media_type != "text/csv":
            raise ValueError(f"expected text/csv, got {self.media_type}")

        return self


def _spec(
    *,
    return_type: type[RawBody] = RawBody,
    error_type: type[BaseModel] | None = None,
) -> HttpServiceSpec:
    return HttpServiceSpec(
        name="fred",
        operations={
            "series": HttpOperationSpec(
                name="series",
                method="GET",
                path="/graph/fredgraph.csv",
                args_type=SeriesArgs,
                return_type=return_type,
                response_decoding="raw",
                idempotent=True,
                error_type=error_type,
            ),
        },
    )


async def _adapter(
    handler: Callable[[httpx.Request], httpx.Response],
    *,
    spec: HttpServiceSpec | None = None,
    max_response_bytes: int | None = None,
) -> tuple[HttpServiceAdapter, HttpClient]:
    client = HttpClient()
    await client.initialize(
        config=HttpConfig(max_response_bytes=max_response_bytes),
        transport=httpx.MockTransport(handler),
    )

    adapter = HttpServiceAdapter(
        client=client,
        config=HttpServiceConfig(base_url="https://fred.example.com"),
        spec=spec or _spec(),
    )

    return adapter, client


# ....................... #


class TestTheBody:
    async def test_csv_arrives_with_its_media_type_and_charset(self) -> None:
        seen: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            return httpx.Response(
                200,
                content=CSV.encode(),
                headers={"Content-Type": "Text/CSV; charset=UTF-8"},
            )

        adapter, client = await _adapter(handler)

        try:
            body = await adapter.invoke("series", SeriesArgs(id="WALCL"))

        finally:
            await client.aclose()

        assert isinstance(body, RawBody)
        assert body.content == CSV.encode()
        assert body.media_type == "text/csv"
        assert body.charset == "utf-8"
        assert list(csv.DictReader(io.StringIO(body.text)))[1]["WALCL"] == "6510000"
        assert str(seen[0].url) == "https://fred.example.com/graph/fredgraph.csv?id=WALCL"

    async def test_bytes_arrive_untouched(self) -> None:
        payload = bytes(range(256))

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                content=payload,
                headers={"Content-Type": "application/octet-stream"},
            )

        adapter, client = await _adapter(handler)

        try:
            body = await adapter.invoke("series", SeriesArgs(id="WALCL"))

        finally:
            await client.aclose()

        assert body == RawBody(content=payload, media_type="application/octet-stream")

    async def test_an_empty_body_is_empty_content(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(204)

        adapter, client = await _adapter(handler)

        try:
            body = await adapter.invoke("series", SeriesArgs(id="WALCL"))

        finally:
            await client.aclose()

        assert body == RawBody(content=b"")

    async def test_a_subclass_that_refuses_the_body_is_an_internal_error(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, html="<p>maintenance</p>")

        adapter, client = await _adapter(handler, spec=_spec(return_type=CsvBody))

        try:
            with pytest.raises(CoreException) as raised:
                await adapter.invoke("series", SeriesArgs(id="WALCL"))

        finally:
            await client.aclose()

        assert raised.value.kind is ExceptionKind.INTERNAL
        assert raised.value.code == "http.response.validation"

    async def test_the_response_cap_holds(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, content=b"x" * 64, headers={"Content-Type": "text/plain"})

        adapter, client = await _adapter(handler, max_response_bytes=16)

        try:
            with pytest.raises(CoreException, match="max_response_bytes"):
                await adapter.invoke("series", SeriesArgs(id="WALCL"))

        finally:
            await client.aclose()

    async def test_stream_still_yields_the_bytes(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, content=CSV.encode(), headers={"Content-Type": "text/csv"})

        adapter, client = await _adapter(handler)

        try:
            chunks = [c async for c in adapter.stream("series", SeriesArgs(id="WALCL"))]

        finally:
            await client.aclose()

        assert b"".join(chunks) == CSV.encode()

    async def test_a_facade_operation_returns_the_raw_body(self) -> None:
        class Fred(BaseHttpIntegration):
            series = async_http_op(
                request=SeriesArgs,
                response=RawBody,
                method="GET",
                path="/graph/fredgraph.csv",
                response_decoding="raw",
            )

        spec = build_http_service_spec(Fred, name="fred")

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, text="ok")

        adapter, client = await _adapter(handler, spec=spec)

        try:
            body = await Fred(port=adapter, spec=spec).series(SeriesArgs(id="WALCL"))

        finally:
            await client.aclose()

        assert spec.operations["series"].response_decoding == "raw"
        assert body.text == "ok"


# ....................... #


class TestTheRestIsUnchanged:
    @pytest.mark.parametrize(
        ("status", "kind"),
        [(404, ExceptionKind.NOT_FOUND), (503, ExceptionKind.INFRASTRUCTURE)],
    )
    async def test_a_rejection_maps_as_on_a_json_operation(
        self, status: int, kind: ExceptionKind
    ) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(status, text="nope", headers={"Content-Type": "text/plain"})

        adapter, client = await _adapter(handler)

        try:
            with pytest.raises(CoreException) as raised:
                await adapter.invoke("series", SeriesArgs(id="WALCL"))

        finally:
            await client.aclose()

        assert raised.value.kind is kind

    async def test_a_declared_json_error_body_reaches_details(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(400, json={"error": "bad_series"})

        adapter, client = await _adapter(handler, spec=_spec(error_type=ProviderError))

        try:
            with pytest.raises(CoreException) as raised:
                await adapter.invoke("series", SeriesArgs(id="nope"))

        finally:
            await client.aclose()

        assert raised.value.details is not None
        assert raised.value.details[RESPONSE_ERROR_DETAIL] == {"error": "bad_series"}

    async def test_the_deadline_budget_travels(self) -> None:
        seen: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            return httpx.Response(200, text=CSV)

        adapter, client = await _adapter(handler)

        try:
            with bind_deadline(5.0):
                await adapter.invoke("series", SeriesArgs(id="WALCL"))

        finally:
            await client.aclose()

        assert 0.0 < float(seen[0].headers[HTTP_HEADER_DEADLINE_BUDGET]) <= 5.0

    async def test_a_port_policy_retries_it(self) -> None:
        from forze.application.execution import Deps

        statuses = iter([503, 503, 200])

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(next(statuses), text=CSV, headers={"Content-Type": "text/csv"})

        async def _no_sleep(_delay: float) -> None:
            return None

        retrying = RetryStrategy(
            max_attempts=3,
            backoff=BackoffStrategy(base=timedelta(milliseconds=1), max=timedelta(seconds=1)),
            retry_on=frozenset({ExceptionKind.INFRASTRUCTURE}),
        )
        executor = InProcessResilienceExecutor(
            policies={"p": ResiliencePolicy(name="p", strategies=(retrying,))},
            sleep=_no_sleep,
        )
        adapter, client = await _adapter(handler)
        port: Any = wrap_port_policy(
            adapter,
            ctx=context_from_deps(Deps.plain({ResilienceExecutorDepKey: executor})),
            port_policy=PortPolicy(key=HttpServiceDepKey, policy="p"),
            resolved_route="fred",
        )

        try:
            body = await port.invoke("series", SeriesArgs(id="WALCL"))

        finally:
            await client.aclose()

        assert body.text == CSV
        assert next(statuses, None) is None
