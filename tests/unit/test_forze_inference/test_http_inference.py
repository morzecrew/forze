"""Served-model inference over HTTP: protocols, adapter, wiring, error taxonomy."""

from __future__ import annotations

import json
from datetime import timedelta
from typing import Any

import httpx
import pytest
from pydantic import BaseModel

from forze.application.contracts.inference import (
    UNSUPPORTED_INFERENCE_FEATURE_CODE,
    InferenceSpec,
)
from forze.application.execution import ExecutionContext
from forze.base.exceptions import CoreException
from forze.base.primitives import remaining_time
from forze.testing import context_from_modules
from forze_inference.http import (
    HttpInferenceConfig,
    HttpInferenceDepsModule,
    InferenceHttpClient,
    inference_http_lifecycle_step,
)

# ----------------------- #


class _Features(BaseModel):
    x: float = 0.0


class _Score(BaseModel):
    y: float = 0.0


def _spec() -> InferenceSpec[_Features, _Score]:
    return InferenceSpec(name="doubler", input=_Features, output=_Score)


async def _client(handler: Any) -> InferenceHttpClient:
    client = InferenceHttpClient()
    await client.initialize(
        "http://model-server",
        transport=httpx.MockTransport(handler),
    )
    return client


def _ctx(client: InferenceHttpClient, config: HttpInferenceConfig) -> ExecutionContext:
    module = HttpInferenceDepsModule(client=client, models={"doubler": config})
    return context_from_modules(module)


def _config(**overrides: Any) -> HttpInferenceConfig:
    values: dict[str, Any] = {
        "protocol": "mlflow",
        "model_name": "doubler-v1",
        "acknowledge_data_egress": True,
    }
    values.update(overrides)
    return HttpInferenceConfig(**values)


# ....................... #


class TestMlflowProtocolAdapter:
    @pytest.mark.asyncio
    async def test_predict_round_trip(self) -> None:
        seen: list[dict[str, Any]] = []

        def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/invocations"
            body = json.loads(request.content)
            seen.append(body)
            return httpx.Response(
                200,
                json={"predictions": [{"y": row["x"] * 2.0} for row in body["instances"]]},
            )

        port = _ctx(await _client(handler), _config()).inference.model(_spec())

        assert (await port.predict(_Features(x=3.0))).y == 6.0
        assert seen == [{"instances": [{"x": 3.0}]}]

    @pytest.mark.asyncio
    async def test_scalar_predictions_wrap_single_output_field(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            body = json.loads(request.content)
            return httpx.Response(
                200,
                json={"predictions": [row["x"] * 2.0 for row in body["instances"]]},
            )

        port = _ctx(await _client(handler), _config()).inference.model(_spec())

        out = await port.predict_many([_Features(x=1.0), _Features(x=2.0)])
        assert [o.y for o in out] == [2.0, 4.0]

    @pytest.mark.asyncio
    async def test_missing_predictions_list_is_a_boundary_error(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"nope": True})

        port = _ctx(await _client(handler), _config()).inference.model(_spec())

        with pytest.raises(CoreException) as ei:
            await port.predict(_Features(x=1.0))
        assert ei.value.code == "inference_output_mismatch"


# ....................... #


class TestKserveV2ProtocolAdapter:
    @pytest.mark.asyncio
    async def test_columnar_round_trip(self) -> None:
        seen: list[dict[str, Any]] = []

        def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/v2/models/doubler-v1/infer"
            body = json.loads(request.content)
            seen.append(body)
            (column,) = body["inputs"]
            return httpx.Response(
                200,
                json={
                    "outputs": [
                        {
                            "name": "y",
                            "shape": [len(column["data"])],
                            "datatype": "FP64",
                            "data": [v * 2.0 for v in column["data"]],
                        }
                    ]
                },
            )

        config = _config(protocol="kserve_v2")
        port = _ctx(await _client(handler), config).inference.model(_spec())

        out = await port.predict_many([_Features(x=1.5), _Features(x=2.5)])
        assert [o.y for o in out] == [3.0, 5.0]
        assert seen[0]["inputs"][0] == {
            "name": "x",
            "shape": [2],
            "datatype": "FP64",
            "data": [1.5, 2.5],
        }

    @pytest.mark.asyncio
    async def test_single_unnamed_output_maps_positionally(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            body = json.loads(request.content)
            n = body["inputs"][0]["shape"][0]
            return httpx.Response(
                200,
                json={
                    "outputs": [
                        {"name": "predict", "shape": [n], "datatype": "FP64", "data": [0.5] * n}
                    ]
                },
            )

        config = _config(protocol="kserve_v2")
        port = _ctx(await _client(handler), config).inference.model(_spec())

        assert (await port.predict(_Features(x=1.0))).y == 0.5

    @pytest.mark.asyncio
    async def test_nested_input_field_refused_at_wiring(self) -> None:
        class _Nested(BaseModel):
            inner: _Features

        class _NestedIn(BaseModel):
            payload: _Nested

        spec = InferenceSpec(name="doubler", input=_NestedIn, output=_Score)

        def handler(request: httpx.Request) -> httpx.Response:  # pragma: no cover
            raise AssertionError("must fail before any wire call")

        config = _config(protocol="kserve_v2")
        ctx = _ctx(await _client(handler), config)

        with pytest.raises(CoreException, match="flat scalar"):
            ctx.inference.model(spec)


# ....................... #


class TestErrorTaxonomy:
    @staticmethod
    async def _port_answering(status: int) -> Any:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(status, text="boom")

        return _ctx(await _client(handler), _config()).inference.model(_spec())

    @pytest.mark.asyncio
    async def test_429_is_throttled(self) -> None:
        port = await self._port_answering(429)
        with pytest.raises(CoreException) as ei:
            await port.predict(_Features())
        assert ei.value.code == "inference_throttled"

    @pytest.mark.asyncio
    async def test_404_is_route_mismatch(self) -> None:
        port = await self._port_answering(404)
        with pytest.raises(CoreException) as ei:
            await port.predict(_Features())
        assert ei.value.code == "inference_route_mismatch"

    @pytest.mark.asyncio
    async def test_400_is_wire_mismatch(self) -> None:
        port = await self._port_answering(400)
        with pytest.raises(CoreException) as ei:
            await port.predict(_Features())
        assert ei.value.code == "inference_output_mismatch"

    @pytest.mark.asyncio
    async def test_500_is_infrastructure(self) -> None:
        port = await self._port_answering(503)
        with pytest.raises(CoreException) as ei:
            await port.predict(_Features())
        assert ei.value.code == "inference_endpoint_unavailable"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", [401, 403])
    async def test_upstream_auth_refusal_is_infrastructure_not_caller_error(
        self, status: int
    ) -> None:
        # An expired service credential or a WAF rule is a deployment fault: as a
        # caller error it would be a permanent 422 with no 5xx alert and no retry.
        port = await self._port_answering(status)
        with pytest.raises(CoreException) as ei:
            await port.predict(_Features())
        assert ei.value.code == "inference_endpoint_unavailable"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", [400, 401, 403, 422, 500])
    async def test_upstream_body_is_never_embedded_in_the_error(self, status: int) -> None:
        # A model server's error body can echo the offending feature values or a
        # container traceback — on the plane declared PII-dense by construction, and
        # into a summary the API caller sees verbatim below 500. Logged, never raised.
        leaked = "rejected feature ssn=078-05-1120"

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(status, text=leaked)

        port = _ctx(await _client(handler), _config()).inference.model(_spec())

        with pytest.raises(CoreException) as ei:
            await port.predict(_Features())

        assert "078-05-1120" not in ei.value.summary
        assert "078-05-1120" not in str(ei.value.details or {})


# ....................... #


class TestGuards:
    def test_egress_acknowledgment_fails_closed(self) -> None:
        with pytest.raises(CoreException, match="acknowledge_data_egress"):
            HttpInferenceConfig(protocol="mlflow", model_name="m")

    @pytest.mark.asyncio
    async def test_oversized_batch_refused_whole(self) -> None:
        calls = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal calls
            calls += 1
            return httpx.Response(200, json={"predictions": []})

        config = _config(max_batch_size=2)
        port = _ctx(await _client(handler), config).inference.model(_spec())

        with pytest.raises(CoreException) as ei:
            await port.predict_many([_Features()] * 3)
        assert ei.value.code == UNSUPPORTED_INFERENCE_FEATURE_CODE
        assert calls == 0  # refused before any wire call

    @pytest.mark.asyncio
    async def test_stream_sub_batches_to_cap_but_keeps_chunk_boundaries(self) -> None:
        wire_sizes: list[int] = []

        def handler(request: httpx.Request) -> httpx.Response:
            body = json.loads(request.content)
            rows = body["instances"]
            wire_sizes.append(len(rows))
            return httpx.Response(
                200,
                json={"predictions": [{"y": row["x"] * 2.0} for row in rows]},
            )

        config = _config(max_batch_size=2)
        port = _ctx(await _client(handler), config).inference.model(_spec())

        async def chunks():
            yield [_Features(x=v) for v in (1.0, 2.0, 3.0, 4.0, 5.0)]
            yield [_Features(x=6.0)]

        seen = [[o.y for o in chunk] async for chunk in port.predict_stream(chunks())]

        assert seen == [[2.0, 4.0, 6.0, 8.0, 10.0], [12.0]]  # caller chunks preserved
        assert wire_sizes == [2, 2, 1, 1]  # wire calls capped at the endpoint's limit

    @pytest.mark.asyncio
    async def test_stream_does_not_charge_consumer_time_to_the_model_budget(self) -> None:
        """The per-call deadline must cover the wire calls, not the consumer's own work:
        a slow consumer would otherwise burn the budget the next chunk still needs."""

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"predictions": [{"y": 1.0}]})

        port = _ctx(await _client(handler), _config()).inference.model(_spec())

        async def chunks():
            yield [_Features(x=1.0)]

        bound_during_yield: list[bool] = []

        async for _ in port.predict_stream(chunks(), options={"timeout": timedelta(seconds=30)}):
            bound_during_yield.append(remaining_time() is not None)

        assert bound_during_yield == [False]

    @pytest.mark.asyncio
    async def test_tenant_aware_route_without_tenant_fails_closed(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:  # pragma: no cover
            raise AssertionError("must fail before any wire call")

        config = _config(tenant_aware=True)
        port = _ctx(await _client(handler), config).inference.model(_spec())

        with pytest.raises(CoreException) as ei:
            await port.predict(_Features())
        assert ei.value.code == "tenant_required"


# ....................... #


class TestLifecycle:
    @pytest.mark.asyncio
    async def test_step_initializes_and_closes_the_client(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"predictions": [{"y": 1.0}]})

        client = InferenceHttpClient()
        module = HttpInferenceDepsModule(client=client, models={"doubler": _config()})
        ctx = context_from_modules(module)

        step = inference_http_lifecycle_step("http://model-server")

        # The startup hook initializes the registered client (transport injection is a
        # test-only concern, so initialize directly here the way the hook would).
        await client.initialize(
            "http://model-server",
            transport=httpx.MockTransport(handler),
        )
        await step.startup(ctx)  # idempotent second initialize via GuardedLifecycle

        port = ctx.inference.model(_spec())
        assert (await port.predict(_Features())).y == 1.0

        await step.shutdown(ctx)

        with pytest.raises(CoreException, match="not initialized"):
            await port.predict(_Features())


# ....................... #


class TestHttpLifecycleWiring:
    @pytest.mark.asyncio
    async def test_startup_leaves_a_custom_port_implementation_alone(self) -> None:
        """A caller may register their own ``InferenceHttpClientPort``; the hook must not
        try to initialize a client it does not own."""

        class _Custom:
            def __init__(self) -> None:
                self.initialized = False

            async def post_json(self, path: str, body: Any, **kwargs: Any) -> dict[str, Any]:
                return {"predictions": []}

            async def close(self) -> None:
                return None

        custom = _Custom()
        module = HttpInferenceDepsModule(client=custom, models={"doubler": _config()})
        ctx = context_from_modules(module)

        await inference_http_lifecycle_step("http://model-server").startup(ctx)

        assert not custom.initialized

    def test_step_id_is_stable_and_overridable(self) -> None:
        assert inference_http_lifecycle_step("http://m").id == "inference_http_client"
        assert inference_http_lifecycle_step("http://m", name="custom").id == "custom"
