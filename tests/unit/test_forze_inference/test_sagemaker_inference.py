"""SageMaker realtime inference: adapter behavior over a stub runtime client."""

from __future__ import annotations

import json
from typing import Any

import attrs
import pytest
from pydantic import BaseModel

from forze.application.contracts.inference import (
    UNSUPPORTED_INFERENCE_FEATURE_CODE,
    InferenceSpec,
)
from forze.application.execution import ExecutionContext
from forze.base.exceptions import CoreException, exc
from forze.testing import context_from_modules
from forze_inference.sagemaker import (
    SageMakerInferenceConfig,
    SageMakerInferenceDepsModule,
)

# ----------------------- #


class _Features(BaseModel):
    x: float = 0.0


class _Score(BaseModel):
    y: float = 0.0


def _spec() -> InferenceSpec[_Features, _Score]:
    return InferenceSpec(name="doubler", input=_Features, output=_Score)


@attrs.define(slots=True)
class _StubRuntimeClient:
    """In-memory ``SageMakerRuntimeClientPort`` doubling every instance."""

    invocations: list[dict[str, Any]] = attrs.field(factory=list)
    raises: Exception | None = None

    async def invoke_endpoint(
        self,
        endpoint_name: str,
        *,
        body: bytes,
        content_type: str = "application/json",
        accept: str = "application/json",
        target_variant: str | None = None,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        if self.raises is not None:
            raise self.raises

        payload = json.loads(body)
        self.invocations.append(
            {
                "endpoint": endpoint_name,
                "variant": target_variant,
                "content_type": content_type,
                "instances": payload["instances"],
            }
        )

        return {"predictions": [{"y": row["x"] * 2.0} for row in payload["instances"]]}

    async def close(self) -> None:
        return None


def _config(**overrides: Any) -> SageMakerInferenceConfig:
    values: dict[str, Any] = {
        "endpoint_name": "doubler-prod",
        "acknowledge_data_egress": True,
    }
    values.update(overrides)
    return SageMakerInferenceConfig(**values)


def _ctx(client: _StubRuntimeClient, config: SageMakerInferenceConfig) -> ExecutionContext:
    module = SageMakerInferenceDepsModule(client=client, models={"doubler": config})
    return context_from_modules(module)


# ....................... #


class TestSageMakerInferenceAdapter:
    @pytest.mark.asyncio
    async def test_predict_round_trip(self) -> None:
        client = _StubRuntimeClient()
        port = _ctx(client, _config(target_variant="blue")).inference.model(_spec())

        assert (await port.predict(_Features(x=3.0))).y == 6.0
        assert client.invocations == [
            {
                "endpoint": "doubler-prod",
                "variant": "blue",
                "content_type": "application/json",
                "instances": [{"x": 3.0}],
            }
        ]

    @pytest.mark.asyncio
    async def test_predict_many_order_preserving(self) -> None:
        port = _ctx(_StubRuntimeClient(), _config()).inference.model(_spec())

        out = await port.predict_many([_Features(x=1.0), _Features(x=2.0)])
        assert [o.y for o in out] == [2.0, 4.0]

    @pytest.mark.asyncio
    async def test_translated_endpoint_error_surfaces_as_is(self) -> None:
        client = _StubRuntimeClient(
            raises=exc.throttled("busy", code="inference_throttled"),
        )
        port = _ctx(client, _config()).inference.model(_spec())

        with pytest.raises(CoreException) as ei:
            await port.predict(_Features())
        assert ei.value.code == "inference_throttled"

    @pytest.mark.asyncio
    async def test_oversized_batch_refused_before_wire(self) -> None:
        client = _StubRuntimeClient()
        port = _ctx(client, _config(max_batch_size=2)).inference.model(_spec())

        with pytest.raises(CoreException) as ei:
            await port.predict_many([_Features()] * 3)
        assert ei.value.code == UNSUPPORTED_INFERENCE_FEATURE_CODE
        assert client.invocations == []

    @pytest.mark.asyncio
    async def test_stream_sub_batches_to_cap(self) -> None:
        client = _StubRuntimeClient()
        port = _ctx(client, _config(max_batch_size=2)).inference.model(_spec())

        async def chunks():
            yield [_Features(x=v) for v in (1.0, 2.0, 3.0)]

        seen = [[o.y for o in chunk] async for chunk in port.predict_stream(chunks())]

        assert seen == [[2.0, 4.0, 6.0]]
        assert [len(call["instances"]) for call in client.invocations] == [2, 1]

    @pytest.mark.asyncio
    async def test_tenant_aware_route_without_tenant_fails_closed(self) -> None:
        port = _ctx(_StubRuntimeClient(), _config(tenant_aware=True)).inference.model(_spec())

        with pytest.raises(CoreException) as ei:
            await port.predict(_Features())
        assert ei.value.code == "tenant_required"

    def test_egress_acknowledgment_fails_closed(self) -> None:
        with pytest.raises(CoreException, match="acknowledge_data_egress"):
            SageMakerInferenceConfig(endpoint_name="e")

    @pytest.mark.asyncio
    async def test_scalar_predictions_wrap_single_output_field(self) -> None:
        @attrs.define(slots=True)
        class _ScalarClient(_StubRuntimeClient):
            async def invoke_endpoint(self, endpoint_name: str, **kwargs: Any) -> dict[str, Any]:
                payload = json.loads(kwargs["body"])
                return {"predictions": [row["x"] * 2.0 for row in payload["instances"]]}

        port = _ctx(_ScalarClient(), _config()).inference.model(_spec())

        assert (await port.predict(_Features(x=4.0))).y == 8.0
