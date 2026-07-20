"""SageMaker realtime adapter against moto's SageMaker runtime reimplementation.

What this proves that the stub-client unit tests cannot: the real ``aioboto3``
sagemaker-runtime client, real SigV4 request signing, real botocore response parsing
(including the streaming ``Body``), and our decode + error-translation path over an
actual HTTP wire.

What it deliberately does not prove: prediction content. moto runs no model container —
responses are canned per request body (see ``conftest.sagemaker_results``). Model-side
fidelity for SageMaker is an env-gated real-endpoint concern.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest
import pytest_asyncio
from pydantic import BaseModel, SecretStr

from forze.application.contracts.inference import InferenceSpec
from forze.base.exceptions import CoreException
from forze.testing import context_from_modules
from forze_inference.sagemaker import (
    SageMakerInferenceConfig,
    SageMakerInferenceDepsModule,
    SageMakerRuntimeClient,
)

# ----------------------- #

MOTO_REGION = "eu-west-1"


class _Features(BaseModel):
    x: float = 0.0


class _Score(BaseModel):
    y: float = 0.0


class _Risk(BaseModel):
    risk: float = 0.0


def _spec() -> InferenceSpec[_Features, _Score]:
    return InferenceSpec(name="doubler", input=_Features, output=_Score)


def _config(**overrides: object) -> SageMakerInferenceConfig:
    values: dict[str, object] = {
        "endpoint_name": "doubler-prod",
        "acknowledge_data_egress": True,
    }
    values.update(overrides)
    return SageMakerInferenceConfig(**values)  # type: ignore[arg-type]


@pytest_asyncio.fixture
async def client(moto_url: str) -> SageMakerRuntimeClient:
    runtime = SageMakerRuntimeClient()
    await runtime.initialize(
        region_name=MOTO_REGION,
        endpoint_url=moto_url,
        access_key_id="testing",
        secret_access_key=SecretStr("testing"),
    )

    try:
        yield runtime
    finally:
        await runtime.close()


def _port(client: SageMakerRuntimeClient, config: SageMakerInferenceConfig):
    module = SageMakerInferenceDepsModule(client=client, models={"doubler": config})
    return context_from_modules(module).inference.model(_spec())


# ....................... #


class TestSageMakerLive:
    @pytest.mark.asyncio
    async def test_predict_round_trip(
        self,
        client: SageMakerRuntimeClient,
        sagemaker_results: Callable[..., None],
    ) -> None:
        sagemaker_results({"predictions": [{"y": 4.0}]})

        out = await _port(client, _config(target_variant="blue")).predict(_Features(x=2.0))

        assert out == _Score(y=4.0)

    @pytest.mark.asyncio
    async def test_predict_many_order_preserving(
        self,
        client: SageMakerRuntimeClient,
        sagemaker_results: Callable[..., None],
    ) -> None:
        sagemaker_results({"predictions": [{"y": 2.0}, {"y": 4.0}, {"y": 6.0}]})

        out = await _port(client, _config()).predict_many(
            [_Features(x=1.0), _Features(x=2.0), _Features(x=3.0)]
        )

        assert [o.y for o in out] == [2.0, 4.0, 6.0]

    @pytest.mark.asyncio
    async def test_scalar_predictions_wrap_single_field_output(
        self,
        client: SageMakerRuntimeClient,
        sagemaker_results: Callable[..., None],
    ) -> None:
        sagemaker_results({"predictions": [0.25]})

        module = SageMakerInferenceDepsModule(client=client, models={"risk": _config()})
        port = context_from_modules(module).inference.model(
            InferenceSpec(name="risk", input=_Features, output=_Risk)
        )

        assert (await port.predict(_Features(x=1.0))).risk == 0.25

    @pytest.mark.asyncio
    async def test_cardinality_mismatch_fails_at_boundary(
        self,
        client: SageMakerRuntimeClient,
        sagemaker_results: Callable[..., None],
    ) -> None:
        sagemaker_results({"predictions": [{"y": 1.0}]})  # one prediction, two instances

        with pytest.raises(CoreException) as ei:
            await _port(client, _config()).predict_many([_Features(x=1.0), _Features(x=2.0)])
        assert ei.value.code == "inference_output_mismatch"

    @pytest.mark.asyncio
    async def test_non_json_endpoint_body_fails_at_boundary(
        self,
        client: SageMakerRuntimeClient,
        sagemaker_results: Callable[..., None],
    ) -> None:
        sagemaker_results()  # nothing queued — moto answers with its default non-JSON body

        with pytest.raises(CoreException) as ei:
            await _port(client, _config()).predict(_Features(x=1.0))
        assert ei.value.code == "inference_output_mismatch"

    @pytest.mark.asyncio
    async def test_predict_stream_sub_batches_to_cap(
        self,
        client: SageMakerRuntimeClient,
        sagemaker_results: Callable[..., None],
    ) -> None:
        # Two distinct wire bodies (2 instances, then 1) consume two queued responses.
        sagemaker_results(
            {"predictions": [{"y": 2.0}, {"y": 4.0}]},
            {"predictions": [{"y": 6.0}]},
        )

        port = _port(client, _config(max_batch_size=2))

        async def chunks():
            yield [_Features(x=1.0), _Features(x=2.0), _Features(x=3.0)]

        seen = [[o.y for o in chunk] async for chunk in port.predict_stream(chunks())]

        assert seen == [[2.0, 4.0, 6.0]]  # caller chunk preserved, wire calls split
